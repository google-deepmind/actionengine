// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "eglt/service/service.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/base/optimization.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include "eglt/actions/action.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/service/session.h"
#include "eglt/util/map_util.h"

namespace eglt {

absl::Status RunSimpleEvergreenSession(
    const std::shared_ptr<WireStream>& stream, Session* absl_nonnull session) {
  const auto owned_stream = stream;
  absl::Status status;
  while (!thread::Cancelled()) {
    std::optional<SessionMessage> message = owned_stream->Receive();
    if (!message.has_value()) {
      break;
    }
    status = session->DispatchMessage(message.value(), owned_stream);
  }

  if (thread::Cancelled()) {
    status = absl::CancelledError("Service is shutting down.");
  }

  return status;
}

std::unique_ptr<Action> MakeActionInConnection(
    const StreamToSessionConnection& connection,
    const std::string_view action_name, const std::string_view action_id) {

  if (connection.session == nullptr) {
    return nullptr;
  }
  if (connection.session->GetActionRegistry() == nullptr) {
    return nullptr;
  }

  auto action = connection.session->GetActionRegistry()->MakeAction(action_name,
                                                                    action_id);
  action->BindNodeMap(connection.session->GetNodeMap());
  action->BindStream(connection.stream);
  action->BindSession(connection.session);
  return action;
}

Service::Service(ActionRegistry* absl_nullable action_registry,
                 EvergreenConnectionHandler connection_handler,
                 ChunkStoreFactory chunk_store_factory)
    : action_registry_(
          action_registry == nullptr
              ? nullptr
              : std::make_unique<ActionRegistry>(*action_registry)),
      connection_handler_(std::move(connection_handler)),
      chunk_store_factory_(std::move(chunk_store_factory)) {}

Service::~Service() {
  CloseStreams();
  JoinConnectionsAndCleanUp(/*cancel=*/true);
}

WireStream* absl_nullable Service::GetStream(std::string_view stream_id) const {
  eglt::MutexLock lock(&mu_);
  if (connections_.contains(stream_id)) {
    return connections_.at(stream_id)->stream.get();
  }
  return nullptr;
}

Session* absl_nullable Service::GetSession(std::string_view session_id) const {
  eglt::MutexLock lock(&mu_);
  if (sessions_.contains(session_id)) {
    return sessions_.at(session_id).get();
  }
  return nullptr;
}

std::vector<std::string> Service::GetSessionKeys() const {
  eglt::MutexLock lock(&mu_);
  std::vector<std::string> keys;
  keys.reserve(sessions_.size());
  for (const auto& [key, _] : sessions_) {
    keys.push_back(key);
  }
  return keys;
}

absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>
Service::EstablishConnection(std::shared_ptr<WireStream>&& stream,
                             EvergreenConnectionHandler connection_handler) {
  return EstablishConnection(
      [stream = std::move(stream)]() { return stream.get(); },
      std::move(connection_handler));
}

absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>
Service::EstablishConnection(net::GetStreamFn get_stream,
                             EvergreenConnectionHandler connection_handler) {
  eglt::MutexLock lock(&mu_);

  std::string stream_id;
  if (const WireStream* raw_stream = get_stream(); raw_stream == nullptr) {
    return absl::InvalidArgumentError("Provided stream resolves to nullptr.");
  } else {
    stream_id = raw_stream->GetId();
  }
  if (stream_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no id.");
  }

  const auto& session_id = stream_id;
  if (session_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no session id.");
  }

  if (cleanup_started_) {
    return absl::FailedPreconditionError(
        "Service is shutting down, cannot establish new connections.");
  }

  streams_.emplace(stream_id, std::make_unique<net::RecoverableStream>(
                                  std::move(get_stream), stream_id,
                                  /*timeout=*/absl::Seconds(10)));

  if (connections_.contains(stream_id)) {
    return absl::AlreadyExistsError(
        absl::StrCat("Stream ", stream_id, " is already connected."));
  }

  if (!sessions_.contains(session_id)) {
    node_maps_.emplace(session_id, std::make_unique<NodeMap>());
    sessions_.emplace(session_id,
                      std::make_unique<Session>(
                          /*node_map=*/node_maps_.at(session_id).get(),
                          /*action_registry=*/action_registry_.get(),
                          /*chunk_store_factory=*/
                          chunk_store_factory_));
  }

  streams_per_session_[session_id].insert(stream_id);

  connections_[stream_id] =
      std::make_shared<StreamToSessionConnection>(StreamToSessionConnection{
          .stream = streams_.at(stream_id),
          .session = sessions_.at(session_id).get(),
          .session_id = session_id,
          .stream_id = stream_id,
      });

  EvergreenConnectionHandler resolved_handler = std::move(connection_handler);
  if (resolved_handler == nullptr) {
    resolved_handler = connection_handler_;
  }
  if (resolved_handler == nullptr) {
    LOG(FATAL)
        << "no connection handler provided, and no default handler is set.";
    ABSL_ASSUME(false);
  }

  // for later: Stubby streams require Accept() to be called before returning
  // from StartSession. This might not be the ideal solution with other streams.
  auto& connection = eglt::FindOrDie(connections_, stream_id);
  if (absl::Status status = connection->stream->Accept(); !status.ok()) {
    CleanupConnection(*connection);
    return status;
  }

  connection_fibers_[stream_id] = thread::NewTree(
      thread::TreeOptions(),
      [this, resolved_handler = std::move(resolved_handler), connection]() {
        connection->status =
            resolved_handler(connection->stream, connection->session);
        eglt::MutexLock cleanup_lock(&mu_);
        CleanupConnection(*connection);
      });

  return connection;
}

absl::Status Service::JoinConnection(
    StreamToSessionConnection* absl_nonnull connection) {
  std::unique_ptr<thread::Fiber> fiber(nullptr);

  // Extract connection and fiber from the map, so we can join them outside
  // the lock with a guarantee that they are not modified while we are trying.
  {
    eglt::MutexLock lock(&mu_);
    if (const auto node = connections_.extract(connection->stream_id);
        !node.empty()) {
      std::shared_ptr<StreamToSessionConnection> service_owned_connection =
          std::move(node.mapped());
    }

    if (const auto node = connection_fibers_.extract(connection->stream_id);
        !node.empty()) {
      fiber = std::move(node.mapped());
    }
  }

  if (fiber == nullptr) {
    // Only possible if the connection was already joined.
    // TODO (hpnkv): actually, also if the connection was never
    //   established by this service. For now, it is considered a user error,
    //   therefore Service does not keep track of it.
    return connection->status;
  }
  fiber->Join();
  return connection->status;
}

void Service::SetActionRegistry(const ActionRegistry& action_registry) const {
  eglt::MutexLock lock(&mu_);
  *action_registry_ = action_registry;
}

void Service::JoinConnectionsAndCleanUp(bool cancel) {
  eglt::MutexLock lock(&mu_);
  if (cleanup_started_) {
    mu_.Unlock();
    thread::Select({cleanup_done_.OnEvent()});
    mu_.Lock();
    return;
  }

  cleanup_started_ = true;

  absl::flat_hash_map<std::string, std::unique_ptr<thread::Fiber>> fibers =
      std::move(connection_fibers_);
  connection_fibers_.clear();

  absl::flat_hash_map<std::string, std::shared_ptr<StreamToSessionConnection>>
      connections = std::move(connections_);
  connections_.clear();

  if (cancel) {
    for (const auto& [_, fiber] : fibers) {
      if (fiber != nullptr) {
        fiber->Cancel();
      }
    }
  }

  DLOG(INFO) << "Cleaning up connections.";
  for (const auto& [_, fiber] : fibers) {
    if (fiber != nullptr) {
      mu_.Unlock();
      fiber->Join();
      mu_.Lock();
    }
  }
  DLOG(INFO) << "Connections cleaned up.";
}

}  // namespace eglt
