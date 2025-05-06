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

#include "service.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/actions/action.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/service/session.h"

namespace eglt {

absl::Status RunSimpleEvergreenSession(EvergreenStream* absl_nonnull stream,
                                       Session* absl_nonnull session) {
  absl::Status status;
  while (true) {
    std::optional<SessionMessage> message = stream->Receive();
    if (!message.has_value()) {
      break;
    }
    status = session->DispatchMessage(message.value(), stream);
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

Service::Service(ActionRegistry* action_registry,
                 EvergreenConnectionHandler connection_handler,
                 ChunkStoreFactory chunk_store_factory)
    : action_registry_(std::make_unique<ActionRegistry>(*action_registry)),
      connection_handler_(std::move(connection_handler)),
      chunk_store_factory_(std::move(chunk_store_factory)) {}

Service::~Service() {
  JoinConnectionsAndCleanUp();
}

EvergreenStream* Service::GetStream(std::string_view stream_id) const {
  concurrency::MutexLock lock(&mutex_);
  if (connections_.contains(stream_id)) {
    return connections_.at(stream_id)->stream;
  }
  return nullptr;
}

Session* Service::GetSession(std::string_view session_id) const {
  concurrency::MutexLock lock(&mutex_);
  if (sessions_.contains(session_id)) {
    return sessions_.at(session_id).get();
  }
  return nullptr;
}

std::vector<std::string> Service::GetSessionKeys() const {
  concurrency::MutexLock lock(&mutex_);
  std::vector<std::string> keys;
  keys.reserve(sessions_.size());
  for (const auto& [key, _] : sessions_) {
    keys.push_back(key);
  }
  return keys;
}

absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>
Service::EstablishConnection(std::shared_ptr<EvergreenStream>&& stream,
                             EvergreenConnectionHandler connection_handler) {
  const auto stream_id = stream->GetId();
  if (stream_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no id.");
  }

  const auto& session_id = stream_id;
  if (session_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no session id.");
  }

  concurrency::MutexLock lock(&mutex_);

  if (cleanup_started_) {
    return absl::FailedPreconditionError(
        "Service is shutting down, cannot establish new connections.");
  }

  streams_.emplace(stream_id, std::move(stream));

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
          .stream = streams_.at(stream_id).get(),
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
  }

  // for later: Stubby streams require Accept() to be called before returning
  // from StartSession. This might not be the ideal solution with other streams.
  auto& connection = eglt::FindOrDie(connections_, stream_id);
  if (absl::Status status = connection->stream->Accept(); !status.ok()) {
    CleanupConnection(*connection);
    return status;
  }

  connection_fibers_[stream_id] = concurrency::NewTree(
      concurrency::TreeOptions(),
      [this, resolved_handler = std::move(resolved_handler), connection]() {
        connection->status =
            resolved_handler(connection->stream, connection->session);
        concurrency::MutexLock cleanup_lock(&mutex_);
        CleanupConnection(*connection);
      });

  return connection;
}

absl::Status Service::JoinConnection(StreamToSessionConnection* connection) {
  std::unique_ptr<concurrency::Fiber> fiber(nullptr);

  // Extract connection and fiber from the map, so we can join them outside
  // the lock with a guarantee that they are not modified while we are trying.
  {
    concurrency::MutexLock lock(&mutex_);
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
  concurrency::MutexLock lock(&mutex_);
  *action_registry_ = action_registry;
}

void Service::JoinConnectionsAndCleanUp(bool cancel) {
  absl::flat_hash_map<std::string, std::unique_ptr<concurrency::Fiber>> fibers;

  bool cleanup_started = false;
  {
    concurrency::MutexLock lock(&mutex_);
    cleanup_started = cleanup_started_;
    if (!cleanup_started_) {
      // We are the first ones here, so we will start the cleanup.
      cleanup_started_ = true;

      fibers = std::move(connection_fibers_);
      connection_fibers_.clear();

      absl::flat_hash_map<std::string,
                          std::shared_ptr<StreamToSessionConnection>>
          connections = std::move(connections_);
      connections_.clear();
    }
  }

  // It is now safe to operate with the extracted data structures, because we
  // know that nobody can modify them while we are doing so, and we have
  // communicated to other threads that the cleanup has started, if so.

  if (cleanup_started) {
    // If we are here, it means that a cleanup is either in progress, or has
    // already finished. In the former case, we need to wait for the cleanup to
    // finish before returning. In either case, we do not need to do anything
    // else.
    concurrency::Select({cleanup_done_.OnEvent()});
    return;
  }

  // If we are here, it means that we are the first ones to start the cleanup,
  // and definitely extracted the right data structures. We need to cancel all
  // the fibers, and then join them.
  if (cancel) {
    for (const auto& [_, fiber] : fibers) {
      if (fiber != nullptr) {
        fiber->Cancel();
      }
    }
  }

  for (const auto& [_, fiber] : fibers) {
    if (fiber != nullptr) {
      concurrency::JoinOptimally(fiber.get());
    }
  }
}

}  // namespace eglt
