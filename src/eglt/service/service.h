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

#ifndef EGLT_SERVICE_SERVICE_H_
#define EGLT_SERVICE_SERVICE_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/actions/action.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/net/recoverable_stream.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/service/session.h"
#include "eglt/stores/chunk_store.h"

namespace eglt {

class Action;

/**
 * @brief
 *   A connection between a stream and a session.
 *
 * This struct is used to represent a connection between a stream and a session.
 * It contains the stream, session, and their IDs, as well as the status of the
 * connection.
 *
 * @collaborationgraph
 *
 * @headerfile eglt/service/service.h
 */
struct StreamToSessionConnection {
  std::shared_ptr<EvergreenWireStream> absl_nullable stream = nullptr;
  Session* absl_nullable session = nullptr;

  std::string session_id;  // dead sessions may lose their id.
  std::string stream_id;   // dead streams may lose their id.

  absl::Status status;
};

std::unique_ptr<Action> MakeActionInConnection(
    const StreamToSessionConnection& connection, std::string_view action_name,
    std::string_view action_id = "");

using EvergreenConnectionHandler = std::function<absl::Status(
    const std::shared_ptr<EvergreenWireStream>& absl_nonnull,
    Session* absl_nonnull)>;

/// @callgraph
absl::Status RunSimpleEvergreenSession(
    const std::shared_ptr<EvergreenWireStream>& absl_nonnull stream,
    Session* absl_nonnull session);

/**
 * @brief
 *   The Evergreen service class. Manages sessions, streams, and connections.
 *
 * This class provides methods to establish and join connections, as well as
 * to set the action registry.
 *
 * The service can be instantiated with an optional action registry and
 * connection handler. If the action registry is not provided, it will be
 * initialized with an empty registry. If the connection handler is not
 * provided, it will be initialized with RunSimpleEvergreenSession. The chunk
 * store factory is used to create chunk stores for new sessions. By default,
 * `LocalChunkStore`s are created.
 *
 * This class is thread-safe. Whenever a connection is joined, it is moved out
 * of the Service object under a lock, and the Service object is no longer
 * responsible for managing it. Action registry is set under a lock, and
 * getters use read locks.
 *
 * The Service object can be destroyed at any time, and it will take care of
 * joining all the connections and cleaning up all the fibers. However, the
 * Service object itself will not be destroyed until all the connections have
 * been joined.
 *
 * @headerfile eglt/service/service.h
 */
class Service : public std::enable_shared_from_this<Service> {
 public:
  explicit Service(
      ActionRegistry* absl_nullable action_registry = nullptr,
      EvergreenConnectionHandler connection_handler = RunSimpleEvergreenSession,
      ChunkStoreFactory chunk_store_factory = {});

  ~Service();

  Service(const Service& other) = delete;
  Service& operator=(const Service& other) = delete;

  auto GetStream(std::string_view stream_id) const -> EvergreenWireStream*;
  auto GetSession(std::string_view session_id) const -> Session*;
  auto GetSessionKeys() const -> std::vector<std::string>;

  auto EstablishConnection(
      std::shared_ptr<EvergreenWireStream>&& stream,
      EvergreenConnectionHandler connection_handler = nullptr)
      -> absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>;
  auto EstablishConnection(
      net::GetStreamFn get_stream,
      EvergreenConnectionHandler connection_handler = nullptr)
      -> absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>;
  auto JoinConnection(StreamToSessionConnection* absl_nonnull connection)
      -> absl::Status;

  auto SetActionRegistry(const ActionRegistry& action_registry) const -> void;

 private:
  void JoinConnectionsAndCleanUp(bool cancel = false)
      ABSL_LOCKS_EXCLUDED(mutex_);

  void CleanupConnection(const StreamToSessionConnection& connection)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    connections_.erase(connection.stream_id);

    std::shared_ptr<net::RecoverableStream> extracted_stream = nullptr;
    std::unique_ptr<NodeMap> extracted_node_map = nullptr;
    std::unique_ptr<Session> extracted_session = nullptr;

    if (const auto map_node = streams_.extract(connection.stream_id);
        !map_node.empty()) {
      extracted_stream = std::move(map_node.mapped());
    }

    streams_per_session_.at(connection.session_id).erase(connection.stream_id);
    if (streams_per_session_.at(connection.session_id).empty()) {
      if (const auto map_node = sessions_.extract(connection.session_id);
          !map_node.empty()) {
        extracted_session = std::move(map_node.mapped());
      }
      if (const auto map_node = node_maps_.extract(connection.stream_id);
          !map_node.empty()) {
        extracted_node_map = std::move(map_node.mapped());
      }
      streams_per_session_.erase(connection.session_id);
    }

    if (extracted_session != nullptr) {
      extracted_session.reset();
      DLOG(INFO) << "session " << connection.session_id
                 << " has no more stable connections, deleted.";
    }

    if (extracted_stream != nullptr) {
      extracted_stream->HalfClose();
    }
    // extracted_stream.reset();

    extracted_node_map.reset();
    auto fiber = connection_fibers_.extract(connection.stream_id);
    if (!fiber.empty() && fiber.mapped() != nullptr) {
      concurrency::Detach(std::move(fiber.mapped()));
    }
  }

  std::unique_ptr<ActionRegistry> action_registry_;
  EvergreenConnectionHandler connection_handler_;
  ChunkStoreFactory chunk_store_factory_;

  mutable concurrency::Mutex mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<net::RecoverableStream>>
      streams_ ABSL_GUARDED_BY(mutex_);
  // for now, we only support one-to-one session-stream mapping, therefore we
  // use the stream id as the session id.
  absl::flat_hash_map<std::string, std::unique_ptr<NodeMap>> node_maps_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, std::unique_ptr<Session>> sessions_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, std::shared_ptr<StreamToSessionConnection>>
      connections_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>
      streams_per_session_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, std::unique_ptr<concurrency::Fiber>>
      connection_fibers_ ABSL_GUARDED_BY(mutex_);

  bool cleanup_started_ ABSL_GUARDED_BY(mutex_) = false;
  concurrency::PermanentEvent cleanup_done_;
};

}  // namespace eglt

#endif  // EGLT_SERVICE_SERVICE_H_
