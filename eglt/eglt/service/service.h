#ifndef EGLT_SERVICE_SERVICE_H_
#define EGLT_SERVICE_SERVICE_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/actions/action.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/node_map.h"
#include "eglt/service/session.h"

namespace eglt {

class Action;

struct StreamToSessionConnection {
  base::EvergreenStream* stream = nullptr;
  Session* session = nullptr;

  std::string session_id; // dead sessions may lose their id.
  std::string stream_id; // dead streams may lose their id.

  absl::Status status;
};

std::unique_ptr<Action> MakeActionInConnection(
  const StreamToSessionConnection& connection, std::string_view action_name,
  std::string_view action_id = "");

using EvergreenConnectionHandler =
std::function<absl::Status(base::EvergreenStream*, Session*)>;

absl::Status RunSimpleEvergreenSession(base::EvergreenStream* stream,
                                       Session* session);

class Service : public std::enable_shared_from_this<Service> {
  // This class is the main entry point for the Evergreen service. It is
  // responsible for managing sessions, streams, and connections. It also
  // provides methods to establish and join connections, as well as to set the
  // action registry.
  //
  // The service can be instantiated with an optional action registry and
  // connection handler. If the action registry is not provided, it will be
  // initialized with an empty registry. If the connection handler is not
  // provided, it will be initialized with RunSimpleEvergreenSession. The chunk
  // store factory is used to create chunk stores for new sessions. By default,
  // LocalChunkStore-s are created.
  //
  // This class is thread-safe. Whenever a connection is joined, it is moved out
  // of the Service object under a lock, and the Service object is no longer
  // responsible for managing it. Action registry is set under a lock, and
  // getters use read locks.
  //
  // The Service object can be destroyed at any time, and it will take care of
  // joining all the connections and cleaning up all the fibers. However, the
  // Service object itself will not be destroyed until all the connections have
  // been joined.
public:
  explicit Service(
    ActionRegistry* action_registry = nullptr,
    EvergreenConnectionHandler connection_handler = RunSimpleEvergreenSession,
    ChunkStoreFactory chunk_store_factory = {});

  ~Service();

  Service(const Service& other) = delete;
  Service& operator=(const Service& other) = delete;

  auto GetStream(std::string_view stream_id) const -> base::EvergreenStream*;
  auto GetSession(std::string_view session_id) const -> Session*;
  auto GetSessionKeys() const -> std::vector<std::string>;

  auto EstablishConnection(
    std::shared_ptr<base::EvergreenStream>&& stream,
    EvergreenConnectionHandler connection_handler = nullptr)
    -> absl::StatusOr<std::shared_ptr<StreamToSessionConnection>>;
  auto JoinConnection(StreamToSessionConnection* connection) -> absl::Status;

  auto SetActionRegistry(const ActionRegistry& action_registry) const -> void;

private:
  void JoinConnectionsAndCleanUp(bool cancel = false)
  ABSL_LOCKS_EXCLUDED(mutex_);

  std::unique_ptr<ActionRegistry> action_registry_;
  EvergreenConnectionHandler connection_handler_;
  ChunkStoreFactory chunk_store_factory_;

  mutable concurrency::Mutex mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<base::EvergreenStream>>
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

} // namespace eglt

#endif  // EGLT_SERVICE_SERVICE_H_
