#ifndef EGLT_SERVICE_SESSION_H_
#define EGLT_SERVICE_SESSION_H_

#include <functional>
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/node_map.h"

namespace eglt {

class ActionRegistry;

using DebugHandler =
    std::function<void(const base::SessionMessage&, base::EvergreenStream*)>;

class Session {
 public:
  explicit Session(NodeMap* node_map, ActionRegistry* action_registry = nullptr,
                   ChunkStoreFactory chunk_store_factory = {});
  ~Session();

  Session(const Session& other) = delete;
  Session& operator=(const Session& other) = delete;

  [[nodiscard]] AsyncNode* GetNode(
      std::string_view id,
      const ChunkStoreFactory& chunk_store_factory = {}) const;

  void DispatchFrom(base::EvergreenStream* stream);
  absl::Status DispatchMessage(base::SessionMessage message,
                               base::EvergreenStream* stream = nullptr);

  void StopDispatchingFrom(base::EvergreenStream* stream);
  void StopDispatchingFromAll();

  [[nodiscard]] NodeMap* GetNodeMap() const { return node_map_; }

  [[nodiscard]] ActionRegistry* GetActionRegistry() const {
    return action_registry_;
  }

  void SetActionRegistry(ActionRegistry* action_registry) {
    action_registry_ = action_registry;
  }

 private:
  void JoinDispatchers(bool cancel = false);
  absl::flat_hash_map<base::EvergreenStream*,
                      std::unique_ptr<concurrency::Fiber>>
      dispatch_tasks_;

  NodeMap* node_map_ = nullptr;
  ActionRegistry* action_registry_ = nullptr;
  ChunkStoreFactory chunk_store_factory_;
};

}  // namespace eglt

#endif  // EGLT_SERVICE_SESSION_H_
