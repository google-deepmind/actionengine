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

#ifndef EGLT_SERVICE_SESSION_H_
#define EGLT_SERVICE_SESSION_H_

#include <functional>
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/peers.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/stores/chunk_store.h"

namespace eglt {

class Action;

class ActionRegistry;

class Session;

class ActionContext {
 public:
  static constexpr absl::Duration kFiberCancellationTimeout = absl::Seconds(3);
  static constexpr absl::Duration kActionDetachTimeout = absl::Seconds(10);

  ActionContext() = default;
  ~ActionContext();

  absl::Status Dispatch(std::shared_ptr<Action> action);
  void CancelActions() ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    CancelActionsImpl();
  }

 private:
  void CancelActionsImpl() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::unique_ptr<concurrency::Fiber> ExtractActionFiber(Action* action)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    const auto map_node = running_actions_.extract(action);
    CHECK(!map_node.empty())
        << "Running action not found in session it was created in.";
    return std::move(map_node.mapped());
  }

  concurrency::Mutex mutex_;
  absl::flat_hash_map<Action*, std::unique_ptr<concurrency::Fiber>>
      running_actions_ ABSL_GUARDED_BY(mutex_);
  concurrency::PermanentEvent cancellation_;
  bool cancelled_;
  concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);
};

/**
 * @brief
 *   A session for handling Evergreen actions.
 *
 * This class is used to manage the lifecycle of a session, including dispatching
 * messages and managing nodes and actions.
 *
 * @headerfile eglt/service/session.h
 */
class Session {
 public:
  explicit Session(NodeMap* absl_nonnull node_map,
                   ActionRegistry* absl_nullable action_registry = nullptr,
                   ChunkStoreFactory chunk_store_factory = {});
  ~Session();

  Session(const Session& other) = delete;
  Session& operator=(const Session& other) = delete;

  [[nodiscard]] AsyncNode* GetNode(
      std::string_view id,
      const ChunkStoreFactory& chunk_store_factory = {}) const;

  void DispatchFrom(
      const std::shared_ptr<EvergreenWireStream>& absl_nonnull stream);
  absl::Status DispatchMessage(SessionMessage message,
                               const std::shared_ptr<EvergreenWireStream>&
                                   absl_nullable stream = nullptr);

  void StopDispatchingFrom(EvergreenWireStream* absl_nonnull stream);
  void StopDispatchingFromAll();

  [[nodiscard]] NodeMap* GetNodeMap() const { return node_map_; }

  [[nodiscard]] ActionRegistry* GetActionRegistry() const {
    return action_registry_;
  }

  void SetActionRegistry(ActionRegistry* action_registry) {
    action_registry_ = action_registry;
  }

 private:
  void JoinDispatchers(bool cancel = false)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  concurrency::Mutex mutex_{};
  bool joined_ ABSL_GUARDED_BY(mutex_) = false;
  absl::flat_hash_map<EvergreenWireStream*, std::unique_ptr<concurrency::Fiber>>
      dispatch_tasks_ ABSL_GUARDED_BY(mutex_){};

  NodeMap* absl_nonnull const node_map_;
  ActionRegistry* absl_nullable action_registry_ = nullptr;
  ChunkStoreFactory chunk_store_factory_;

  std::unique_ptr<ActionContext> action_context_ = nullptr;
};

}  // namespace eglt

#endif  // EGLT_SERVICE_SESSION_H_
