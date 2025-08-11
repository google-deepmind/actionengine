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

#ifndef ACTIONENGINE_SERVICE_SESSION_H_
#define ACTIONENGINE_SERVICE_SESSION_H_

#include <memory>
#include <string_view>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/actions/action.h"
#include "actionengine/actions/registry.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

class ActionContext {
 public:
  static constexpr absl::Duration kFiberCancellationTimeout = absl::Seconds(3);
  static constexpr absl::Duration kActionDetachTimeout = absl::Seconds(10);

  ActionContext() = default;
  ~ActionContext();

  absl::Status Dispatch(std::shared_ptr<Action> action);

  void CancelContext();

  void WaitForActionsToDetach(
      absl::Duration cancel_timeout = kFiberCancellationTimeout,
      absl::Duration detach_timeout = kActionDetachTimeout);

 private:
  void CancelContextInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::unique_ptr<thread::Fiber> ExtractActionFiber(Action* absl_nonnull action)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void WaitForActionsToDetachInternal(
      absl::Duration cancel_timeout = kFiberCancellationTimeout,
      absl::Duration detach_timeout = kActionDetachTimeout)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  act::Mutex mu_;
  absl::flat_hash_map<Action*, std::unique_ptr<thread::Fiber>> running_actions_
      ABSL_GUARDED_BY(mu_);
  bool cancelled_ ABSL_GUARDED_BY(mu_) = false;
  act::CondVar cv_ ABSL_GUARDED_BY(mu_);
};

struct StreamDispatchTask {
  std::shared_ptr<WireStream> stream;
  std::unique_ptr<thread::Fiber> fiber;
  absl::Status status;
  thread::PermanentEvent done;
};

/**
 * A session for handling ActionEngine actions.
 *
 * This class is used to manage the lifecycle of a session, including dispatching
 * messages and managing nodes and actions.
 *
 * @headerfile actionengine/service/session.h
 */
class Session {
 public:
  /**
   * Constructs a Session with the given NodeMap and optional ActionRegistry.
   *
   * @param node_map
   *   The NodeMap to use for this session. Must not be null.
   * @param action_registry
   *   The ActionRegistry to use for this session. If null, no actions will be
   *   available in this session, only data exchange.
   * @param chunk_store_factory
   *   The factory to use for creating chunk stores. Defaults to an empty
   *   factory, which means that the choice is delegated to the NodeMap.
   */
  explicit Session(NodeMap* absl_nonnull node_map,
                   ActionRegistry* absl_nullable action_registry = nullptr,
                   ChunkStoreFactory chunk_store_factory = {});
  ~Session();

  // This class is not copyable or movable.
  Session(const Session& other) = delete;
  Session& operator=(const Session& other) = delete;

  [[nodiscard]] AsyncNode* absl_nonnull
  GetNode(std::string_view id,
          const ChunkStoreFactory& chunk_store_factory = {}) const;

  void DispatchFrom(const std::shared_ptr<WireStream>& stream);
  absl::Status DispatchMessage(SessionMessage message,
                               WireStream* absl_nullable stream = nullptr);

  void StopDispatchingFrom(WireStream* absl_nonnull stream);
  void StopDispatchingFromAll();

  [[nodiscard]] absl::Duration GetRecvTimeout() const { return recv_timeout_; }

  [[nodiscard]] NodeMap* absl_nullable GetNodeMap() const { return node_map_; }

  [[nodiscard]] ActionRegistry* absl_nullable GetActionRegistry() const {
    return action_registry_;
  }

  void SetActionRegistry(ActionRegistry* absl_nullable action_registry) {
    action_registry_ = action_registry;
  }

 private:
  void JoinDispatchers(bool cancel = false) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  act::Mutex mu_;
  bool joined_ ABSL_GUARDED_BY(mu_) = false;
  absl::flat_hash_map<WireStream*, std::unique_ptr<thread::Fiber>>
      dispatch_tasks_ ABSL_GUARDED_BY(mu_){};
  const absl::Duration recv_timeout_ = absl::Seconds(3600);

  NodeMap* absl_nonnull const node_map_;
  ActionRegistry* absl_nullable action_registry_ = nullptr;
  ChunkStoreFactory chunk_store_factory_;

  std::unique_ptr<ActionContext> action_context_ = nullptr;
};

namespace internal {
struct FromSessionTag {};
}  // namespace internal

}  // namespace act

#endif  // ACTIONENGINE_SERVICE_SESSION_H_
