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

#include "session.h"

#include <memory>
#include <string_view>
#include <utility>

#include "eglt/absl_headers.h"
#include "eglt/actions/action.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/async_node.h"
#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/local_chunk_store.h"
#include "eglt/nodes/node_map.h"

namespace eglt {

Session::Session(NodeMap* absl_nonnull node_map,
                 ActionRegistry* absl_nullable action_registry,
                 ChunkStoreFactory chunk_store_factory)
    : node_map_(node_map),
      action_registry_(action_registry),
      chunk_store_factory_(std::move(chunk_store_factory)) {}

Session::~Session() {
  concurrency::MutexLock lock(&mutex_);
  DLOG(INFO) << "Session destructor called.";
  if (actions_cancelled_.HasBeenNotified()) {
    actions_cancelled_.Notify();
  }

  CancelActions();

  const absl::Time deadline = absl::Now() + absl::Seconds(10);
  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mutex_, deadline)) {
      CHECK(running_actions_.empty())
          << "Session destructor timed out waiting for actions to "
             "finish. Some actions have not completed.";
    }
  }

  JoinDispatchers(/*cancel=*/true);
}

AsyncNode* Session::GetNode(
    const std::string_view id,
    const ChunkStoreFactory& chunk_store_factory) const {
  ChunkStoreFactory factory = chunk_store_factory;
  if (factory == nullptr) {
    factory = chunk_store_factory_;
  }
  return node_map_->Get(id, factory);
}

void Session::DispatchFrom(EvergreenStream* absl_nonnull stream) {
  concurrency::MutexLock lock(&mutex_);

  if (dispatch_tasks_.contains(stream)) {
    return;
  }

  dispatch_tasks_.emplace(
      stream, concurrency::NewTree({}, [this, stream]() {
        while (true) {
          if (auto message = stream->Receive(); message.has_value()) {
            DispatchMessage(std::move(*message), stream).IgnoreError();
          } else {
            break;
          }
        }

        std::unique_ptr<concurrency::Fiber> dispatcher_fiber;
        {
          concurrency::MutexLock cleanup_lock(&mutex_);
          if (const auto node = dispatch_tasks_.extract(stream);
              !node.empty()) {
            dispatcher_fiber = std::move(node.mapped());
          }
          CHECK(dispatcher_fiber != nullptr)
              << "Dispatched stream not found in session.";
        }

        concurrency::Detach(std::move(dispatcher_fiber));
      }));
}

absl::Status Session::DispatchMessage(SessionMessage message,
                                      EvergreenStream* absl_nullable stream) {
  concurrency::MutexLock lock(&mutex_);
  if (joined_) {
    return absl::FailedPreconditionError(
        "Session has been joined, cannot dispatch messages.");
  }
  absl::Status status;
  for (auto& node_fragment : message.node_fragments) {
    AsyncNode* absl_nonnull node = GetNode(node_fragment.id);
    status.Update(node->Put(std::move(node_fragment)));
    if (!status.ok()) {
      LOG(ERROR) << "Failed to dispatch node fragment: " << status;
      return status;
    }
    status.Update(node->GetWriterStatus());
    if (!status.ok()) {
      LOG(ERROR) << "Failed to dispatch node fragment: " << status;
      return status;
    }
  }
  for (auto& action_message : message.actions) {
    auto action = std::shared_ptr(action_registry_->MakeAction(
        action_message.name, action_message.id, action_message.inputs,
        action_message.outputs));

    Action* action_ptr = action.get();

    auto action_fiber = concurrency::NewTree(
        {}, [action = std::move(action), stream, this]() mutable {
          action->BindNodeMap(node_map_);
          action->BindStream(stream);
          action->BindSession(this);
          if (const auto run_status = action->Run(&actions_cancelled_);
              !run_status.ok()) {
            LOG(ERROR) << "Failed to run action: " << run_status;
          }
          concurrency::MutexLock lock(&mutex_);
          const auto map_node = running_actions_.extract(action.get());
          CHECK(!map_node.empty())
              << "Running action not found in session it was created in.";
          action = nullptr;
          cv_.SignalAll();
          concurrency::Detach(std::move(map_node.mapped()));
        });

    running_actions_[action_ptr] = std::move(action_fiber);
  }
  return status;
}

void Session::CancelActions() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  // cooperative (external) cancellation of all actions
  if (actions_cancelled_.HasBeenNotified()) {
    actions_cancelled_.Notify();
  }

  for (auto& [action, action_fiber] : running_actions_) {
    CHECK(action_fiber != nullptr) << "Action is still in the running "
                                      "actions map, but the fiber is null.";

    // cooperative cancellation of each individual action
    action->Cancel();

    // "forceful" cancellation of running actions' fibers. It is also
    // cooperative (not obligatory), but it will help the library's classes
    // to react to cancellation even if the user forgot to check for it in
    // their handler code.
    action_fiber->Cancel();
  }
  DLOG(INFO) << "Cancelled all actions in session.";
}

void Session::StopDispatchingFrom(EvergreenStream* absl_nonnull stream) {
  std::unique_ptr<concurrency::Fiber> task;
  {
    concurrency::MutexLock lock(&mutex_);
    if (const auto node = dispatch_tasks_.extract(stream); !node.empty()) {
      task = std::move(node.mapped());
    }

    if (task == nullptr) {
      return;
    }
    task->Cancel();
  }

  task->Join();
}

void Session::StopDispatchingFromAll() {
  concurrency::MutexLock lock(&mutex_);
  JoinDispatchers(/*cancel=*/true);
}

void Session::JoinDispatchers(bool cancel) {
  joined_ = true;

  if (cancel) {
    for (auto& [_, task] : dispatch_tasks_) {
      task->Cancel();
    }
  }
  for (const auto& [_, task] : dispatch_tasks_) {
    mutex_.Unlock();
    task->Join();
    mutex_.Lock();
  }
}

ActionRegistry* Action::GetRegistry() const {
  concurrency::MutexLock lock(&mutex_);
  return session_->GetActionRegistry();
}

}  // namespace eglt
