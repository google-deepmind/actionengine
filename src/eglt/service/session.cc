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
#include "eglt/nodes/node_map.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/local_chunk_store.h"

namespace eglt {

ActionContext::~ActionContext() {
  concurrency::MutexLock lock(&mutex_);
  DLOG(INFO) << "Action context destructor called.";
  const absl::Time now = absl::Now();
  const absl::Time fiber_cancel_by = now + kFiberCancellationTimeout;
  const absl::Time expect_actions_to_detach_by = now + kActionDetachTimeout;

  if (!cancelled_) {
    CancelActions();
  }

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mutex_, fiber_cancel_by)) {
      break;
    }
  }

  if (running_actions_.empty()) {
    DLOG(INFO) << "All action have detached cooperatively.";
    return;
  }

  for (auto& [_, action_fiber] : running_actions_) {
    action_fiber->Cancel();
  }
  DLOG(INFO) << "Some actions are still running, waiting for them to detach.";

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mutex_, expect_actions_to_detach_by)) {
      break;
    }
  }

  CHECK(running_actions_.empty())
      << "Action context destructor timed out waiting for actions to "
         "detach. Please make sure that all actions react to cancellation "
         "and detach themselves from the context.";
}

absl::Status ActionContext::Dispatch(std::shared_ptr<Action> action) {
  concurrency::MutexLock lock(&mutex_);
  if (cancelled_) {
    return absl::CancelledError("Action context is cancelled.");
  }

  Action* action_ptr = action.get();

  running_actions_[action_ptr] =
      concurrency::NewTree({}, [action = std::move(action), this]() {
        if (const auto run_status = action->Run(&cancellation_);
            !run_status.ok()) {
          LOG(ERROR) << "Failed to run action: " << run_status;
        }
        concurrency::MutexLock cleanup_lock(&mutex_);
        concurrency::Detach(ExtractActionFiber(action.get()));
        cv_.SignalAll();
      });

  return absl::OkStatus();
}

void ActionContext::CancelActionsImpl() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  cancellation_.Notify();
  cancelled_ = true;
  DLOG(INFO) << "Action context cancelled.";
}

Session::Session(NodeMap* absl_nonnull node_map,
                 ActionRegistry* absl_nullable action_registry,
                 ChunkStoreFactory chunk_store_factory)
    : node_map_(node_map),
      action_registry_(action_registry),
      chunk_store_factory_(std::move(chunk_store_factory)),
      action_context_(std::make_unique<ActionContext>()) {}

Session::~Session() {
  concurrency::MutexLock lock(&mutex_);
  DLOG(INFO) << "Session destructor called.";
  action_context_->CancelActions();
  JoinDispatchers(/*cancel=*/false);
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

void Session::DispatchFrom(
    const std::shared_ptr<EvergreenWireStream>& absl_nonnull stream) {
  concurrency::MutexLock lock(&mutex_);

  if (joined_) {
    return;
  }

  if (dispatch_tasks_.contains(stream.get())) {
    return;
  }

  dispatch_tasks_.emplace(
      stream.get(), concurrency::NewTree({}, [this, stream]() {
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
          if (const auto node = dispatch_tasks_.extract(stream.get());
              !node.empty()) {
            dispatcher_fiber = std::move(node.mapped());
          }
          CHECK(dispatcher_fiber != nullptr)
              << "Dispatched stream not found in session.";
        }

        concurrency::Detach(std::move(dispatcher_fiber));
      }));
}

absl::Status Session::DispatchMessage(
    SessionMessage message,
    const std::shared_ptr<EvergreenWireStream>& absl_nullable stream) {
  concurrency::MutexLock lock(&mutex_);
  if (joined_) {
    return absl::FailedPreconditionError(
        "Session has been joined, cannot dispatch messages.");
  }
  absl::Status status;
  for (auto& node_fragment : message.node_fragments) {
    AsyncNode* absl_nonnull node = GetNode(node_fragment.id);
    status.Update(node->Put(std::move(node_fragment)));
  }
  for (auto& [action_id, action_name, inputs, outputs] : message.actions) {
    auto action = action_registry_->MakeAction(
        action_name, action_id, std::move(inputs), std::move(outputs));
    action->BindNodeMap(node_map_);
    action->BindSession(this);
    action->BindStream(stream);

    status.Update(action_context_->Dispatch(std::move(action)));
  }
  return status;
}

void Session::StopDispatchingFrom(EvergreenWireStream* absl_nonnull stream) {
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
