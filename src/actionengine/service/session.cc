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

#include "actionengine/service/session.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>

#include "actionengine/actions/action.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

ActionContext::~ActionContext() {
  act::MutexLock lock(&mu_);
  DLOG(INFO) << "ActionContext::~ActionContext()";

  CancelContextInternal();

  WaitForActionsToDetachInternal();

  CHECK(running_actions_.empty())
      << "ActionContext::~ActionContext() timed out waiting for actions to "
         "detach. Please make sure that all actions react to cancellation "
         "and detach themselves from the context.";
}

absl::Status ActionContext::Dispatch(std::shared_ptr<Action> action) {
  act::MutexLock l(&mu_);
  if (cancelled_) {
    return absl::CancelledError("Action context is cancelled.");
  }

  action->SetUserData(std::make_shared<internal::FromSessionTag>());

  Action* absl_nonnull action_ptr = action.get();
  running_actions_[action_ptr] =
      thread::NewTree({}, [action = std::move(action), this]() mutable {
        act::MutexLock lock(&mu_);

        mu_.Unlock();
        if (const auto run_status = action->Run(); !run_status.ok()) {
          LOG(ERROR) << "Failed to run action: " << run_status;
        }
        mu_.Lock();

        thread::Detach(ExtractActionFiber(action.get()));
        cv_.SignalAll();
      });

  return absl::OkStatus();
}

void ActionContext::CancelContext() {
  act::MutexLock lock(&mu_);
  CancelContextInternal();
}

void ActionContext::WaitForActionsToDetach(absl::Duration cancel_timeout,
                                           absl::Duration detach_timeout) {
  act::MutexLock lock(&mu_);
  WaitForActionsToDetachInternal(cancel_timeout, detach_timeout);
}

void ActionContext::CancelContextInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (cancelled_) {
    return;
  }
  for (auto& [action, action_fiber] : running_actions_) {
    action->Cancel();
    action_fiber->Cancel();
  }
  cancelled_ = true;
  DLOG(INFO) << absl::StrFormat(
      "Action context cancelled, actions pending: %v.",
      running_actions_.size());
}

std::unique_ptr<thread::Fiber> ActionContext::ExtractActionFiber(
    Action* action) {
  const auto map_node = running_actions_.extract(action);
  CHECK(!map_node.empty())
      << "Running action not found in session it was created in.";
  return std::move(map_node.mapped());
}

void ActionContext::WaitForActionsToDetachInternal(
    absl::Duration cancel_timeout, absl::Duration detach_timeout) {
  const absl::Time now = absl::Now();
  const absl::Time fiber_cancel_by = now + cancel_timeout;
  const absl::Time expect_actions_to_detach_by = now + detach_timeout;

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mu_, fiber_cancel_by)) {
      break;
    }
  }

  if (running_actions_.empty()) {
    DLOG(INFO) << "All actions have detached cooperatively.";
    return;
  }

  CancelContextInternal();
  DLOG(INFO) << "Some actions are still running: sent cancellations and "
                "waiting for them to detach.";

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mu_, expect_actions_to_detach_by)) {
      DLOG(ERROR) << "Timed out waiting for actions to detach.";
      break;
    }
  }
}

Session::Session(NodeMap* absl_nonnull node_map,
                 ActionRegistry* absl_nullable action_registry,
                 ChunkStoreFactory chunk_store_factory)
    : node_map_(node_map),
      action_registry_(action_registry),
      chunk_store_factory_(std::move(chunk_store_factory)),
      action_context_(std::make_unique<ActionContext>()) {}

Session::~Session() {
  act::MutexLock lock(&mu_);
  action_context_->CancelContext();
  action_context_->WaitForActionsToDetach();
  JoinDispatchers(/*cancel=*/true);
}

AsyncNode* absl_nonnull
Session::GetNode(const std::string_view id,
                 const ChunkStoreFactory& chunk_store_factory) const {
  ChunkStoreFactory factory = chunk_store_factory;
  if (factory == nullptr) {
    factory = chunk_store_factory_;
  }
  return node_map_->Get(id, factory);
}

void Session::DispatchFrom(const std::shared_ptr<WireStream>& stream) {
  act::MutexLock lock(&mu_);

  if (joined_) {
    return;
  }

  if (dispatch_tasks_.contains(stream.get())) {
    return;
  }

  dispatch_tasks_.emplace(
      stream.get(), thread::NewTree({}, [this, stream]() {
        while (true) {
          absl::StatusOr<std::optional<SessionMessage>> message =
              stream->Receive(GetRecvTimeout());
          if (!message.ok()) {
            DLOG(ERROR) << "Failed to receive message: " << message.status()
                        << " from stream: " << stream->GetId()
                        << ". Stopping dispatch.";
          }
          if (!message.ok()) {
            stream->Abort();
            break;
          }
          if (!message->has_value()) {
            stream->HalfClose();
            break;
          }
          if (absl::Status dispatch_status =
                  DispatchMessage(**std::move(message), stream.get());
              !dispatch_status.ok()) {
            DLOG(ERROR) << "Failed to dispatch message: " << dispatch_status
                        << " from stream: " << stream->GetId()
                        << ". Stopping dispatch.";
            break;
          }
        }

        std::unique_ptr<thread::Fiber> dispatcher_fiber;
        {
          act::MutexLock cleanup_lock(&mu_);
          if (const auto node = dispatch_tasks_.extract(stream.get());
              !node.empty()) {
            dispatcher_fiber = std::move(node.mapped());
          }
        }
        if (dispatcher_fiber == nullptr) {
          return;
        }
        thread::Detach(std::move(dispatcher_fiber));
      }));
}

absl::Status Session::DispatchMessage(SessionMessage message,
                                      WireStream* absl_nullable stream) {
  act::MutexLock lock(&mu_);
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
    if (!action_registry_->IsRegistered(action_name)) {
      status.Update(
          absl::NotFoundError(absl::StrCat("Action not found: ", action_name)));
      break;
    }
    auto action = action_registry_->MakeAction(
        action_name, action_id, std::move(inputs), std::move(outputs));
    action->BindNodeMap(node_map_);
    action->BindSession(this);
    action->BindStream(stream);

    // The session class is intended to represent a session where there is
    // another party involved. In this case, we want to clear inputs and outputs
    // after the action is run, because they will already have been sent to the
    // other party, and we don't want to keep them around locally.
    action->ClearInputsAfterRun(true);
    action->ClearOutputsAfterRun(true);

    status.Update(action_context_->Dispatch(std::move(action)));
  }
  if (!status.ok()) {
    DLOG(ERROR) << "Failed to dispatch message: " << status;
  }
  return status;
}

void Session::StopDispatchingFrom(WireStream* absl_nonnull stream) {
  std::unique_ptr<thread::Fiber> task;
  {
    act::MutexLock lock(&mu_);
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
  act::MutexLock lock(&mu_);
  JoinDispatchers(/*cancel=*/true);
}

void Session::JoinDispatchers(bool cancel) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  joined_ = true;

  std::vector<std::unique_ptr<thread::Fiber>> tasks_to_join;
  for (auto& [_, task] : dispatch_tasks_) {
    tasks_to_join.push_back(std::move(task));
  }

  if (cancel) {
    for (const auto& task : tasks_to_join) {
      task->Cancel();
    }
  }
  for (const auto& task : tasks_to_join) {
    mu_.Unlock();
    task->Join();
    mu_.Lock();
  }
}

}  // namespace act
