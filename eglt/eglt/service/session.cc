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
#include "eglt/nodes/chunk_store_local.h"
#include "eglt/nodes/node_map.h"

namespace eglt {

Session::Session(NodeMap* node_map, ActionRegistry* action_registry,
                 ChunkStoreFactory chunk_store_factory)
    : node_map_(node_map),
      action_registry_(action_registry),
      chunk_store_factory_(std::move(chunk_store_factory)) {}

Session::~Session() {
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

void Session::DispatchFrom(base::EvergreenStream* stream) {
  concurrency::MutexLock dispatch_from_lock(&mutex_);

  if (stream == nullptr || dispatch_tasks_.contains(stream)) {
    return;
  }

  auto task = concurrency::NewTree({}, [this, stream]() {
    while (stream != nullptr) {
      if (auto message = stream->Receive(); message.has_value()) {
        DispatchMessage(message.value(), stream).IgnoreError();
      } else {
        break;
      }
    }

    std::unique_ptr<concurrency::Fiber> dispatcher_fiber;
    {
      concurrency::MutexLock lock(&mutex_);
      if (const auto node = dispatch_tasks_.extract(stream); !node.empty()) {
        dispatcher_fiber = std::move(node.mapped());
      }
      CHECK(dispatcher_fiber != nullptr)
          << "Dispatched stream not found in session.";
    }

    concurrency::Detach(std::move(dispatcher_fiber));
  });

  dispatch_tasks_.emplace(stream, std::move(task));
}

absl::Status Session::DispatchMessage(SessionMessage message,
                                      base::EvergreenStream* stream) {
  concurrency::MutexLock lock(&mutex_);
  if (joined_) {
    return absl::FailedPreconditionError(
        "Session has been joined, cannot dispatch messages.");
  }
  absl::Status status;
  for (auto& node_fragment : message.node_fragments) {
    AsyncNode* node = GetNode(node_fragment.id);
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
    // for later: error handling
    concurrency::Detach(
        {}, [action_message = std::move(action_message), stream, this] {
          const auto action = std::shared_ptr(action_registry_->MakeAction(
              action_message.name, action_message.id, action_message.inputs,
              action_message.outputs));
          action->BindNodeMap(node_map_);
          action->BindStream(stream);
          action->BindSession(this);
          if (const auto run_status = action->Run(); !run_status.ok()) {
            LOG(ERROR) << "Failed to run action: " << run_status;
          }
        });
  }
  return status;
}

void Session::StopDispatchingFrom(base::EvergreenStream* stream) {
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
  JoinDispatchers(/*cancel=*/true);
}

void Session::JoinDispatchers(bool cancel) {
  mutex_.Lock();
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

  mutex_.Unlock();
}

ActionRegistry* Action::GetRegistry() const {
  return session_->GetActionRegistry();
}

}  // namespace eglt
