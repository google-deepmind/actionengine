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

void Session::DispatchFrom(base::EvergreenStream* stream) {
  if (dispatch_tasks_.contains(stream)) {
    return;
  }

  if (stream == nullptr) {
    return;
  }

  auto task =
      concurrency::NewTree(concurrency::TreeOptions(), [this, stream]() {
        while (true) {
          if (stream == nullptr) {
            break;
          }

          auto message = stream->Receive();
          if (!message) {
            break;
          }
          DispatchMessage(message.value(), stream).IgnoreError();
        }
      });

  dispatch_tasks_.emplace(stream, std::move(task));
}

absl::Status Session::DispatchMessage(base::SessionMessage message,
                                      base::EvergreenStream* stream) {
  absl::Status status;
  for (auto& node_fragment : message.node_fragments) {
    AsyncNode* node = GetNode(node_fragment.id);
    node << std::move(node_fragment);
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
          const auto action = std::shared_ptr(Action::FromActionMessage(
              action_message, action_registry_, node_map_, stream, this));
          if (const auto run_status = action->Run(); !run_status.ok()) {
            LOG(ERROR) << "Failed to run action: " << run_status;
          }
        });
  }
  return status;
}

void Session::StopDispatchingFrom(base::EvergreenStream* stream) {
  std::unique_ptr<concurrency::Fiber> task;
  if (const auto node = dispatch_tasks_.extract(stream); !node.empty()) {
    task = std::move(node.mapped());
  }

  if (task == nullptr) {
    return;
  }

  task->Cancel();
  task->Join();
}

void Session::StopDispatchingFromAll() {
  JoinDispatchers(/*cancel=*/true);
}

void Session::JoinDispatchers(bool cancel) {
  if (cancel) {
    for (auto& [_, task] : dispatch_tasks_) {
      task->Cancel();
    }
  }
  for (const auto& [_, task] : dispatch_tasks_) {
    task->Join();
  }
  dispatch_tasks_.clear();
}

ActionRegistry* Action::GetRegistry() const {
  return session_->GetActionRegistry();
}

}  // namespace eglt
