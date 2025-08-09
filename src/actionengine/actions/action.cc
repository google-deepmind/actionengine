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

#include "actionengine/actions/action.h"

#include <functional>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>

#include "actionengine/actions/registry.h"
#include "actionengine/data/conversion.h"
#include "actionengine/service/session.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"

namespace act {

Action::Action(ActionSchema schema, std::string_view id,
               std::vector<Port> inputs, std::vector<Port> outputs)
    : schema_(std::move(schema)),
      id_(id.empty() ? GenerateUUID4() : std::string(id)),
      cancelled_(std::make_unique<thread::PermanentEvent>()) {
  ActionMessage message = schema_.GetActionMessage(id_);

  std::vector<Port>& input_parameters =
      inputs.empty() ? message.inputs : inputs;
  for (auto& [input_name, input_id] : std::move(input_parameters)) {
    input_name_to_id_[std::move(input_name)] = std::move(input_id);
  }

  std::vector<Port>& output_parameters =
      outputs.empty() ? message.outputs : outputs;
  for (auto& [output_name, output_id] : std::move(output_parameters)) {
    output_name_to_id_[std::move(output_name)] = std::move(output_id);
  }
}

Action::Action(std::string_view name, std::string_view id,
               std::vector<Port> inputs, std::vector<Port> outputs) {
  ActionSchema schema;
  schema.name = name;
  for (const auto& [input_name, _] : inputs) {
    schema.inputs[input_name] = "*";
  }
  for (const auto& [output_name, _] : outputs) {
    schema.outputs[output_name] = "*";
  }
  *this = Action(std::move(schema), id, std::move(inputs), std::move(outputs));
}

Action& Action::operator=(Action&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  concurrency::TwoMutexLock lock(&mu_, &other.mu_);

  CHECK(!other.has_been_run_)
      << "Cannot move an action that has been run. Handlers rely on the "
         "validity of their pointers to an action.";

  schema_ = std::move(other.schema_);
  id_ = std::move(other.id_);
  input_name_to_id_ = std::move(other.input_name_to_id_);
  output_name_to_id_ = std::move(other.output_name_to_id_);
  handler_ = std::move(other.handler_);
  has_been_run_ = other.has_been_run_;
  node_map_ = other.node_map_;
  session_ = other.session_;
  cancelled_ = std::move(other.cancelled_);
  nodes_with_bound_streams_ = std::move(other.nodes_with_bound_streams_);
  bind_streams_on_inputs_default_ = other.bind_streams_on_inputs_default_;
  bind_streams_on_outputs_default_ = other.bind_streams_on_outputs_default_;
  return *this;
}

Action::~Action() {
  act::MutexLock lock(&mu_);

  reffed_readers_.clear();

  // for (const auto& [output_name, output_id] : output_name_to_id_) {
  //   node_map_->Extract(output_id).reset();
  // }
}

ActionMessage Action::GetActionMessage() const {
  std::vector<Port> input_parameters;
  input_parameters.reserve(input_name_to_id_.size());
  for (const auto& [name, id] : input_name_to_id_) {
    input_parameters.push_back({
        .name = name,
        .id = id,
    });
  }

  std::vector<Port> output_parameters;
  output_parameters.reserve(output_name_to_id_.size());
  for (const auto& [name, id] : output_name_to_id_) {
    output_parameters.push_back({
        .name = name,
        .id = id,
    });
  }

  return {
      .id = id_,
      .name = schema_.name,
      .inputs = input_parameters,
      .outputs = output_parameters,
  };
}

AsyncNode* absl_nullable Action::GetNode(std::string_view id) {
  if (input_name_to_id_.contains(id)) {
    return GetInput(id);
  }
  if (output_name_to_id_.contains(id)) {
    return GetOutput(id);
  }

  return nullptr;
}

AsyncNode* absl_nullable Action::GetInput(std::string_view name,
                                          std::optional<bool> bind_stream) {
  act::MutexLock lock(&mu_);
  if (node_map_ == nullptr) {
    return nullptr;
  }

  if (!input_name_to_id_.contains(name)) {
    return nullptr;
  }

  AsyncNode* node = node_map_->Get(GetInputId(name));
  if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_inputs_default_)) {
    absl::flat_hash_map<std::string, WireStream*> peers;
    peers.insert({std::string(stream_->GetId()), stream_});
    node->BindPeers(std::move(peers));
    nodes_with_bound_streams_.insert(node);
  }

  ChunkStoreReader* reader = &node->GetReader();

  if (!reffed_readers_.contains(reader)) {
    reffed_readers_.insert(reader);
  }

  if (cancelled_->HasBeenNotified()) {
    reader->Cancel();
  }

  return node;
}

void Action::BindNodeMap(NodeMap* absl_nullable node_map) {
  act::MutexLock lock(&mu_);
  node_map_ = node_map;
}

NodeMap* absl_nullable Action::GetNodeMap() const {
  act::MutexLock lock(&mu_);
  return node_map_;
}

void Action::BindStream(WireStream* absl_nullable stream) {
  act::MutexLock lock(&mu_);
  stream_ = stream;
}

WireStream* absl_nullable Action::GetStream() const {
  act::MutexLock lock(&mu_);
  return stream_;
}

void Action::BindSession(Session* absl_nullable session) {
  act::MutexLock lock(&mu_);
  session_ = session;
}

Session* absl_nullable Action::GetSession() const {
  act::MutexLock lock(&mu_);
  return session_;
}

absl::Status Action::Await(absl::Duration timeout) {
  const absl::Time started_at = absl::Now();

  act::MutexLock lock(&mu_);
  if (has_been_run_) {
    while (!run_status_.has_value()) {
      cv_.WaitWithDeadline(&mu_, started_at + timeout);
    }
    return *run_status_;
  }

  if (has_been_called_) {
    AsyncNode* status_node =
        GetOutputInternal("__status__", /*bind_stream=*/false);

    mu_.Unlock();
    absl::StatusOr<absl::Status> run_status =
        status_node->ConsumeAs<absl::Status>(timeout);
    mu_.Lock();

    if (!run_status.ok()) {
      LOG(ERROR) << absl::StrFormat("Failed to consume status from node %s: %s",
                                    status_node->GetId(),
                                    run_status.status().message());
      return run_status.status();
    }
    return *run_status;
  }

  return absl::FailedPreconditionError(
      "Action has not been run or called yet. Awaiting is only possible "
      "after Run() or Call() has been invoked.");
}

absl::Status Action::Call() {
  act::MutexLock lock(&mu_);
  bind_streams_on_inputs_default_ = true;
  bind_streams_on_outputs_default_ = false;

  RETURN_IF_ERROR(
      stream_->Send(SessionMessage{.actions = {GetActionMessage()}}));

  has_been_called_ = true;
  return absl::OkStatus();
}

absl::Status Action::Run() {
  act::MutexLock lock(&mu_);
  bind_streams_on_inputs_default_ = false;
  bind_streams_on_outputs_default_ = true;

  has_been_run_ = true;

  mu_.Unlock();
  auto handler = std::move(handler_);
  if (handler == nullptr) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Action %s with id=%s has no handler bound. "
                        "Cannot run the action.",
                        schema_.name, id_));
  }
  absl::Status handler_status = std::move(handler)(shared_from_this());
  mu_.Lock();

  for (auto& reader : reffed_readers_) {
    reader->Cancel();
  }
  reffed_readers_.clear();

  UnbindStreams();

  absl::Status full_run_status = handler_status;
  auto handler_status_chunk = ConvertToOrDie<Chunk>(handler_status);
  if (stream_ != nullptr) {
    // If the stream is bound, we send the status chunk to it.
    // We are doing it here instead of relying on a bound stream.
    const auto stream_ptr = stream_;
    mu_.Unlock();
    full_run_status.Update(
        stream_ptr->Send(SessionMessage{.node_fragments = {NodeFragment{
                                            .id = GetOutputId("__status__"),
                                            .chunk = handler_status_chunk,
                                            .seq = 0,
                                            .continued = false,
                                        }}}));
    mu_.Lock();
  }

  AsyncNode* status_node =
      GetOutputInternal("__status__", /*bind_stream=*/false);
  full_run_status.Update(status_node->Put(std::move(handler_status_chunk),
                                          /*seq=*/0, /*final=*/true));

  run_status_ = full_run_status;
  cv_.SignalAll();

  if (!clear_inputs_after_run_ && !clear_outputs_after_run_) {
    // If no clearing is requested, we can return early.
    return full_run_status;
  }

  // Without a node map, we do not need to clear inputs or outputs.
  if (node_map_ == nullptr) {
    return full_run_status;
  }

  if (clear_inputs_after_run_) {
    for (const auto& [input_name, input_id] : input_name_to_id_) {
      node_map_->Extract(input_id).reset();
    }
  }

  if (clear_outputs_after_run_) {
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      node_map_->Extract(output_id).reset();
    }
  }

  return full_run_status;
}

void Action::ClearInputsAfterRun(bool clear) {
  act::MutexLock lock(&mu_);
  clear_inputs_after_run_ = clear;
}

void Action::ClearOutputsAfterRun(bool clear) {
  act::MutexLock lock(&mu_);
  clear_outputs_after_run_ = clear;
}

void Action::Cancel() const {
  act::MutexLock lock(&mu_);
  if (cancelled_->HasBeenNotified()) {
    return;
  }

  for (const auto& [input_name, input_id] : input_name_to_id_) {
    if (AsyncNode* input_node = node_map_->Get(GetInputId(input_name));
        input_node != nullptr) {
      input_node->GetReader().Cancel();
    }
  }

  cancelled_->Notify();
}

bool Action::Cancelled() const {
  act::MutexLock lock(&mu_);
  return cancelled_->HasBeenNotified();
}

std::string Action::GetInputId(const std::string_view name) const {
  return absl::StrCat(id_, "#", name);
}

std::string Action::GetOutputId(const std::string_view name) const {
  return absl::StrCat(id_, "#", name);
}

AsyncNode* absl_nullable Action::GetOutputInternal(
    std::string_view name, const std::optional<bool> bind_stream) {
  if (node_map_ == nullptr) {
    return nullptr;
  }

  if (!output_name_to_id_.contains(name) && name != "__status__") {
    return nullptr;
  }

  AsyncNode* node = node_map_->Get(GetOutputId(name));
  if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_outputs_default_) &&
      name != "__status__") {
    absl::flat_hash_map<std::string, WireStream*> peers;
    peers.insert({std::string(stream_->GetId()), stream_});
    node->BindPeers(std::move(peers));
    nodes_with_bound_streams_.insert(node);
  }
  return node;
}

void Action::UnbindStreams() {
  for (const auto& node : nodes_with_bound_streams_) {
    if (node == nullptr) {
      continue;
    }
    node->BindPeers({});
  }
  nodes_with_bound_streams_.clear();
}

ActionRegistry* Action::GetRegistry() const {
  act::MutexLock lock(&mu_);
  return session_->GetActionRegistry();
}

std::unique_ptr<Action> Action::MakeActionInSameSession(
    const std::string_view name, const std::string_view action_id) const {
  auto action = GetRegistry()->MakeAction(name, action_id);
  concurrency::TwoMutexLock lock(&mu_, &action->mu_);
  action->node_map_ = node_map_;
  action->stream_ = stream_;
  action->session_ = session_;
  return action;
}

}  // namespace act