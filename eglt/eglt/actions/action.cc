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

#include "action.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/util/map_util.h"
#include "eglt/util/random.h"

namespace eglt {
void ActionRegistry::Register(const std::string_view name,
                              const ActionDefinition& def,
                              const ActionHandler& handler) {
  definitions_[name] = def;
  handlers_[name] = handler;
}

base::ActionMessage ActionRegistry::MakeActionMessage(
    const std::string_view name, const std::string_view id) const {
  const ActionDefinition& def = eglt::FindOrDie(definitions_, name);

  std::vector<base::NamedParameter> inputs;
  inputs.reserve(def.inputs.size());
  for (auto& input : def.inputs) {
    inputs.push_back(base::NamedParameter{
        .name = input.name, .id = absl::StrCat(id, "#", input.name)});
  }

  std::vector<base::NamedParameter> outputs;
  outputs.reserve(def.outputs.size());
  for (auto& output : def.outputs) {
    outputs.push_back(base::NamedParameter{
        .name = output.name, .id = absl::StrCat(id, "#", output.name)});
  }

  return base::ActionMessage{
      .name = def.name,
      .inputs = inputs,
      .outputs = outputs,
  };
}

std::unique_ptr<Action> ActionRegistry::MakeAction(
    const std::string_view name, std::string_view id, NodeMap* node_map,
    base::EvergreenStream* stream, Session* session) const {
  std::string action_id = std::string(id);
  if (action_id.empty()) {
    action_id = GenerateUUID4();
  }

  return std::make_unique<Action>(eglt::FindOrDie(definitions_, name),
                                  eglt::FindOrDie(handlers_, name), action_id,
                                  node_map, stream, session);
}

AsyncNode* Action::GetNode(const std::string_view id) const {
  return node_map_->Get(id);
}

base::ActionMessage Action::GetActionMessage() const {
  auto def = GetDefinition();

  // TODO(helenapankov): add action id to the action message, or figure out how
  // to get it from the action without implicit coding into input/output names.

  std::vector<base::NamedParameter> inputs;
  inputs.reserve(def.inputs.size());
  for (auto& [name, _] : def.inputs) {
    inputs.push_back(
        base::NamedParameter{.name = name, .id = absl::StrCat(id_, "#", name)});
  }

  std::vector<base::NamedParameter> outputs;
  outputs.reserve(def.outputs.size());
  for (auto& [name, _] : def.outputs) {
    outputs.push_back(
        base::NamedParameter{.name = name, .id = absl::StrCat(id_, "#", name)});
  }

  return base::ActionMessage{
      .name = def.name,
      .inputs = inputs,
      .outputs = outputs,
  };
}

Action::Action(ActionDefinition def, ActionHandler handler,
               const std::string_view id, NodeMap* node_map,
               base::EvergreenStream* stream, Session* session)
    : def_(std::move(def)),
      handler_(std::move(handler)),
      id_(id),
      node_map_(node_map),
      stream_(stream),
      session_(session) {
  id_ = id.empty() ? GenerateUUID4() : std::string(id);
}

std::unique_ptr<Action> Action::FromActionMessage(
    const base::ActionMessage& action, ActionRegistry* registry,
    NodeMap* node_map, base::EvergreenStream* stream, Session* session) {
  const auto inputs = action.inputs;
  const auto outputs = action.outputs;

  std::string action_id_src;
  if (inputs.empty()) {
    action_id_src = outputs[0].id;
  } else {
    action_id_src = inputs[0].id;
  }
  auto action_id_parts =
      std::vector<std::string>(absl::StrSplit(action_id_src, '#'));
  std::string action_id = action_id_parts[0];

  std::vector<std::string> input_names;
  for (const auto& input : inputs) {
    auto parts = std::vector<std::string>(absl::StrSplit(input.id, '#'));
    input_names.push_back(parts.back());
  }
  std::vector<std::string> output_names;
  for (const auto& output : outputs) {
    auto parts = std::vector<std::string>(absl::StrSplit(output.id, '#'));
    output_names.push_back(parts.back());
  }

  return std::make_unique<Action>(
      /*def=*/registry->GetDefinition(action.name),
      /*handler=*/
      registry->GetHandler(action.name),
      /*id=*/
      action_id, node_map, stream, session);
}
}  // namespace eglt
