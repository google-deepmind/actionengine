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
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/nodes/node_map.h"
#include "eglt/util/map_util.h"

namespace eglt {
void ActionRegistry::Register(std::string_view name, const ActionSchema& schema,
                              const ActionHandler& handler) {
  schemas_[name] = schema;
  handlers_[name] = handler;
}

ActionMessage ActionRegistry::MakeActionMessage(
    const std::string_view action_key, const std::string_view id) const {
  return eglt::FindOrDie(schemas_, action_key).GetActionMessage(id);
}

std::unique_ptr<Action> ActionRegistry::MakeAction(
    std::string_view action_key, std::string_view action_id,
    std::vector<NamedParameter> inputs,
    std::vector<NamedParameter> outputs) const {

  auto action = std::make_unique<Action>(eglt::FindOrDie(schemas_, action_key),
                                         action_id, inputs, outputs);
  action->BindHandler(eglt::FindOrDie(handlers_, action_key));

  return action;
}

AsyncNode* Action::GetNode(const std::string_view id) const {
  return node_map_->Get(id);
}

}  // namespace eglt
