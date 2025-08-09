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

#include "actionengine/actions/registry.h"

#include <functional>
#include <utility>

#include "actionengine/actions/action.h"
#include "actionengine/data/types.h"
#include "actionengine/util/map_util.h"

namespace act {

void ActionRegistry::Register(std::string_view name, const ActionSchema& schema,
                              const ActionHandler& handler) {
  schemas_[name] = schema;
  handlers_[name] = handler;
}

bool ActionRegistry::IsRegistered(std::string_view name) const {
  return schemas_.contains(name) && handlers_.contains(name);
}

ActionMessage ActionRegistry::MakeActionMessage(std::string_view name,
                                                std::string_view id) const {
  return act::FindOrDie(schemas_, name).GetActionMessage(id);
}

std::unique_ptr<Action> ActionRegistry::MakeAction(
    std::string_view name, std::string_view action_id, std::vector<Port> inputs,
    std::vector<Port> outputs) const {

  auto action =
      std::make_unique<Action>(act::FindOrDie(schemas_, name), action_id,
                               std::move(inputs), std::move(outputs));
  action->BindHandler(act::FindOrDie(handlers_, name));

  return action;
}

const ActionSchema& ActionRegistry::GetSchema(std::string_view name) const {
  return act::FindOrDie(schemas_, name);
}

const ActionHandler& ActionRegistry::GetHandler(std::string_view name) const {
  return act::FindOrDie(handlers_, name);
}

}  // namespace act