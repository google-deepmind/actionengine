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

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <boost/system/detail/error_code.hpp>

#include "actionengine/actions/action.h"
#include "actionengine/data/types.h"
#include "actionengine/util/map_util.h"
#include "actionengine/util/status_macros.h"

namespace act {

ActionRegistry::ActionRegistry() {
  Register(kListActionsSchema.name, kListActionsSchema,
           [this](const std::shared_ptr<Action>& action) -> absl::Status {
             AsyncNode* actions_output = action->GetOutput("actions");
             if (actions_output == nullptr) {
               return absl::FailedPreconditionError(
                   "Action has no 'actions' output. Cannot list actions.");
             }

             for (const auto& name : ListRegisteredActions()) {
               boost::json::object schema_obj;
               schema_obj["name"] = boost::json::string(name);
               const ActionSchema& schema = GetSchema(name);

               boost::json::object inputs_obj;
               for (const auto& [input_name, input_type] : schema.inputs) {
                 inputs_obj["name"] = boost::json::string(input_name);
                 inputs_obj["type"] = boost::json::string(input_type);
               }
               schema_obj["inputs"] = std::move(inputs_obj);

               boost::json::object outputs_obj;
               for (const auto& [output_name, output_type] : schema.outputs) {
                 outputs_obj["name"] = boost::json::string(output_name);
                 outputs_obj["type"] = boost::json::string(output_type);
               }
               schema_obj["outputs"] = std::move(outputs_obj);

               schema_obj["description"] =
                   boost::json::string(schema.description);

               RETURN_IF_ERROR(actions_output->Put({
                   .metadata = ChunkMetadata{.mimetype = "application/json"},
                   .data = boost::json::serialize(schema_obj),
               }));
             }

             RETURN_IF_ERROR(actions_output->Put(act::EndOfStream()));
             return absl::OkStatus();
           });
}

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

std::unique_ptr<Action> ActionRegistry::MakeAction(std::string_view name,
                                                   std::string_view action_id,
                                                   std::vector<Port> inputs,
                                                   std::vector<Port> outputs) {

  auto action =
      std::make_unique<Action>(act::FindOrDie(schemas_, name), action_id,
                               std::move(inputs), std::move(outputs));
  action->BindHandler(act::FindOrDie(handlers_, name));
  action->BindRegistry(this);

  return action;
}

const ActionSchema& ActionRegistry::GetSchema(std::string_view name) const {
  return act::FindOrDie(schemas_, name);
}

const ActionHandler& ActionRegistry::GetHandler(std::string_view name) const {
  return act::FindOrDie(handlers_, name);
}

}  // namespace act