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

#ifndef ACTIONENGINE_ACTIONS_SCHEMA_H_
#define ACTIONENGINE_ACTIONS_SCHEMA_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include "actionengine/data/types.h"

namespace act {
class Action;
}

/** @file
 *  @brief
 *    A type that represents schemas for ActionEngine actions.
 *
 * This file contains the definition of the ActionSchema class, which is used to
 * define the schema for an action, including its name, input and output
 * parameters with their respective mimetypes. The schema is used to create
 * ActionMessages that can be sent over a SessionMessage in a WireStream to
 * call the action, as well as for well-defined preparation and cleanup.
 */

namespace act {

using ActionHandler =
    std::function<absl::Status(const std::shared_ptr<Action>&)>;

// A type for Python and other bindings that cannot use absl::Status
using VoidActionHandler = std::function<void(const std::shared_ptr<Action>&)>;

using NameAndMimetype = std::pair<std::string, std::string>;
using NameToMimetype = absl::flat_hash_map<std::string, std::string>;

/** A schema for an action.
 *
 * This class defines the schema for an action, including its name, input and
 * output parameters with their respective mimetypes. It provides a method to
 * create an ActionMessage that can be used to call the action by sending it
 * in a @see SessionMessage over a @see WireStream.
 *
 * @headerfile actionengine/actions/schema.h
 */
struct ActionSchema {
  /** @brief
   *    Creates an ActionMessage to create an action instance with the given ID.
   *
   * This method generates an ActionMessage that can be used to call the action
   * by sending it in a SessionMessage over a WireStream. The action ID is used
   * to uniquely identify the action instance.
   *
   * @param action_id
   *   The ID of the action instance. Must not be empty.
   * @return
   *   An ActionMessage representing the action with the given ID.
   */
  [[nodiscard]] ActionMessage GetActionMessage(
      std::string_view action_id) const;

  /** The action's name that is used to register it in the ActionRegistry. */
  std::string name;

  /** A mapping of input names to their mimetypes.
   *
   * The input names are used to identify the input parameters in the action
   * message, and the mimetypes are used to specify the type of data that is
   * expected for each input.
   */
  NameToMimetype inputs;

  /** A mapping of output names to their mimetypes.
   *
   * The output names are used to identify the output parameters in the action
   * message, and the mimetypes are used to specify the type of data that is
   * produced by the action for a given output.
   */
  NameToMimetype outputs;
};

/** @private */
template <typename Sink>
void AbslStringify(Sink& sink, const ActionSchema& schema) {
  std::vector<std::string> input_reprs;
  input_reprs.reserve(schema.inputs.size());
  for (const auto& [name, type] : schema.inputs) {
    input_reprs.push_back(absl::StrCat(name, ":", type));
  }

  std::vector<std::string> output_reprs;
  output_reprs.reserve(schema.outputs.size());
  for (const auto& [name, type] : schema.outputs) {
    output_reprs.push_back(absl::StrCat(name, ":", type));
  }

  absl::Format(&sink, "ActionSchema{name: %s, inputs: %s, outputs: %s}",
               schema.name, absl::StrJoin(input_reprs, ", "),
               absl::StrJoin(output_reprs, ", "));
}

}  // namespace act

#endif  // ACTIONENGINE_ACTIONS_SCHEMA_H_