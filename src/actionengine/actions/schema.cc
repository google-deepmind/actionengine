// Copyright 2026 Google LLC
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

#include "actionengine/actions/schema.h"

#include <absl/log/check.h>

#include "actionengine/data/types.h"

namespace act {

absl::StatusOr<ActionMessage> ActionSchema::GetActionMessage(
    std::string_view action_id) const {
  if (action_id.empty()) {
    return absl::InvalidArgumentError(
        "Action ID cannot be empty to create a message");
  }

  std::vector<Port> input_parameters;
  input_parameters.reserve(inputs.size());
  for (const auto& [input_name, _] : inputs) {
    input_parameters.push_back(Port{
        .name = input_name,
        .id = absl::StrCat(action_id, "#", input_name),
    });
  }

  std::vector<Port> output_parameters;
  output_parameters.reserve(outputs.size());
  for (const auto& [output_name, _] : outputs) {
    output_parameters.push_back(Port{
        .name = output_name,
        .id = absl::StrCat(action_id, "#", output_name),
    });
  }

  return ActionMessage{
      .id = std::string(action_id),
      .name = name,
      .inputs = input_parameters,
      .outputs = output_parameters,
  };
}

}  // namespace act