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

#include <cstddef>
#include <string_view>

#include "eglt/data/eg_structs.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

namespace eglt::base {

std::vector<std::string> Indent(std::vector<std::string> fields, int num_spaces,
                                bool indent_first_line) {
  if (fields.empty()) {
    return fields;
  }

  std::vector<std::string> result = std::move(fields);
  const size_t start_index = indent_first_line ? 0 : 1;

  for (size_t index = start_index; index < result.size(); ++index) {
    result[index] = absl::StrCat(std::string(num_spaces, ' '), result[index]);
  }

  return result;
}

std::string Indent(std::string field, int num_spaces, bool indent_first_line) {
  const std::vector<std::string> lines = Indent(
      absl::StrSplit(std::move(field), '\n'), num_spaces, indent_first_line);

  return absl::StrJoin(lines, "\n",
                       [](std::string* out, const std::string_view line) {
                         absl::StrAppend(out, line);
                       });
}

}  // namespace eglt::base
