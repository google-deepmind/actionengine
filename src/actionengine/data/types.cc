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

#include "actionengine/data/types.h"

#include <cstddef>
#include <string_view>

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>

namespace act::internal {
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
}  // namespace act::internal

namespace act {

absl::Status EgltAssignInto(Chunk chunk, std::string* string) {
  if (const std::string chunk_mimetype = chunk.GetMimetype();
      !MimetypeIsTextual(chunk_mimetype)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot move as std::string from a non-textual chunk: ",
                     chunk_mimetype));
  }
  *string = std::move(chunk.data);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(std::string string, Chunk* chunk) {
  chunk->metadata = ChunkMetadata{
      .mimetype = kMimetypeTextPlain,
      .timestamp = absl::Now(),
  };
  chunk->data = std::move(string);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(const Chunk& chunk, absl::Status* status) {
  if (const std::string chunk_mimetype = chunk.GetMimetype();
      chunk_mimetype != "__status__") {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid mimetype: ", chunk_mimetype));
  }
  if (chunk.data.empty()) {
    return absl::InvalidArgumentError(absl::StrCat("Empty data: ", chunk.data));
  }
  std::string message;
  int raw_code = static_cast<uint8_t>(chunk.data[0]);
  if (chunk.data.size() > 1) {
    message = chunk.data.substr(1);
  }

  *status = absl::Status(static_cast<absl::StatusCode>(raw_code), message);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(const absl::Status& status, Chunk* chunk) {
  chunk->metadata = ChunkMetadata{
      .mimetype = "__status__",
      .timestamp = absl::Now(),
  };
  chunk->data = absl::StrCat(" ", status.message());
  chunk->data[0] = static_cast<uint8_t>(status.raw_code());
  return absl::OkStatus();
}

}  // namespace act
