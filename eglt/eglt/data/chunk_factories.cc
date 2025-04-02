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
#include <string>
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"

namespace eglt::base {

template <>
Chunk ConstructFrom(std::nullptr_t) {
  return MakeNullChunk();
}

template <>
Chunk ConstructFrom(std::string value) {
  return Chunk{.metadata = {.mimetype = kMimetypeTextPlain},
               .data = std::move(value)};
}

template <>
Chunk ConstructFrom(const std::string_view value) {
  return Chunk{.metadata = {.mimetype = kMimetypeTextPlain},
               .data = std::string(value)};
}

template <>
Chunk ConstructFrom(const char* value) {
  return ConstructFrom<Chunk>(std::string(value));
}

template <>
absl::StatusOr<std::string> MoveAsAndReturnStatus(Chunk value) {
  if (!MimetypeIsTextual(value.metadata.mimetype)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot move as std::string from a non-textual chunk: ",
                     value.metadata.mimetype));
  }
  return std::move(value.data);
}

}  // namespace eglt::base
