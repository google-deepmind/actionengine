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

#ifndef ACTIONENGINE_MSGPACK_MSGPACK_H
#define ACTIONENGINE_MSGPACK_MSGPACK_H

#include <cstddef>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/status/status.h>

// IWYU pragma: begin_exports
#include "actionengine/msgpack/array.h"
#include "actionengine/msgpack/core_helpers.h"
#include "actionengine/msgpack/float.h"
#include "actionengine/msgpack/int.h"
#include "actionengine/msgpack/misc.h"
#include "actionengine/msgpack/strbin.h"

// IWYU pragma: end_exports

namespace act::msgpack {

class Packer {
 public:
  explicit Packer(SerializedBytesVector bytes = {});

  template <typename T>
  absl::Status Pack(const T& value) {
    InsertInfo insert{&bytes_, bytes_.end()};
    return EgltMsgpackSerialize(value, insert);
  }

  template <typename T>
  absl::Status Unpack(T& destination) {
    if (offset_ >= bytes_.size()) {
      return absl::InvalidArgumentError("Offset is out of bounds.");
    }
    const auto pos = bytes_.data() + offset_;
    const auto end = bytes_.data() + bytes_.size();
    auto deserialized_extent =
        Deserialize<T>(LookupPointer(pos, end), &destination);
    if (!deserialized_extent.ok()) {
      return deserialized_extent.status();
    }
    offset_ += *deserialized_extent;
    return absl::OkStatus();
  }

  void Reset();

  void Reserve(size_t size);

  [[nodiscard]] const SerializedBytesVector& GetVector() const;

  [[nodiscard]] std::vector<Byte> GetStdVector() const;

  SerializedBytesVector ReleaseVector();

  std::vector<Byte> ReleaseStdVector();

 private:
  SerializedBytesVector bytes_;
  size_t offset_ = 0;
};

}  // namespace act::msgpack

#endif  // ACTIONENGINE_MSGPACK_MSGPACK_H