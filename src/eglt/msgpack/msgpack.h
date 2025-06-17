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

#ifndef EGLT_MSGPACK_MSGPACK_H
#define EGLT_MSGPACK_MSGPACK_H

// IWYU pragma: begin_exports
#include "eglt/msgpack/array.h"
#include "eglt/msgpack/core_helpers.h"
#include "eglt/msgpack/float.h"
#include "eglt/msgpack/int.h"
#include "eglt/msgpack/misc.h"
#include "eglt/msgpack/strbin.h"
// IWYU pragma: end_exports

namespace eglt::msgpack {

class Packer {
 public:
  explicit Packer(SerializedBytesVector bytes = {})
      : bytes_(std::move(bytes)) {}

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

  void Reset() {
    bytes_.clear();
    offset_ = 0;
  }

  void Reserve(size_t size) { bytes_.reserve(size); }

  [[nodiscard]] const SerializedBytesVector& GetVector() const {
    return bytes_;
  }

  [[nodiscard]] std::vector<Byte> GetStdVector() const {
    return ToStdVector(bytes_);
  }

  SerializedBytesVector ReleaseVector() {
    SerializedBytesVector result = std::move(bytes_);
    bytes_.clear();
    offset_ = 0;
    return result;
  }

  std::vector<Byte> ReleaseStdVector() {
    std::vector<Byte> result = ToStdVector(std::move(bytes_));
    bytes_.clear();
    offset_ = 0;
    return result;
  }

 private:
  SerializedBytesVector bytes_;
  size_t offset_ = 0;
};

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_MSGPACK_H