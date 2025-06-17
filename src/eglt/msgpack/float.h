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

#ifndef EGLT_MSGPACK_FLOAT_H
#define EGLT_MSGPACK_FLOAT_H

#include <limits>

#include "eglt/msgpack/core_helpers.h"

namespace eglt::msgpack {

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     float32_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFloat32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "float32_t");
    }
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  if (const auto int64_extent = GetExtent<int64_t>(pos, end);
      int64_extent.ok()) {
    return int64_extent;
  }

  if (const auto uint64_extent = GetExtent<uint64_t>(pos, end);
      uint64_extent.ok()) {
    return uint64_extent;
  }

  return GetInvalidFormatSignatureError(pos, "float32_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(float32_t value,
                                         const InsertInfo& insert) {
  if constexpr (!std::numeric_limits<float32_t>::is_iec559) {
    return SerializeNonIec559Float32(value, insert);
  }
  auto result = ToBigEndianBytes<float32_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kFloat32;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, float32_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFloat32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "float32_t");
    }
    *output = FromBigEndianBytes<float32_t>(pos + 1);
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  if (*pos == FormatSignature::kFloat64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "float32_t");
    }
    *output = static_cast<float32_t>(FromBigEndianBytes<float64_t>(pos + 1));
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  if (auto deserialized = Deserialize<int64_t>(data); deserialized.ok()) {
    *output = static_cast<float32_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<uint64_t>(data); deserialized.ok()) {
    *output = static_cast<float32_t>(deserialized->value);
    return deserialized->extent;
  }

  return GetInvalidFormatSignatureError(pos, "float32_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     float64_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFloat64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "float64_t");
    }
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  if (const auto float32_extent = GetExtent<float32_t>(pos, end);
      float32_extent.ok()) {
    return float32_extent;
  }

  if (const auto int64_extent = GetExtent<int64_t>(pos, end);
      int64_extent.ok()) {
    return int64_extent;
  }

  if (const auto uint64_extent = GetExtent<uint64_t>(pos, end);
      uint64_extent.ok()) {
    return uint64_extent;
  }

  return GetInvalidFormatSignatureError(pos, "float64_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(float64_t value,
                                         const InsertInfo& insert) {
  if constexpr (!std::numeric_limits<float64_t>::is_iec559) {
    return SerializeNonIec559Float64(value, insert);
  }
  auto result = ToBigEndianBytes<float64_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kFloat64;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, float64_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFloat64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "float64_t");
    }
    *output = FromBigEndianBytes<float64_t>(pos + 1);
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  if (*pos == FormatSignature::kFloat32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "float64_t");
    }
    *output = static_cast<float64_t>(FromBigEndianBytes<float32_t>(pos + 1));
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  if (auto deserialized = Deserialize<int64_t>(data); deserialized.ok()) {
    *output = static_cast<float64_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<uint64_t>(data); deserialized.ok()) {
    *output = static_cast<float64_t>(deserialized->value);
    return deserialized->extent;
  }

  return GetInvalidFormatSignatureError(pos, "float64_t", data.begin);
}

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_FLOAT_H