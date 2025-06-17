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

#ifndef EGLT_MSGPACK_INT_H
#define EGLT_MSGPACK_INT_H

#include "eglt/msgpack/core_helpers.h"

namespace eglt::msgpack {

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     uint8_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kUInt8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, "uint8_t");
    }
    return 2;  // 1 byte for the signature + 1 byte for the value.
  }

  if (*pos > 0x7f) {
    return GetInvalidFormatSignatureError(pos, "uint8_t", data.begin);
  }

  return 1;
}

inline absl::Status EgltMsgpackSerialize(uint8_t value,
                                         const InsertInfo& insert) {
  if (value <= 0x7f) {
    insert.bytes->insert(insert.at, value);
    return absl::OkStatus();
  }
  insert.bytes->insert(insert.at, {FormatSignature::kUInt8, value});
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, uint8_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos <= 0x7f) {
    *output = *pos;
    return 1;
  }

  if (*pos == FormatSignature::kUInt8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, "uint8_t");
    }
    *output = static_cast<uint8_t>(*(pos + 1));
    return 2;
  }

  return GetInvalidFormatSignatureError(pos, "uint8_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     int8_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kInt8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, "int8_t");
    }
    return 2;  // 1 byte for the signature + 1 byte for the value.
  }

  if (*pos <= 0x7f || *pos >= FormatSignature::kNegativeFixint) {
    return 1;  // Fixint, no additional bytes needed.
  }

  return GetInvalidFormatSignatureError(pos, "int8_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(int8_t value,
                                         const InsertInfo& insert) {
  if (value >= 0 && value <= 0x7f) {
    insert.bytes->insert(insert.at, static_cast<Byte>(value));
    return absl::OkStatus();
  }
  if (value < 0 && value >= -32) {
    insert.bytes->insert(
        insert.at,
        static_cast<Byte>(FormatSignature::kNegativeFixint | (32 + value)));
    return absl::OkStatus();
  }

  insert.bytes->insert(insert.at,
                       {FormatSignature::kInt8, static_cast<Byte>(value)});
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, int8_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos <= 0x7f) {
    *output = static_cast<int8_t>(*pos);
    return 1;
  }

  if (*pos >= FormatSignature::kNegativeFixint) {
    *output = static_cast<int8_t>(-32 + (*pos & 0x1F));
    return 1;
  }

  if (*pos == FormatSignature::kInt8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, "int8_t");
    }
    *output = static_cast<int8_t>(*(pos + 1));
    return 2;
  }

  return GetInvalidFormatSignatureError(pos, "int8_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     uint16_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kUInt16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "uint16_t");
    }
    return 3;  // 1 byte for the signature + 2 bytes for the value.
  }

  if (const auto uint8_extent = GetExtent<uint8_t>(pos, end);
      uint8_extent.ok()) {
    return uint8_extent;
  }

  return GetInvalidFormatSignatureError(pos, "uint16_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(uint16_t value,
                                         const InsertInfo& insert) {
  if (value <= 0x7f) {
    insert.bytes->insert(insert.at, static_cast<Byte>(value));
    return absl::OkStatus();
  }
  if (value <= 0xff) {
    insert.bytes->insert(insert.at,
                         {FormatSignature::kUInt8, static_cast<Byte>(value)});
    return absl::OkStatus();
  }
  auto result = ToBigEndianBytes<uint16_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kUInt16;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, uint16_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (auto deserialized = Deserialize<uint8_t>(data); deserialized.ok()) {
    *output = deserialized->value;
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int8_t>(data); deserialized.ok()) {
    if (deserialized->value < 0) {
      return absl::InvalidArgumentError(
          "Expected a uint16_t value, but found a negative int8_t value.");
    }
    *output = static_cast<uint16_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kUInt16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "uint16_t");
    }
    *output = FromBigEndianBytes<uint16_t>(pos + 1);
    return 3;  // 1 byte for the signature + 2 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "uint16_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     int16_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kInt16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "int16_t");
    }
    return 3;  // 1 byte for the signature + 2 bytes for the value.
  }

  if (const auto uint8_extent = GetExtent<uint8_t>(pos, end);
      uint8_extent.ok()) {
    return uint8_extent;
  }

  if (const auto int8_extent = GetExtent<int8_t>(pos, end); int8_extent.ok()) {
    return int8_extent;
  }

  return GetInvalidFormatSignatureError(pos, "int16_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(int16_t value,
                                         const InsertInfo& insert) {
  if (value >= -128 && value <= 127) {
    return Serialize<int8_t>(static_cast<int8_t>(value), insert);
  }
  if (value >= 128 && value <= 255) {
    return Serialize<uint8_t>(static_cast<uint8_t>(value), insert);
  }
  auto result = ToBigEndianBytes<int16_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kInt16;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, int16_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (auto deserialized = Deserialize<uint8_t>(data); deserialized.ok()) {
    *output = static_cast<int16_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int8_t>(data); deserialized.ok()) {
    *output = static_cast<int16_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kInt16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "int16_t");
    }
    *output = FromBigEndianBytes<int16_t>(pos + 1);
    return 3;  // 1 byte for the signature + 2 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "int16_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     uint32_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kUInt32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "uint32_t");
    }
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  if (const auto uint16_extent = GetExtent<uint16_t>(pos, end);
      uint16_extent.ok()) {
    return uint16_extent;
  }

  return GetInvalidFormatSignatureError(pos, "uint32_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(uint32_t value,
                                         const InsertInfo& insert) {
  if (value <= 0xffff) {
    return Serialize<uint16_t>(static_cast<uint16_t>(value), insert);
  }
  auto result = ToBigEndianBytes<uint32_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kUInt32;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, uint32_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (auto deserialized = Deserialize<uint16_t>(data); deserialized.ok()) {
    *output = static_cast<uint32_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int16_t>(data); deserialized.ok()) {
    if (deserialized->value < 0) {
      return absl::InvalidArgumentError(
          "Expected a uint32_t value, but found a negative int16_t value.");
    }
    *output = static_cast<uint32_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kUInt32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "uint32_t");
    }
    *output = FromBigEndianBytes<uint32_t>(pos + 1);
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "uint32_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                      int32_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kInt32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "int32_t");
    }
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  if (const auto uint16_extent = GetExtent<uint16_t>(pos, end);
      uint16_extent.ok()) {
    return uint16_extent;
  }

  if (const auto int16_extent = GetExtent<int16_t>(pos, end);
      int16_extent.ok()) {
    return int16_extent;
  }

  return GetInvalidFormatSignatureError(pos, "int32_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(int32_t value,
                                         const InsertInfo& insert) {
  if (value >= -32768 && value <= 32767) {
    return Serialize<int16_t>(static_cast<int16_t>(value), insert);
  }
  if (value >= 0 && value <= 0xFFFF) {
    return Serialize<uint16_t>(static_cast<uint16_t>(value), insert);
  }
  auto result = ToBigEndianBytes<int32_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kInt32;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, int32_t* absl_nonnull output) {
  const auto [pos, end, _] = data;

  if (auto deserialized = Deserialize<uint16_t>(data); deserialized.ok()) {
    *output = static_cast<int32_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int16_t>(data); deserialized.ok()) {
    *output = static_cast<int32_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kInt32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "int32_t");
    }
    *output = FromBigEndianBytes<int32_t>(pos + 1);
    return 5;  // 1 byte for the signature + 4 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "int32_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                      uint64_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kUInt64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "uint64_t");
    }
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  if (const auto uint32_extent = GetExtent<uint32_t>(pos, end);
      uint32_extent.ok()) {
    return uint32_extent;
  }

  return GetInvalidFormatSignatureError(pos, "uint64_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(uint64_t value,
                                         const InsertInfo& insert) {
  if (value <= 0xFFFFFFFF) {
    return Serialize<uint32_t>(static_cast<uint32_t>(value), insert);
  }
  auto result = ToBigEndianBytes<uint64_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kUInt64;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, uint64_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (auto deserialized = Deserialize<uint32_t>(data); deserialized.ok()) {
    *output = static_cast<uint64_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int32_t>(data); deserialized.ok()) {
    if (deserialized->value < 0) {
      return absl::InvalidArgumentError(
          "Expected a uint64_t value, but found a negative int32_t value.");
    }
    *output = static_cast<uint64_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kUInt64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "uint64_t");
    }
    *output = FromBigEndianBytes<uint64_t>(pos + 1);
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "uint64_t", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                      int64_t* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kInt64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "int64_t");
    }
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  if (const auto uint32_extent = GetExtent<uint32_t>(pos, end);
      uint32_extent.ok()) {
    return uint32_extent;
  }

  if (const auto int32_extent = GetExtent<int32_t>(pos, end);
      int32_extent.ok()) {
    return int32_extent;
  }

  return GetInvalidFormatSignatureError(pos, "int64_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(int64_t value,
                                         const InsertInfo& insert) {
  if (value >= -2147483648 && value <= 2147483647) {
    return Serialize<int32_t>(static_cast<int32_t>(value), insert);
  }
  if (value >= 0 && value <= 0xFFFFFFFF) {
    return Serialize<uint32_t>(static_cast<uint32_t>(value), insert);
  }
  auto result = ToBigEndianBytes<int64_t>(value, /*pad=*/1);
  result[0] = FormatSignature::kInt64;
  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, int64_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (auto deserialized = Deserialize<uint32_t>(data); deserialized.ok()) {
    *output = static_cast<int64_t>(deserialized->value);
    return deserialized->extent;
  }

  if (auto deserialized = Deserialize<int32_t>(data); deserialized.ok()) {
    *output = static_cast<int64_t>(deserialized->value);
    return deserialized->extent;
  }

  if (*pos == FormatSignature::kInt64) {
    if (end - pos < 9) {
      return GetInsufficientDataError(data, "int64_t");
    }
    *output = FromBigEndianBytes<int64_t>(pos + 1);
    return 9;  // 1 byte for the signature + 8 bytes for the value.
  }

  return GetInvalidFormatSignatureError(pos, "int64_t", data.begin);
}

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_INT_H