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

#ifndef EGLT_MSGPACK_CORE_HELPERS_H_
#define EGLT_MSGPACK_CORE_HELPERS_H_

#include <any>
#include <bit>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

namespace eglt::msgpack {

using Byte = uint8_t;
// Using absl::InlinedVector to optimize for small sizes; 9 is chosen
// to accomodate a format byte and a typical 64-bit value.
using SerializedBytesVector = absl::InlinedVector<Byte, 9>;

inline std::vector<Byte> ToStdVector(SerializedBytesVector bytes) {
  return {std::make_move_iterator(bytes.begin()),
          std::make_move_iterator(bytes.end())};
}

struct InsertInfo {
  SerializedBytesVector* absl_nonnull bytes;
  SerializedBytesVector::iterator at;
};

struct LookupPointer {
  Byte* absl_nonnull pos;
  Byte* absl_nonnull end;
  Byte* absl_nullable begin;

  LookupPointer(Byte* absl_nonnull pos, Byte* absl_nonnull end,
                Byte* absl_nullable begin = nullptr)
      : pos(pos), end(end), begin(begin) {}

  bool operator==(const LookupPointer& other) const {
    return pos == other.pos && end == other.end && begin == other.begin;
  }
};

template <typename T>
struct Deserialized {
  T value;
  uint32_t extent;
};

template <typename T>
SerializedBytesVector ToBigEndianBytes(const T& value, uint8_t pad = 0) {
  SerializedBytesVector bytes(sizeof(T) + pad);
  if constexpr (std::endian::native == std::endian::little) {
    for (size_t i = 0; i < sizeof(T); ++i) {
      bytes[i + pad] = (value >> ((sizeof(T) - 1 - i) * 8)) & 0xFF;
    }
  } else {
    for (size_t i = 0; i < sizeof(T); ++i) {
      bytes[i + pad] = (value >> (i * 8)) & 0xFF;
    }
  }
  return bytes;
}

#if __STDCPP_FLOAT32_T__ != 1
using float32_t = float;
using float64_t = double;
#endif

template <>
inline SerializedBytesVector ToBigEndianBytes<float32_t>(const float32_t& value,
                                                         uint8_t pad) {
  return ToBigEndianBytes<uint32_t>(reinterpret_cast<const uint32_t&>(value),
                                    pad);
}

template <>
inline SerializedBytesVector ToBigEndianBytes<float64_t>(const float64_t& value,
                                                         uint8_t pad) {
  return ToBigEndianBytes<uint64_t>(reinterpret_cast<const uint64_t&>(value),
                                    pad);
}

template <typename T>
T FromBigEndianBytes(const Byte* absl_nonnull bytes) {
  if constexpr (std::endian::native == std::endian::big) {
    return *reinterpret_cast<T*>(bytes);
  }
  absl::InlinedVector<Byte, sizeof(T)> reversed_bytes(sizeof(T));
  for (size_t i = 0; i < sizeof(T); ++i) {
    reversed_bytes[i] = bytes[sizeof(T) - 1 - i];
  }
  return *reinterpret_cast<T*>(reversed_bytes.data());
}

enum FormatSignature {
  kPositiveFixint = 0b00000000,  // 0x00 to 0x7F
  kFixMap = 0b10000000,          // 0x80 to 0x8F
  kFixArray = 0b10010000,        // 0x90 to 0x9F
  kFixStr = 0b10100000,          // 0xA0 to 0xBF
  kNil = 0b11000000,             // 0xC0
  kNeverUsed = 0b11000001,       // Reserved for future use, should not be used
  kFalse = 0b11000010,           // 0xC2
  kTrue = 0b11000011,            // 0xC3
  kBin8 = 0b11000100,            // 0xC4
  kBin16 = 0b11000101,           // 0xC5
  kBin32 = 0b11000110,           // 0xC6
  kExt8 = 0b11000111,            // 0xC7
  kExt16 = 0b11001000,           // 0xC8
  kExt32 = 0b11001001,           // 0xC9
  kFloat32 = 0b11001010,         // 0xCA
  kFloat64 = 0b11001011,         // 0xCB
  kUInt8 = 0b11001100,           // 0xCC
  kUInt16 = 0b11001101,          // 0xCD
  kUInt32 = 0b11001110,          // 0xCE
  kUInt64 = 0b11001111,          // 0xCF
  kInt8 = 0b11010000,            // 0xD0
  kInt16 = 0b11010001,           // 0xD1
  kInt32 = 0b11010010,           // 0xD2
  kInt64 = 0b11010011,           // 0xD3
  kFixExt1 = 0b11010100,         // 0xD4
  kFixExt2 = 0b11010101,         // 0xD5
  kFixExt4 = 0b11010110,         // 0xD6
  kFixExt8 = 0b11010111,         // 0xD7
  kFixExt16 = 0b11011000,        // 0xD8
  kStr8 = 0b11011001,            // 0xD9
  kStr16 = 0b11011010,           // 0xDA
  kStr32 = 0b11011011,           // 0xDB
  kArray16 = 0b11011100,         // 0xDC
  kArray32 = 0b11011101,         // 0xDD
  kMap16 = 0b11011110,           // 0xDE
  kMap32 = 0b11011111,           // 0xDF
  kNegativeFixint = 0b11100000   // 0xE0 to 0xFF, two's complement
};

inline std::string GetPositionString(
    const Byte* absl_nonnull pos, const Byte* absl_nullable begin = nullptr) {
  if (begin != nullptr) {
    return absl::StrFormat("%d", pos - begin);
  }
  return absl::StrFormat("0x%02x", *pos);
}

inline absl::Status GetInvalidFormatSignatureError(
    const Byte* absl_nonnull pos, std::string_view type_name,
    const Byte* absl_nullable begin = nullptr) {

  return absl::InvalidArgumentError(absl::StrCat(
      "Expected a ", type_name,
      " value, but found a different format signature at position ",
      GetPositionString(pos, begin), ": ", absl::StrFormat("0x%02x", *pos)));
}

inline absl::Status GetInsufficientDataError(const LookupPointer& data,
                                             std::string_view type_name) {
  return absl::InvalidArgumentError(absl::StrCat(
      "Insufficient data to read a ", type_name, " value at position ",
      GetPositionString(data.pos, data.begin), ": ",
      absl::StrFormat("0x%02x", *data.pos), ". Only have ",
      absl::StrFormat("%d", data.end - data.pos), " bytes."));
}

// Forward declarations for EgltMsgpackGetExtent, Serialize, and Deserialize.
template <typename T>
absl::StatusOr<uint32_t> GetExtent(Byte* absl_nonnull pos,
                                   Byte* absl_nonnull end) {
  if (pos >= end) {
    return absl::InvalidArgumentError(
        "Position is not within the bounds of the data.");
  }

  if (*pos == FormatSignature::kNeverUsed) {
    return absl::InvalidArgumentError(
        "FormatSignature::kNeverUsed is not a valid format signature.");
  }

  return EgltMsgpackGetExtent(LookupPointer(pos, end),
                              static_cast<T*>(nullptr));
}

template <typename T>
absl::StatusOr<SerializedBytesVector> Serialize(T&& value) {
  SerializedBytesVector output;
  auto status = EgltMsgpackSerialize(std::forward<T>(value),
                                     InsertInfo{&output, output.end()});
  if (!status.ok()) {
    return status;
  }
  return output;
}

template <typename T>
absl::Status Serialize(T&& value, const InsertInfo& insert) {
  return EgltMsgpackSerialize(std::forward<T>(value), insert);
}

template <typename T>
concept HasDefinedGetExtent = requires(T t) {
  {
    EgltMsgpackGetExtent(std::declval<LookupPointer>(), std::declval<T*>())
  } -> std::same_as<absl::StatusOr<uint32_t>>;
};

template <typename T>
absl::StatusOr<uint32_t> Deserialize(const LookupPointer& data,
                                     T* absl_nonnull output)
    requires(!HasDefinedGetExtent<T>) {
  const auto [pos, end, _] = data;
  if (pos >= end) {
    return absl::InvalidArgumentError(
        "Position is not within the bounds of the data.");
  }

  if (*pos == FormatSignature::kNeverUsed) {
    return absl::InvalidArgumentError(
        "FormatSignature::kNeverUsed is not a valid format signature.");
  }

  auto extent = EgltMsgpackDeserialize(LookupPointer(pos, end), output);
  if (!extent.ok()) {
    return extent.status();
  }
  return *extent;
}

template <typename T>
absl::StatusOr<uint32_t> Deserialize(const LookupPointer& data,
                                     T* absl_nonnull output)
    requires(HasDefinedGetExtent<T>) {
  const auto [pos, end, _] = data;
  if (pos >= end) {
    return absl::InvalidArgumentError(
        "Position is not within the bounds of the data.");
  }

  if (*pos == FormatSignature::kNeverUsed) {
    return absl::InvalidArgumentError(
        "FormatSignature::kNeverUsed is not a valid format signature.");
  }

#ifndef NDEBUG
  absl::StatusOr<uint32_t> expected_extent = GetExtent<T>(pos, end);
  if (!expected_extent.ok()) {
    return expected_extent.status();
  }
#endif  // NDEBUG

  auto actual_extent = EgltMsgpackDeserialize(LookupPointer(pos, end), output);
  if (!actual_extent.ok()) {
    return actual_extent.status();
  }

#ifndef NDEBUG
  if (*expected_extent != *actual_extent) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Expected extent: %d, actual extent: %d at position %s",
        *expected_extent, *actual_extent, GetPositionString(pos, data.begin)));
  }
#endif  // NDEBUG

  return *actual_extent;
}

template <typename T>
absl::StatusOr<Deserialized<T>> Deserialize(const LookupPointer& data) {
  Deserialized<T> deserialized;
  deserialized.extent = 0;
  auto status_or_extent = Deserialize(data, &deserialized.value);
  if (!status_or_extent.ok()) {
    return status_or_extent.status();
  }
  deserialized.extent = *status_or_extent;
  return deserialized;
}

// These are unimplemented functions that should be defined on platforms
// that use non-IEC559 floating-point types. They are placeholders for
// future implementations and should not be used in the current codebase.
absl::Status SerializeNonIec559Float32(float32_t value,
                                       const InsertInfo& insert);
absl::Status SerializeNonIec559Float64(float64_t value,
                                       const InsertInfo& insert);

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_CORE_HELPERS_H_