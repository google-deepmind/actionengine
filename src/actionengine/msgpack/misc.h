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

#ifndef ACTIONENGINE_MSGPACK_MISC_H
#define ACTIONENGINE_MSGPACK_MISC_H

namespace act::msgpack {

struct Timestamp {
  int64_t seconds = 0;
  uint64_t nanos = 0;

  [[nodiscard]] absl::Time ToAbslTime() const {
    return absl::FromUnixSeconds(seconds) + absl::Nanoseconds(nanos);
  }

  template <typename Clock>
  [[nodiscard]] std::chrono::time_point<Clock> ToChronoTimePoint() const {
    return std::chrono::time_point<Clock, std::chrono::nanoseconds>(
        std::chrono::seconds(seconds) + std::chrono::nanoseconds(nanos));
  }
};

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     Timestamp* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFixExt4) {
    if (end - pos < 6) {
      return GetInsufficientDataError(data, "Timestamp");
    }
    return 6;  // 2 bytes for the (ext) signature + 4 bytes for the value.
  }
  if (*pos == FormatSignature::kFixExt8) {
    if (end - pos < 10) {
      return GetInsufficientDataError(data, "Timestamp");
    }
    return 10;  // 2 bytes for the (ext) signature + 8 bytes for the value.
  }
  if (*pos == FormatSignature::kExt8) {
    if (end - pos < 15) {
      return GetInsufficientDataError(data, "Timestamp");
    }
    return 15;  // 3 bytes for the (ext) signature + 4 bytes for nanos,
                // 8 bytes for seconds.
  }
  return GetInvalidFormatSignatureError(pos, "Timestamp", data.begin);
}

inline absl::Status EgltMsgpackSerialize(const Timestamp& value,
                                         const InsertInfo& insert) {
  // Check if the timestamp can be represented as a 32-bit timestamp.
  if (value.seconds >= 0 && value.seconds <= 0xFFFFFFFF && value.nanos == 0) {
    // This is timestamp 32
    SerializedBytesVector result = ToBigEndianBytes<uint32_t>(value.seconds,
                                                              /*pad=*/2);
    result[0] = FormatSignature::kFixExt4;
    result[1] = 255;  // Ext type for Timestamp
    insert.bytes->insert(insert.at, result.begin(), result.end());
    return absl::OkStatus();
  }
  // TODO: timestamp 64
  // timestamp 96
  if (insert.at == insert.bytes->end()) {
    insert.bytes->reserve(insert.bytes->size() + 15);
  }

  insert.bytes->insert(insert.at, {FormatSignature::kExt8, 12, 255});
  auto nanos_bytes =
      ToBigEndianBytes(static_cast<uint32_t>(value.nanos), /*pad=*/0);
  auto seconds_bytes = ToBigEndianBytes(value.seconds, /*pad=*/0);
  insert.bytes->insert(insert.at, nanos_bytes.begin(), nanos_bytes.end());
  insert.bytes->insert(insert.at, seconds_bytes.begin(), seconds_bytes.end());
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, Timestamp* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFixExt4) {
    if (end - pos < 6) {
      return GetInsufficientDataError(data, "Timestamp");
    }
    output->seconds = FromBigEndianBytes<uint32_t>(pos + 2);
    output->nanos = 0;
    return 6;  // 2 bytes for the (ext) signature + 4 bytes for the value.
  }
  if (*pos == FormatSignature::kFixExt8) {
    return absl::UnimplementedError(
        "FixExt8 for Timestamp64 is not yet implemented.");
  }
  if (*pos == FormatSignature::kExt8) {
    if (end - pos < 15) {
      return GetInsufficientDataError(data, "Timestamp");
    }
    output->nanos = FromBigEndianBytes<uint32_t>(pos + 3);
    output->seconds = FromBigEndianBytes<int64_t>(pos + 7);
    return 15;  // 3 bytes for the (ext) signature + 4 bytes for nanos,
    // 8 bytes for seconds.
  }
  return GetInvalidFormatSignatureError(pos, "Timestamp", data.begin);
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(
    const LookupPointer& data, absl::Time* absl_nullable) {
  return EgltMsgpackGetExtent(data, static_cast<Timestamp*>(nullptr));
}

inline absl::Status EgltMsgpackSerialize(absl::Time value,
                                         const InsertInfo& insert) {
  const uint64_t nanos = absl::ToUnixNanos(value) % 1000000000;
  return EgltMsgpackSerialize(Timestamp{absl::ToUnixSeconds(value), nanos},
                              insert);
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, absl::Time* absl_nonnull output) {
  Timestamp timestamp;
  auto status = EgltMsgpackDeserialize(data, &timestamp);
  if (!status.ok()) {
    return status;
  }
  *output = timestamp.ToAbslTime();
  return status;
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     bool* absl_nullable) {
  const auto [pos, end, _] = data;
  if (pos >= end) {
    return absl::InvalidArgumentError(
        "Position is not within the bounds of the data.");
  }
  if (*pos == FormatSignature::kFalse || *pos == FormatSignature::kTrue) {
    return 1;
  }
  return absl::InvalidArgumentError(
      "Expected a boolean value, but found a different format signature.");
}

inline absl::Status EgltMsgpackSerialize(bool value, const InsertInfo& insert) {
  if (value) {
    insert.bytes->insert(insert.at, FormatSignature::kTrue);
    return absl::OkStatus();
  }
  insert.bytes->insert(insert.at, FormatSignature::kFalse);
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, bool* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kFalse) {
    *output = false;
    return absl::OkStatus();
  }
  if (*pos == FormatSignature::kTrue) {
    *output = true;
    return absl::OkStatus();
  }
  return GetInvalidFormatSignatureError(pos, "bool", data.begin);
}

template <typename T>
absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::optional<T>* absl_nullable) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kNil) {
    return 1;
  }
  return GetExtent<T>(pos, end);
}

template <typename T>
absl::Status EgltMsgpackSerialize(const std::optional<T>& value,
                                  const InsertInfo& insert) {
  if (!value) {
    insert.bytes->insert(insert.at, FormatSignature::kNil);
    return absl::OkStatus();
  }
  return Serialize<T>(*value, insert);
}

template <typename T>
absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, std::optional<T>* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kNil) {
    *output = std::nullopt;
  }
  *output = Deserialize<T>(pos, end);
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(
    const LookupPointer& data, std::nullopt_t* absl_nullable) {
  if (data.pos >= data.end) {
    return absl::InvalidArgumentError(
        "Position is not within the bounds of the data.");
  }
  if (*data.pos == FormatSignature::kNil) {
    return 1;  // 1 byte for the nil signature.
  }
  return GetInvalidFormatSignatureError(data.pos, "std::nullopt_t", data.begin);
}

inline absl::Status EgltMsgpackSerialize(std::nullopt_t,
                                         const InsertInfo& insert) {
  insert.bytes->insert(insert.at, FormatSignature::kNil);
  return absl::OkStatus();
}

inline absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, std::nullopt_t* absl_nonnull output) {
  const auto [pos, end, _] = data;
  if (*pos == FormatSignature::kNil) {
    *output = std::nullopt;
    return 1;  // 1 byte for the nil signature.
  }
  return GetInvalidFormatSignatureError(pos, "std::nullopt_t", data.begin);
}

}  // namespace act::msgpack

#endif  // ACTIONENGINE_MSGPACK_MISC_H