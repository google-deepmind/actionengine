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

#ifndef EGLT_MSGPACK_MISC_H
#define EGLT_MSGPACK_MISC_H

namespace eglt::msgpack {

inline absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                                     absl_nullable bool*) {
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
  return GetInvalidFormatSignatureError(pos, "bool", end);
}

template <typename T>
absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              absl_nullable std::optional<T>*) {
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

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_MISC_H