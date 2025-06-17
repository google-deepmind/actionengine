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

#ifndef EGLT_MSGPACK_ARRAY_H
#define EGLT_MSGPACK_ARRAY_H

#include "eglt/msgpack/core_helpers.h"

namespace eglt::msgpack {

template <typename T>
absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::vector<T>* absl_nullable) {
  const auto [pos, end, _] = data;

  uint32_t extent = 0;
  uint32_t length = -1;
  auto elements_pos = const_cast<Byte*>(pos);

  if (*pos >= 0x90 && *pos <= 0x9F) {
    length = *pos - 0x90;  // FixArray case
    elements_pos += 1;     // Move past the FixArray header
    extent += 1;           // 1 byte for the FixArray header
  }

  if (*pos == FormatSignature::kArray16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "std::vector<T>");
    }
    length = FromBigEndianBytes<uint16_t>(pos + 1);
    elements_pos += 3;  // Move past the Array16 header
    extent += 3;        // 1 byte for the signature + 2 bytes for length
  }

  if (*pos == FormatSignature::kArray32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "std::vector<T>");
    }
    length = FromBigEndianBytes<uint32_t>(pos + 1);
    elements_pos += 5;  // Move past the Array32 header
    extent += 5;        // 1 byte for the signature + 4 bytes for length
  }

  if (length == -1) {
    return GetInvalidFormatSignatureError(pos, "std::vector<T>", data.begin);
  }

  if (elements_pos >= end) {
    return GetInsufficientDataError(data, "std::vector<T> elements");
  }

  for (uint32_t i = 0; i < length; ++i) {
    if (elements_pos >= end) {
      return GetInsufficientDataError(data, "std::vector<T> elements");
    }
    auto element_extent = GetExtent<T>(elements_pos, end);
    if (!element_extent.ok()) {
      return element_extent.status();
    }
    extent += *element_extent;
    elements_pos += *element_extent;
  }

  return extent;
}

template <typename T>
absl::Status EgltMsgpackSerialize(const std::vector<T>& value,
                                  const InsertInfo& insert) {
  SerializedBytesVector result, encoded_size;

  if (value.size() <= 15) {
    encoded_size = {
        static_cast<Byte>(FormatSignature::kFixArray + value.size())};
  } else if (value.size() <= 65535) {
    encoded_size = ToBigEndianBytes<uint16_t>(value.size(), 1);
    encoded_size[0] = FormatSignature::kArray16;
  } else {
    encoded_size = ToBigEndianBytes<uint32_t>(value.size(), 1);
    encoded_size[0] = FormatSignature::kArray32;
  }

  // value.size() * sizeof(T) is a rough estimate of the size needed for
  // serializing the elements, which may not be exact for all types.
  result.reserve(encoded_size.size() + value.size() * sizeof(T));
  result.insert(result.end(), encoded_size.begin(), encoded_size.end());

  for (const auto& element : value) {
    auto serialized_element = Serialize(element);
    if (!serialized_element.ok()) {
      return serialized_element.status();
    }
    result.insert(result.end(), serialized_element->begin(),
                  serialized_element->end());
  }

  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

template <typename T>
absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, std::vector<T>* absl_nonnull output) {
  const auto [pos, end, _] = data;

  uint32_t length = -1;
  auto elements_pos = const_cast<Byte*>(pos);

  if (*pos >= 0x90 && *pos <= 0x9F) {
    length = *pos - 0x90;  // FixArray case
    elements_pos += 1;     // Move past the FixArray header
  }

  if (*pos == FormatSignature::kArray16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "std::vector<T>");
    }
    length = FromBigEndianBytes<uint16_t>(pos + 1);
    elements_pos += 3;  // Move past the Array16 header
  }

  if (*pos == FormatSignature::kArray32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "std::vector<T>");
    }
    length = FromBigEndianBytes<uint32_t>(pos + 1);
    elements_pos += 5;  // Move past the Array32 header
  }

  if (length == -1) {
    return GetInvalidFormatSignatureError(pos, "std::vector<T>", data.begin);
  }

  if (elements_pos >= end) {
    return GetInsufficientDataError(data, "std::vector<T> elements");
  }

  output->reserve(output->size() + length);
  for (uint32_t i = 0; i < length; ++i) {
    if (elements_pos >= end) {
      return GetInsufficientDataError(data, "std::vector<T> elements");
    }
    auto expected_element_extent =
        EgltMsgpackGetExtent(LookupPointer(elements_pos, end),
                             static_cast<std::vector<T>*>(nullptr));
    if (!expected_element_extent.ok()) {
      return expected_element_extent.status();
    }
    auto element = Deserialize<T>(LookupPointer(elements_pos, end));
    if (!element.ok()) {
      return element.status();
    }
    output->push_back(std::move(element->value));
    DCHECK(*expected_element_extent == element->extent)
        << "Expected extent: " << *expected_element_extent
        << ", actual extent: " << element->extent;
    elements_pos += element->extent;
  }

  return elements_pos - pos;  // Return the total extent of the array.
}

}  // namespace eglt::msgpack

#endif  // EGLT_MSGPACK_ARRAY_H