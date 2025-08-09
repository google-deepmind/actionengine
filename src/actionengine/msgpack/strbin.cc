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

#include "actionengine/msgpack/strbin.h"

namespace act::msgpack {

absl::StatusOr<uint32_t> GetStrOrBinExtent(const LookupPointer& data,
                                           std::string_view type_for_error) {
  const auto [pos, end, _] = data;

  // check for fixstr:
  if (*pos >= 0xA0 && *pos <= 0xBF) {
    const uint32_t extent = *pos - 0xA0 + 1;
    if (end - pos < extent) {
      return GetInsufficientDataError(data, type_for_error);
    }
    return extent;
  }

  // check for str8 and bin8
  if (*pos == FormatSignature::kStr8 || *pos == FormatSignature::kBin8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, type_for_error);
    }
    const uint8_t length = *(pos + 1);
    if (end - pos < 2 + length) {
      return GetInsufficientDataError(data, type_for_error);
    }
    return 2 + length;
  }

  // check for str16 and bin16
  if (*pos == FormatSignature::kStr16 || *pos == FormatSignature::kBin16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, type_for_error);
    }
    const auto length = FromBigEndianBytes<uint16_t>(pos + 1);
    if (end - pos < 3 + length) {
      return GetInsufficientDataError(data, type_for_error);
    }
    return 3 + length;
  }

  // check for str32 and bin32
  if (*pos == FormatSignature::kStr32 || *pos == FormatSignature::kBin32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, type_for_error);
    }
    const auto length = FromBigEndianBytes<uint32_t>(pos + 1);
    if (end - pos < 5 + length) {
      return GetInsufficientDataError(data, type_for_error);
    }
    return 5 + length;
  }

  return GetInvalidFormatSignatureError(pos, type_for_error, data.begin);
}

absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::string*) {
  return GetStrOrBinExtent(data, "std::string");
}

absl::Status EgltMsgpackSerialize(const std::string& value,
                                  const InsertInfo& insert) {
  SerializedBytesVector result, encoded_size;

  if (value.size() <= 31) {
    encoded_size = {static_cast<Byte>(FormatSignature::kFixStr + value.size())};
  } else if (value.size() <= 255) {
    encoded_size = {FormatSignature::kStr8, static_cast<Byte>(value.size())};
  } else if (value.size() <= 65535) {
    encoded_size = ToBigEndianBytes<uint16_t>(value.size(), /*pad=*/1);
    encoded_size[0] = FormatSignature::kStr16;
  } else {
    encoded_size = ToBigEndianBytes<uint32_t>(value.size(), /*pad=*/1);
    encoded_size[0] = FormatSignature::kStr32;
  }

  result.reserve(encoded_size.size() + value.size());
  result.insert(result.end(), encoded_size.begin(), encoded_size.end());
  result.insert(result.end(), value.begin(), value.end());

  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

absl::StatusOr<uint32_t> EgltMsgpackDeserializeBin(const LookupPointer& data,
                                                   std::vector<Byte>* output) {
  const auto [pos, end, _] = data;
  if (const auto expected_extent = GetStrOrBinExtent(data, "std::vector<Byte>");
      !expected_extent.ok()) {
    return expected_extent.status();
  }

  if (*pos >= 0xA0 && *pos <= 0xBF) {
    // Fixstr case
    const uint32_t length = *pos - 0xA0;
    if (end - pos < length + 1) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    *output = std::vector(pos + 1, pos + 1 + length);
    return 1 + length;
  }

  if (*pos == FormatSignature::kBin8 || *pos == FormatSignature::kStr8) {
    if (end - pos < 2) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    const uint8_t length = *(pos + 1);
    if (end - pos < 2 + length) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    *output = std::vector(pos + 2, pos + 2 + length);
    return 2 + length;
  }

  if (*pos == FormatSignature::kBin16 || *pos == FormatSignature::kStr16) {
    if (end - pos < 3) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    const auto length = FromBigEndianBytes<uint16_t>(pos + 1);
    if (end - pos < 3 + length) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    *output = std::vector(pos + 3, pos + 3 + length);
    return 3 + length;
  }

  if (*pos == FormatSignature::kBin32 || *pos == FormatSignature::kStr32) {
    if (end - pos < 5) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    const auto length = FromBigEndianBytes<uint32_t>(pos + 1);
    if (end - pos < 5 + length) {
      return GetInsufficientDataError(data, "std::vector<Byte>");
    }
    *output = std::vector(pos + 5, pos + 5 + length);
    return 5 + length;
  }

  return absl::InvalidArgumentError(
      "Expected a binary format signature for deserialization.");
}

absl::StatusOr<uint32_t> EgltMsgpackDeserialize(const LookupPointer& data,
                                                std::string* output) {
  std::vector<Byte> bin_result;
  auto deserialized_extent = EgltMsgpackDeserializeBin(data, &bin_result);
  if (!deserialized_extent.ok()) {
    return deserialized_extent.status();
  }
  *output = std::string(std::make_move_iterator(bin_result.begin()),
                        std::make_move_iterator(bin_result.end()));
  return *deserialized_extent;
}

absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::vector<Byte>*) {
  return GetStrOrBinExtent(data, "std::vector<uint8_t>");
}

absl::Status EgltMsgpackSerialize(const std::vector<uint8_t>& value,
                                  const InsertInfo& insert) {
  SerializedBytesVector result, encoded_size;

  if (value.size() <= 255) {
    encoded_size = {FormatSignature::kBin8, static_cast<Byte>(value.size())};
  } else if (value.size() <= 65535) {
    encoded_size = ToBigEndianBytes<uint16_t>(value.size(), /*pad=*/1);
    encoded_size[0] = FormatSignature::kBin16;
  } else {
    encoded_size = ToBigEndianBytes<uint32_t>(value.size(), /*pad=*/1);
    encoded_size[0] = FormatSignature::kBin32;
  }

  result.reserve(encoded_size.size() + value.size());
  result.insert(result.end(), encoded_size.begin(), encoded_size.end());
  result.insert(result.end(), value.begin(), value.end());

  insert.bytes->insert(insert.at, result.begin(), result.end());
  return absl::OkStatus();
}

absl::StatusOr<uint32_t> EgltMsgpackDeserialize(const LookupPointer& data,
                                                std::vector<uint8_t>* output) {
  std::vector<Byte> bin_result;
  auto deserialized_extent = EgltMsgpackDeserializeBin(data, &bin_result);
  if (!deserialized_extent.ok()) {
    return deserialized_extent.status();
  }
  *output = std::vector(std::make_move_iterator(bin_result.begin()),
                        std::make_move_iterator(bin_result.end()));
  return *deserialized_extent;
}

}  // namespace act::msgpack