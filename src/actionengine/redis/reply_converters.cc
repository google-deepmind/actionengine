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

#include "actionengine/redis/reply_converters.h"

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include "actionengine/redis/reply.h"

namespace act::redis {

absl::Status EgltAssignInto(const Reply& from, absl::Status* to) {
  if (from.IsStatus()) {
    *to = std::get<StatusReplyData>(from.data).AsAbslStatus();
    return absl::OkStatus();
  }
  if (from.IsError()) {
    *to = std::get<ErrorReplyData>(from.data).AsAbslStatus();
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to absl::Status"));
}

absl::Status EgltAssignInto(const Reply& from, int64_t* to) {
  if (from.type == ReplyType::Integer) {
    *to = std::get<IntegerReplyData>(from.data).value;
    return absl::OkStatus();
  }
  if (from.type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(from.data).value;
    if (!absl::SimpleAtoi(str, to)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Cannot convert string to int64_t: ", str));
    }
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to int64_t"));
}

absl::Status EgltAssignInto(const Reply& from, double* to) {
  if (from.type == ReplyType::Double) {
    *to = std::get<DoubleReplyData>(from.data).value;
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Integer) {
    *to = static_cast<double>(std::get<IntegerReplyData>(from.data).value);
    return absl::OkStatus();
  }
  if (from.type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(from.data).value;
    if (!absl::SimpleAtod(str, to)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Cannot convert string to double: ", str));
    }
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to double"));
}

absl::Status EgltAssignInto(Reply from, std::string* to) {
  Reply moved = std::move(from);

  if (moved.IsString()) {
    *to = std::move(std::get<StringReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Verbatim) {
    *to = std::move(std::get<VerbatimReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Error) {
    *to = std::move(std::get<ErrorReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Status) {
    *to = std::move(std::get<StatusReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Nil) {
    *to = "";
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::BigNum) {
    *to = std::move(std::get<BigNumReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Integer) {
    *to = absl::StrCat(std::get<IntegerReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Bool) {
    *to = std::get<BoolReplyData>(moved.data).value ? "1" : "0";
    return absl::OkStatus();
  }
  if (moved.type == ReplyType::Double) {
    *to = absl::StrCat(std::get<DoubleReplyData>(moved.data).value);
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(moved.type), " to std::string"));
}

absl::Status EgltAssignInto(const Reply& from, bool* to) {
  if (from.type == ReplyType::Bool) {
    *to = std::get<BoolReplyData>(from.data).value;
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Integer) {
    *to = std::get<IntegerReplyData>(from.data).value != 0;
    return absl::OkStatus();
  }
  if (from.type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(from.data).value;
    if (str == "1" || absl::EqualsIgnoreCase(str, "true")) {
      *to = true;
      return absl::OkStatus();
    }
    if (str == "0" || absl::EqualsIgnoreCase(str, "false")) {
      *to = false;
      return absl::OkStatus();
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot convert string to bool: ", str));
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to bool"));
}

absl::Status EgltAssignInto(Reply from, ArrayReplyData* to) {
  if (from.type == ReplyType::Array) {
    *to = std::move(std::get<ArrayReplyData>(from.data));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Set) {
    *to = ArrayReplyData{
        .values = std::move(std::get<SetReplyData>(from.data).values)};
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Push) {
    *to = std::move(std::get<PushReplyData>(from.data).value_array);
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Map) {
    MapReplyData map_reply_data = std::move(std::get<MapReplyData>(from.data));
    ASSIGN_OR_RETURN(*to, ConvertTo<ArrayReplyData>(std::move(map_reply_data)));
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to ArrayReplyData"));
}

absl::Status EgltAssignInto(Reply from, MapReplyData* to) {
  if (from.type == ReplyType::Map) {
    *to = std::move(std::get<MapReplyData>(from.data));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Array) {
    const ArrayReplyData& array = std::get<ArrayReplyData>(from.data);
    ASSIGN_OR_RETURN(*to, ConvertTo<MapReplyData>(array));
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to MapReplyData"));
}

absl::Status EgltAssignInto(Reply from, SetReplyData* to) {
  if (from.type == ReplyType::Set) {
    *to = std::move(std::get<SetReplyData>(from.data));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Array) {
    *to = SetReplyData{
        .values = std::move(std::get<ArrayReplyData>(from.data).values)};
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to SetReplyData"));
}

absl::Status EgltAssignInto(Reply from, PushReplyData* to) {
  if (from.type == ReplyType::Push) {
    *to = std::move(std::get<PushReplyData>(from.data));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Array) {
    *to = PushReplyData{.value_array =
                            std::move(std::get<ArrayReplyData>(from.data))};
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to PushReplyData"));
}

absl::Status EgltAssignInto(Reply from, VerbatimReplyData* to) {
  if (from.type == ReplyType::Verbatim) {
    *to = std::move(std::get<VerbatimReplyData>(from.data));
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot convert reply of type ",
                   MapReplyEnumToTypeName(from.type), " to VerbatimReplyData"));
}

absl::Status EgltAssignInto(ArrayReplyData from, MapReplyData* to) {
  if (from.values.size() % 2 != 0) {
    return absl::InvalidArgumentError(
        "Array length is not even, cannot convert to MapReplyData");
  }
  absl::flat_hash_map<std::string, Reply> map;
  for (size_t i = 0; i < from.values.size(); i += 2) {
    if (!from.values[i].IsString()) {
      return absl::InvalidArgumentError(
          "Cannot convert ArrayReplyData to MapReplyData, "
          "all keys must be strings.");
    }
    map.emplace(std::move(std::get<StringReplyData>(from.values[i].data).value),
                std::move(from.values[i + 1]));
  }
  *to = MapReplyData{.values = std::move(map)};
  return absl::OkStatus();
}

absl::Status EgltAssignInto(MapReplyData from, ArrayReplyData* to) {
  to->values.reserve(from.values.size() * 2);
  for (const auto& [key, value] : from.values) {
    to->values.emplace_back(
        Reply{ReplyType::String, StringReplyData{.value = std::move(key)}});
    to->values.push_back(std::move(value));
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(PushReplyData from, ArrayReplyData* to) {
  *to = std::move(from.value_array);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(ArrayReplyData from, std::vector<Reply>* to) {
  *to = std::move(from.values);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(PushReplyData from, std::vector<Reply>* to) {
  *to = std::move(from.value_array.values);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(MapReplyData from, std::vector<Reply>* to) {
  ASSIGN_OR_RETURN(ArrayReplyData array,
                   ConvertTo<ArrayReplyData>(std::move(from)));
  ASSIGN_OR_RETURN(*to, ConvertTo<std::vector<Reply>>(std::move(array)));
  return absl::OkStatus();
}

absl::Status EgltAssignInto(MapReplyData from,
                            absl::flat_hash_map<std::string, Reply>* to) {
  *to = std::move(from.values);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(SetReplyData from, std::vector<Reply>* to) {
  *to = std::move(from.values);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(VerbatimReplyData from, std::string* to) {
  *to = std::move(from.value);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(ArrayReplyData from,
                            absl::flat_hash_map<std::string, Reply>* to) {
  ASSIGN_OR_RETURN(MapReplyData map, ConvertTo<MapReplyData>(std::move(from)));
  *to = std::move(map).values;
  return absl::OkStatus();
}

absl::Status EgltAssignInto(Reply from, std::vector<Reply>* to) {
  if (from.type == ReplyType::Array) {
    ASSIGN_OR_RETURN(*to, ConvertTo<std::vector<Reply>>(
                              std::move(std::get<ArrayReplyData>(from.data))));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Set) {
    ASSIGN_OR_RETURN(*to, ConvertTo<std::vector<Reply>>(
                              std::move(std::get<SetReplyData>(from.data))));
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Push) {
    ASSIGN_OR_RETURN(*to, ConvertTo<std::vector<Reply>>(std::move(
                              std::get<PushReplyData>(from.data).value_array)));
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "Cannot convert reply of type ", MapReplyEnumToTypeName(from.type),
      " to std::vector<Reply>"));
}

absl::Status EgltAssignInto(Reply from,
                            absl::flat_hash_map<std::string, Reply>* to) {
  if (from.type == ReplyType::Map) {
    *to = std::move(std::get<MapReplyData>(from.data).values);
    return absl::OkStatus();
  }
  if (from.type == ReplyType::Array) {
    ASSIGN_OR_RETURN(MapReplyData map,
                     ConvertTo<MapReplyData>(std::move(from)));
    *to = std::move(map.values);
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "Cannot convert reply of type ", MapReplyEnumToTypeName(from.type),
      " to absl::flat_hash_map<std::string, Reply>"));
}

}  // namespace act::redis