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

#ifndef EGLT_REDIS_REPLY_H_
#define EGLT_REDIS_REPLY_H_

#include <array>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include <hiredis/hiredis.h>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "eglt/util/status_macros.h"

namespace eglt::redis {

enum class ReplyType {
  Status = REDIS_REPLY_STATUS,
  Error = REDIS_REPLY_ERROR,
  Integer = REDIS_REPLY_INTEGER,
  Nil = REDIS_REPLY_NIL,
  String = REDIS_REPLY_STRING,
  Array = REDIS_REPLY_ARRAY,
  Bool = REDIS_REPLY_BOOL,
  Double = REDIS_REPLY_DOUBLE,
  Map = REDIS_REPLY_MAP,
  Set = REDIS_REPLY_SET,
  Push = REDIS_REPLY_PUSH,
  Attr = REDIS_REPLY_ATTR,
  BigNum = REDIS_REPLY_BIGNUM,
  Verbatim = REDIS_REPLY_VERB,
  Uninitialized = -1,
};

struct StatusReplyData;
struct ErrorReplyData;
struct IntegerReplyData;
struct NilReplyData;
struct StringReplyData;
struct BoolReplyData;
struct DoubleReplyData;
struct PushReplyData;
struct AttrReplyData;
struct BigNumReplyData;
struct VerbatimReplyData;
struct ArrayReplyData;
struct MapReplyData;
struct SetReplyData;

using ReplyData =
    std::variant<StatusReplyData, ErrorReplyData, IntegerReplyData,
                 NilReplyData, StringReplyData, BoolReplyData, DoubleReplyData,
                 PushReplyData, AttrReplyData, BigNumReplyData,
                 VerbatimReplyData, ArrayReplyData, MapReplyData, SetReplyData>;

struct NilReplyData {};

struct StatusReplyData {
  [[nodiscard]] std::string Consume() { return std::move(value); }

  absl::Status AsAbslStatus() const { return {absl::StatusCode::kOk, value}; }

  std::string value{};
};

struct ErrorReplyData {
  [[nodiscard]] std::string Consume() { return std::move(value); }

  absl::Status AsAbslStatus() const {
    return {absl::StatusCode::kInternal, value};
  }

  std::string value{};
};

struct IntegerReplyData {
  [[nodiscard]] int64_t Consume() const { return value; }

  int64_t value{0};
};

struct StringReplyData {
  [[nodiscard]] std::string Consume() { return std::move(value); }

  std::string value{};
};

struct BoolReplyData {
  [[nodiscard]] bool Consume() const { return value; }

  bool value{false};
};

struct DoubleReplyData {
  [[nodiscard]] double Consume() const { return value; }

  double value{0.0};
};

struct BigNumReplyData {
  [[nodiscard]] std::string Consume() { return std::move(value); }

  std::string value{};
};

struct VerbatimReplyData {
  [[nodiscard]] std::string Consume() { return std::move(value); }

  std::array<char, 3> type{};
  std::string value{};
};

struct AttrReplyData {
  AttrReplyData() { CHECK(false) << "AttrReplyData is not implemented yet."; }
};

struct Reply;

struct ArrayReplyData {
  [[nodiscard]] std::vector<Reply> Consume();
  [[nodiscard]] absl::flat_hash_map<std::string, Reply> ConsumeAsMapOrDie();

  std::vector<Reply> values;
};

struct MapReplyData {
  [[nodiscard]] absl::flat_hash_map<std::string, Reply> Consume();
  absl::flat_hash_map<std::string, Reply> values;
};

struct SetReplyData {
  [[nodiscard]] std::vector<Reply> Consume();

  std::vector<Reply> values;
};

struct PushReplyData {
  [[nodiscard]] std::vector<Reply> ConsumeValueArray();

  std::string type{};
  ArrayReplyData value_array;
};

struct Reply {
  // Reply() = default;
  // ~Reply() = default;

  std::string ConsumeStringContentOrDie();
  std::vector<Reply> ConsumeAsArrayOrDie();
  absl::flat_hash_map<std::string, Reply> ConsumeAsMapOrDie();

  absl::StatusOr<bool> ToBool() const;
  absl::StatusOr<double> ToDouble() const;
  absl::StatusOr<int64_t> ToInt() const;

  bool ToBoolOrDie() const {
    auto status_or_value = ToBool();
    CHECK_OK(status_or_value);
    return *status_or_value;
  }

  double ToDoubleOrDie() const {
    auto status_or_value = ToDouble();
    CHECK_OK(status_or_value);
    return *status_or_value;
  }

  int64_t ToIntOrDie() const {
    auto status_or_value = ToInt();
    CHECK_OK(status_or_value);
    return *status_or_value;
  }

  [[nodiscard]] bool IsString() const {
    return type == ReplyType::String &&
           std::holds_alternative<StringReplyData>(data);
  }

  [[nodiscard]] bool IsStatus() const {
    return type == ReplyType::Status &&
           std::holds_alternative<StatusReplyData>(data);
  }

  [[nodiscard]] bool IsError() const {
    return type == ReplyType::Error &&
           std::holds_alternative<ErrorReplyData>(data);
  }

  [[nodiscard]] bool IsNil() const {
    return type == ReplyType::Nil && std::holds_alternative<NilReplyData>(data);
  }

  ReplyType type{ReplyType::Uninitialized};
  ReplyData data{NilReplyData{}};
};

absl::Status GetStatusOrErrorFrom(const Reply& reply);

}  // namespace eglt::redis

#endif  // EGLT_REDIS_REPLY_H_