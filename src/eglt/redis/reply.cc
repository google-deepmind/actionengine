#include "eglt/redis/reply.h"

namespace eglt::redis {

std::string Reply::ConsumeStringContentOrDie() {
  CHECK(std::holds_alternative<StringReplyData>(data) ||
        std::holds_alternative<VerbatimReplyData>(data) ||
        std::holds_alternative<StatusReplyData>(data) ||
        std::holds_alternative<ErrorReplyData>(data))
      << "Cannot consume reply of type as string: " << static_cast<int>(type);

  if (std::holds_alternative<StringReplyData>(data)) {
    DCHECK(type == ReplyType::String)
        << "Expected REDIS_REPLY_STRING set for StringReplyData, got "
        << static_cast<int>(type);
    return std::get<StringReplyData>(std::move(data)).Consume();
  }

  if (std::holds_alternative<VerbatimReplyData>(data)) {
    DCHECK(type == ReplyType::Verbatim)
        << "Expected REDIS_REPLY_VERB set for VerbatimReplyData, got "
        << static_cast<int>(type);
    return std::get<VerbatimReplyData>(std::move(data)).Consume();
  }

  if (std::holds_alternative<StatusReplyData>(data)) {
    DCHECK(type == ReplyType::Status)
        << "Expected REDIS_REPLY_STATUS set for StatusReplyData, got "
        << static_cast<int>(type);
    return std::get<StatusReplyData>(std::move(data)).Consume();
  }

  if (std::holds_alternative<ErrorReplyData>(data)) {
    DCHECK(type == ReplyType::Error)
        << "Expected REDIS_REPLY_ERROR set for ErrorReplyData, got "
        << static_cast<int>(type);
    return std::get<ErrorReplyData>(std::move(data)).Consume();
  }

  LOG(FATAL)
      << "ConsumeStringContentOrDie() is not implemented for this reply type: "
      << static_cast<int>(type);
  ABSL_ASSUME(false);
}

std::vector<Reply> Reply::ConsumeAsArrayOrDie() {
  CHECK(type == ReplyType::Array || type == ReplyType::Push)
      << "Cannot consume reply of type as array: " << static_cast<int>(type);

  if (type == ReplyType::Array) {
    return std::get<ArrayReplyData>(std::move(data)).Consume();
  }

  if (type == ReplyType::Push) {
    return std::get<PushReplyData>(std::move(data)).value_array.Consume();
  }

  LOG(FATAL) << "ConsumeAsArrayOrDie() is not implemented for this reply type: "
             << static_cast<int>(type);
  ABSL_ASSUME(false);
}

absl::flat_hash_map<std::string, Reply> Reply::ConsumeAsMapOrDie() {
  CHECK(type == ReplyType::Map || type == ReplyType::Array)
      << "Cannot consume reply of type as map: " << static_cast<int>(type);

  if (type == ReplyType::Map) {
    return std::get<MapReplyData>(std::move(data)).Consume();
  }

  if (type == ReplyType::Array) {
    return std::get<ArrayReplyData>(std::move(data)).ConsumeAsMapOrDie();
    ABSL_ASSUME(false);
  }

  LOG(FATAL) << "ConsumeAsMapOrDie() is not implemented for this reply type: "
             << static_cast<int>(type);
  ABSL_ASSUME(false);
}

std::vector<Reply> ArrayReplyData::Consume() {
  return std::move(values);
}

absl::flat_hash_map<std::string, Reply> ArrayReplyData::ConsumeAsMapOrDie() {
  CHECK(values.size() % 2 == 0)
      << "ArrayReplyData cannot be consumed as a map if it has an odd number "
         "of elements. Expected pairs of key-value replies.";

  absl::flat_hash_map<std::string, Reply> result;
  result.reserve(values.size() / 2);

  for (size_t pair_idx = 0; pair_idx < values.size() / 2; pair_idx++) {
    const size_t key_idx = pair_idx * 2;
    const size_t value_idx = key_idx + 1;

    std::string key = std::move(values[key_idx]).ConsumeStringContentOrDie();
    Reply value = std::move(values[value_idx]);

    result[std::move(key)] = std::move(value);
  }
  return result;
}

absl::flat_hash_map<std::string, Reply> MapReplyData::Consume() {
  return std::move(values);
}

std::vector<Reply> SetReplyData::Consume() {
  return std::move(values);
}

std::vector<Reply> PushReplyData::ConsumeValueArray() {
  return value_array.Consume();
}

absl::Status GetStatusOrErrorFrom(const Reply& reply) {
  const bool holds_status = reply.type == ReplyType::Status &&
                            std::holds_alternative<StatusReplyData>(reply.data);
  const bool holds_error = reply.type == ReplyType::Error &&
                           std::holds_alternative<ErrorReplyData>(reply.data);

  CHECK(holds_status || holds_error)
      << "Cannot get status or error from reply of type coded as: "
      << static_cast<int>(reply.type);

  if (holds_status) {
    return std::get<StatusReplyData>(reply.data).AsAbslStatus();
  }
  if (holds_error) {
    return std::get<ErrorReplyData>(reply.data).AsAbslStatus();
  }

  return absl::InternalError(
      absl::StrCat("Unexpected reply type for status or error: ", reply.type));
}

absl::StatusOr<bool> Reply::ToBool() const {
  if (type == ReplyType::Bool) {
    return std::get<BoolReplyData>(data).value;
  }
  if (type == ReplyType::Integer) {
    // Convert integer to bool.
    return std::get<IntegerReplyData>(data).value != 0;
  }
  if (type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(data).value;
    if (str == "1" || absl::EqualsIgnoreCase(str, "true")) {
      return true;
    }
    if (str == "0" || absl::EqualsIgnoreCase(str, "false")) {
      return false;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot cast string to bool, got: ", str));
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot cast type to bool, got type code ", type));
}

absl::StatusOr<double> Reply::ToDouble() const {
  if (type == ReplyType::Double) {
    return std::get<DoubleReplyData>(data).value;
  }
  if (type == ReplyType::Integer) {
    // Convert integer to double.
    return static_cast<double>(std::get<IntegerReplyData>(data).value);
  }
  if (type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(data).value;
    double result = 0.0;
    if (!absl::SimpleAtod(str, &result)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Cannot cast string to double, got: ", str));
    }
    return result;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot cast type to double, got type code ", type));
}

absl::StatusOr<int64_t> Reply::ToInt() const {
  if (type == ReplyType::Integer) {
    return std::get<IntegerReplyData>(data).value;
  }
  if (type == ReplyType::String) {
    const std::string_view str = std::get<StringReplyData>(data).value;
    int64_t result = 0;
    if (!absl::SimpleAtoi(str, &result)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Cannot cast string to int64_t, got: ", str));
    }
    return result;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Cannot cast type to int64_t, got type code ", type));
}

}  // namespace eglt::redis