#include "eglt/redis/streams.h"

namespace eglt::redis {

StreamMessageId StreamMessageId::operator+(absl::Duration duration) const {
  CHECK(!is_wildcard) << "Cannot add duration to a wildcard StreamMessageId.";
  StreamMessageId result;
  result.millis = millis + absl::ToInt64Milliseconds(duration);
  result.sequence = 0;
  result.is_wildcard = false;
  return result;
}

absl::StatusOr<StreamMessageId> StreamMessageId::FromString(
    std::string_view id) {
  if (id == "*") {
    return StreamMessageId{.is_wildcard = true};
  }
  StreamMessageId message_id;
  const size_t dash_pos = id.find('-');
  if (dash_pos == std::string_view::npos) {
    if (!absl::SimpleAtoi(id, &message_id.millis)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid message ID: ", id));
    }
    message_id.sequence = 0;
    return message_id;
  }

  if (!absl::SimpleAtoi(id.substr(0, dash_pos), &message_id.millis)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid message ID millis: ", id));
  }
  if (!absl::SimpleAtoi(id.substr(dash_pos + 1), &message_id.sequence)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid message ID sequence: ", id));
  }

  return message_id;
}

std::string StreamMessageId::ToString() const {
  if (is_wildcard) {
    return "*";
  }
  return absl::StrCat(millis, "-", sequence);
}

absl::Status EgltAssignInto(Reply from, StreamMessage* to) {
  auto single_kv_map =
      StatusOrConvertTo<absl::flat_hash_map<std::string, Reply>>(
          std::move(from));
  RETURN_IF_ERROR(single_kv_map.status());

  if (single_kv_map->size() != 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Expected a single key-value pair, got ", single_kv_map->size()));
  }

  // Key is the message ID, value is a map of fields.
  auto& [id_str, fields_reply] = *single_kv_map->begin();
  ASSIGN_OR_RETURN(to->id, StreamMessageId::FromString(id_str));

  auto fields_map =
      StatusOrConvertTo<absl::flat_hash_map<std::string, std::string>>(
          std::move(fields_reply));
  RETURN_IF_ERROR(fields_map.status());
  to->fields = std::move(*fields_map);

  return absl::OkStatus();
}

absl::StatusOr<std::vector<StreamMessage>> RedisStream::XRead(
    StreamMessageId offset_id, int count, absl::Duration timeout) const {
  if (count == 0) {
    return absl::InvalidArgumentError(
        "Count must be greater than 0 or negative, meaning unlimited.");
  }

  std::vector<std::string_view> args;
  args.reserve(7);
  if (count > 0) {
    args.push_back("COUNT");
    args.push_back(absl::StrCat(count));
  }
  if (timeout != absl::InfiniteDuration()) {
    args.push_back("BLOCK");
    args.push_back(absl::StrCat(absl::ToInt64Milliseconds(timeout)));
  }
  args.push_back("STREAMS");
  args.push_back(key_);
  const std::string offset_id_str = offset_id.ToString();
  args.push_back(offset_id_str);

  ASSIGN_OR_RETURN(Reply reply,
                   redis_->ExecuteCommand("XREAD", std::move(args)));
  // If the reply is nil, it means the read timed out without receiving any
  // messages.
  if (reply.IsNil()) {
    return absl::DeadlineExceededError(
        "XRead timed out before receiving any messages on a stream.");
  }

  // A successful XRead reply is a map where keys are stream names and
  // values are arrays of messages.
  auto messages_by_stream =
      StatusOrConvertTo<absl::flat_hash_map<std::string, Reply>>(reply);
  RETURN_IF_ERROR(messages_by_stream.status());

  // We expect a single stream in the reply, as we only read from one stream.
  if (messages_by_stream->size() != 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Expected a single stream, got ", messages_by_stream->size()));
  }

  // Extract the messages for the stream.
  auto it = messages_by_stream->find(key_);
  if (it == messages_by_stream->end()) {
    return absl::NotFoundError(
        absl::StrCat("Stream ", key_, " not found in XRead reply."));
  }
  // Convert Reply to a vector of StreamMessage.
  return StatusOrConvertTo<std::vector<StreamMessage>>(std::move(it->second));
}

}  // namespace eglt::redis