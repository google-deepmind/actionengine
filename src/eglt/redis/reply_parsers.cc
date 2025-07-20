#include "eglt/redis/reply_parsers.h"
#include "eglt/redis/reply.h"

namespace eglt::redis {

absl::StatusOr<ArrayReplyData> ParseHiredisArrayReply(
    redisReply* absl_nonnull hiredis_reply, bool free) {
  DCHECK(hiredis_reply->type == REDIS_REPLY_ARRAY)
      << "Expected REDIS_REPLY_ARRAY, got " << hiredis_reply->type;

  std::vector<Reply> values{};
  values.reserve(hiredis_reply->elements);

  for (size_t i = 0; i < hiredis_reply->elements; ++i) {
    ASSIGN_OR_RETURN(Reply reply,
                     ParseHiredisReply(hiredis_reply->element[i], false));
    values.push_back(std::move(reply));
  }

  if (free) {
    freeReplyObject(hiredis_reply);
  }

  return ArrayReplyData{.values = std::move(values)};
}

absl::StatusOr<MapReplyData> ParseHiredisMapReply(
    redisReply* absl_nonnull hiredis_reply, bool free) {
  DCHECK(hiredis_reply->type == REDIS_REPLY_MAP)
      << "Expected REDIS_REPLY_MAP, got " << hiredis_reply->type;

  absl::flat_hash_map<std::string, Reply> map_values{};
  map_values.reserve(hiredis_reply->elements);

  for (size_t pair_idx = 0; pair_idx < hiredis_reply->elements / 2;
       ++pair_idx) {
    const size_t key_idx = pair_idx * 2;
    const size_t value_idx = key_idx + 1;

    ASSIGN_OR_RETURN(Reply key,
                     ParseHiredisReply(hiredis_reply->element[key_idx], false));
    ASSIGN_OR_RETURN(
        Reply value,
        ParseHiredisReply(hiredis_reply->element[value_idx], false));

    if (key.type != ReplyType::String) {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected key to be a string, got ", key.type));
    }
    map_values.emplace(key.ConsumeStringContentOrDie(), std::move(value));
  }

  if (free) {
    freeReplyObject(hiredis_reply);
  }

  return MapReplyData{.values = std::move(map_values)};
}

absl::StatusOr<SetReplyData> ParseHiredisSetReply(
    redisReply* absl_nonnull hiredis_reply, bool free) {
  DCHECK(hiredis_reply->type == REDIS_REPLY_SET)
      << "Expected REDIS_REPLY_SET, got " << hiredis_reply->type;

  std::vector<Reply> set_values{};
  set_values.reserve(hiredis_reply->elements);

  for (size_t i = 0; i < hiredis_reply->elements; ++i) {
    ASSIGN_OR_RETURN(Reply reply,
                     ParseHiredisReply(hiredis_reply->element[i], false));
    set_values.push_back(std::move(reply));
  }

  if (free) {
    freeReplyObject(hiredis_reply);
  }

  return SetReplyData{.values = std::move(set_values)};
}

absl::StatusOr<PushReplyData> ParseHiredisPushReply(
    redisReply* absl_nonnull hiredis_reply, bool free) {
  DCHECK(hiredis_reply->type == REDIS_REPLY_PUSH)
      << "Expected REDIS_REPLY_PUSH, got " << hiredis_reply->type;

  std::vector<Reply> values{};
  values.reserve(hiredis_reply->elements);

  for (size_t i = 0; i < hiredis_reply->elements; ++i) {
    ASSIGN_OR_RETURN(Reply reply,
                     ParseHiredisReply(hiredis_reply->element[i], false));
    values.push_back(std::move(reply));
  }

  auto data =
      PushReplyData{.value_array = ArrayReplyData{.values = std::move(values)}};

  if (free) {
    freeReplyObject(hiredis_reply);
  }

  return data;
}

absl::StatusOr<Reply> ParseHiredisReply(redisReply* absl_nonnull hiredis_reply,
                                        bool free) {
  Reply reply;

  const auto type = static_cast<ReplyType>(hiredis_reply->type);

  if (type == ReplyType::String) {
    reply.type = ReplyType::String;
    reply.data =
        StringReplyData{.value = hiredis_reply->str ? hiredis_reply->str : ""};
  }

  if (type == ReplyType::Status) {
    reply.type = ReplyType::Status;
    reply.data =
        StatusReplyData{.value = hiredis_reply->str ? hiredis_reply->str : ""};
  }

  if (type == ReplyType::Error) {
    reply.type = ReplyType::Error;
    reply.data =
        ErrorReplyData{.value = hiredis_reply->str ? hiredis_reply->str : ""};
  }

  if (type == ReplyType::Integer) {
    reply.type = ReplyType::Integer;
    reply.data = IntegerReplyData{.value = hiredis_reply->integer};
  }

  if (type == ReplyType::Nil) {
    reply.type = ReplyType::Nil;
    reply.data = NilReplyData{};
  }

  if (type == ReplyType::Bool) {
    reply.type = ReplyType::Bool;
    reply.data = BoolReplyData{.value = hiredis_reply->integer != 0};
  }

  if (type == ReplyType::Double) {
    reply.type = ReplyType::Double;
    reply.data = DoubleReplyData{.value = hiredis_reply->dval};
  }

  if (type == ReplyType::BigNum) {
    reply.type = ReplyType::BigNum;
    reply.data = BigNumReplyData{
        .value = hiredis_reply->str ? hiredis_reply->str : std::string{}};
  }

  if (type == ReplyType::Verbatim) {
    reply.type = ReplyType::Verbatim;
    reply.data = VerbatimReplyData{
        .type = {hiredis_reply->vtype[0], hiredis_reply->vtype[1],
                 hiredis_reply->vtype[2]},
        .value = hiredis_reply->str ? hiredis_reply->str : std::string{}};
  }

  if (type == ReplyType::Array) {
    reply.type = ReplyType::Array;
    ASSIGN_OR_RETURN(reply.data,
                     ParseHiredisArrayReply(hiredis_reply, /*free=*/false));
  }

  if (type == ReplyType::Map) {
    reply.type = ReplyType::Map;
    ASSIGN_OR_RETURN(reply.data,
                     ParseHiredisMapReply(hiredis_reply, /*free=*/false));
  }

  if (type == ReplyType::Set) {
    reply.type = ReplyType::Set;
    ASSIGN_OR_RETURN(reply.data,
                     ParseHiredisSetReply(hiredis_reply, /*free=*/false));
  }

  if (type == ReplyType::Push) {
    reply.type = ReplyType::Push;
    ASSIGN_OR_RETURN(reply.data,
                     ParseHiredisPushReply(hiredis_reply, /*free=*/false));
  }

  if (type == ReplyType::Attr) {
    reply.type = ReplyType::Attr;
    reply.data = AttrReplyData{};
  }

  if (reply.type == ReplyType::Uninitialized) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unsupported reply type: ", type,
                     " (hiredis reply type: ", hiredis_reply->type, ")"));
  }

  if (free) {
    freeReplyObject(hiredis_reply);
  }

  return reply;
}

}  // namespace eglt::redis