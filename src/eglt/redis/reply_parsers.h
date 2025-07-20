#ifndef EGLT_REDIS_REPLY_PARSERS_H_
#define EGLT_REDIS_REPLY_PARSERS_H_

#include <array>

#include <hiredis/hiredis.h>

#include "eglt/absl_headers.h"
#include "eglt/redis/reply.h"

namespace eglt::redis {

absl::StatusOr<ArrayReplyData> ParseHiredisArrayReply(
    redisReply* absl_nonnull hiredis_reply, bool free);
absl::StatusOr<MapReplyData> ParseHiredisMapReply(
    redisReply* absl_nonnull hiredis_reply, bool free);
absl::StatusOr<SetReplyData> ParseHiredisSetReply(
    redisReply* absl_nonnull hiredis_reply, bool free);
absl::StatusOr<PushReplyData> ParseHiredisPushReply(
    redisReply* absl_nonnull hiredis_reply, bool free);

absl::StatusOr<Reply> ParseHiredisReply(redisReply* absl_nonnull hiredis_reply,
                                        bool free = true);

}  // namespace eglt::redis

#endif  // EGLT_REDIS_REPLY_PARSERS_H_