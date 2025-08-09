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

#ifndef ACTIONENGINE_REDIS_REPLY_PARSERS_H_
#define ACTIONENGINE_REDIS_REPLY_PARSERS_H_

#include <array>

#include <hiredis/hiredis.h>

#include "actionengine/redis/reply.h"

namespace act::redis {

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

}  // namespace act::redis

#endif  // ACTIONENGINE_REDIS_REPLY_PARSERS_H_