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

#ifndef ACTIONENGINE_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_
#define ACTIONENGINE_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_

#include <string_view>

#include "actionengine/redis/chunk_store_ops/unindent.h"

namespace act::redis {

const std::array<std::string, 4> kCloseWritesScriptKeys = {
    "{}:status", "{}:closed", "{}:final_seq", "{}:events"};

constexpr std::string_view kCloseWritesScriptCode = R"(
  -- close_writes.lua
  -- KEYS[1]: <id>:status
  -- KEYS[2]: <id>:closed
  -- KEYS[3]: <id>:final_seq
  -- KEYS[4]: <id>:events
  -- ARGV[1]: status

  local status = ARGV[1]

  local status_key = KEYS[1]
  local closed_key = KEYS[2]
  local final_seq_key = KEYS[3]
  local events_channel = KEYS[4]

  -- Use SET with NX to prevent a race condition where two clients try to close simultaneously
  local was_set = redis.call('SET', closed_key, '1', 'NX')

  if was_set then
    redis.call('SET', status_key, status)
    -- Notify all listeners that the stream is now closed
    -- This allows blocking operations to stop waiting and return an error.
    redis.call('PUBLISH', events_channel, 'CLOSE:' .. status)
    return 'OK'
  else
    return {err = 'ALREADY_CLOSED'}
  end
)"_unindent;

}  // namespace act::redis

#endif  // ACTIONENGINE_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_