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

#ifndef EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_
#define EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_

#include <string_view>

#include "eglt/redis/chunk_store_ops/unindent.h"

namespace eglt::redis {

const std::array<std::string, 3> kPopScriptKeys = {"{}:s", "{}:seq_to_id",
                                                   "{}:offset_to_seq"};

constexpr std::string_view kPopScriptCode = R"(
  -- pop.lua
  -- KEYS[1]: <id>:s
  -- KEYS[2]: <id>:seq_to_id
  -- KEYS[3]: <id>:offset_to_seq
  -- ARGV[1]: seq_to_pop

  local seq_to_pop = ARGV[1]

  local stream_key = KEYS[1]
  local seq_to_id_key = KEYS[2]
  local offset_to_seq_key = KEYS[3]

  -- 1. Find the element's stream ID. If it doesn't exist, return nil.
  local id_in_stream = redis.call('HGET', seq_to_id_key, seq_to_pop)
  if not id_in_stream then
    return nil
  end

  -- 2. Get the element's data before deleting.
  local stream_reply = redis.call('XRANGE', stream_key, id_in_stream, id_in_stream, 'COUNT', 1)
  if #stream_reply == 0 then
    redis.call('HDEL', seq_to_id_key, seq_to_pop) -- Cleanup dangling ref
    return nil
  end

  local fields = stream_reply[1][2]
  local data_to_return = nil
  for i = 1, #fields, 2 do
    if fields[i] == 'data' then
    data_to_return = fields[i+1]
    break
    end
  end

  -- 3. Perform the deletions. There is no shifting.
  redis.call('XDEL', stream_key, id_in_stream)
  redis.call('HDEL', seq_to_id_key, seq_to_pop)
  redis.call('ZREM', offset_to_seq_key, seq_to_pop) -- This leaves a gap in offsets.

  -- 4. Return the retrieved data.
  return data_to_return
)"_unindent;

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_