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

#ifndef EGLT_REDIS_CHUNK_STORE_OPS_PUT_LUA_H_
#define EGLT_REDIS_CHUNK_STORE_OPS_PUT_LUA_H_

#include <string_view>

#include "eglt/redis/chunk_store_ops/unindent.h"

namespace eglt::redis {

const std::array<std::string, 8> kPutScriptKeys = {
    "{}:s",         "{}:seq_to_id", "{}:offset_to_seq", "{}:offset_cursor",
    "{}:final_seq", "{}:status",    "{}:closed",        "{}:events"};

constexpr std::string_view kPutScriptCode = R"(
  -- put.lua
  -- KEYS[1]: <id>:s
  -- KEYS[2]: <id>:seq_to_id
  -- KEYS[3]: <id>:offset_to_seq
  -- KEYS[4]: <id>:offset_cursor
  -- KEYS[5]: <id>:final_seq
  -- KEYS[6]: <id>:status
  -- KEYS[7]: <id>:closed
  -- KEYS[8]: <id>:events
  -- ARGV[1]: seq
  -- ARGV[2]: data
  -- ARGV[3]: final ('1' or '0')
  -- ARGV[4]: ttl_seconds (0 or less means no expiration)
  -- ARGV[5]: status_ttl_seconds (0 or less means no expiration)

  local seq = ARGV[1]
  local data = ARGV[2]
  local is_final = ARGV[3]
  local ttl_seconds = tonumber(ARGV[4])
  local status_ttl_seconds = tonumber(ARGV[5])
  
  -- Define all keys used by the abstraction
  local stream_key = KEYS[1]
  local seq_to_id_key = KEYS[2]
  local offset_to_seq_key = KEYS[3]
  local offset_cursor_key = KEYS[4]
  local final_seq_key = KEYS[5]
  local status_key = KEYS[6]
  local closed_key = KEYS[7]
  local events_key = KEYS[8]
  
  -- --- Main Put Logic (from put_v2.lua) ---
  if redis.call('EXISTS', closed_key) == 1 then
    return {err = 'PUT_CLOSED'}
  end
  
  if redis.call('HEXISTS', seq_to_id_key, seq) == 1 then
    return {err = 'SEQ_EXISTS'}
  end
  
  local offset = tonumber(redis.call('INCR', offset_cursor_key)) - 1
  local id_in_stream = redis.call('XADD', stream_key, '*', 'seq', seq, 'data', data)
  redis.call('HSET', seq_to_id_key, seq, id_in_stream)
  redis.call('ZADD', offset_to_seq_key, offset, seq)
  
  if is_final == '1' then
    redis.call('SET', final_seq_key, seq)
  end
  -- --- End of Main Put Logic ---
  
  -- --- TTL Application Logic ---
  if ttl_seconds > 0 then
    redis.call('EXPIRE', stream_key, ttl_seconds)
    redis.call('EXPIRE', seq_to_id_key, ttl_seconds)
    redis.call('EXPIRE', offset_to_seq_key, ttl_seconds)
    redis.call('EXPIRE', offset_cursor_key, ttl_seconds)
    redis.call('EXPIRE', status_key, status_ttl_seconds)
    redis.call('EXPIRE', closed_key, status_ttl_seconds)
  
    -- Also expire metadata keys if they were created
    if is_final == '1' then
      redis.call('EXPIRE', final_seq_key, ttl_seconds)
    end
  end
  
  redis.call('PUBLISH', events_key, 'NEW:' .. offset .. ':' .. seq .. ':' .. id_in_stream)
  
  return {offset, id_in_stream}
)"_unindent;

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_OPS_PUT_LUA_H_