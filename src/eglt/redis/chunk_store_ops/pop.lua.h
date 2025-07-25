#ifndef EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_
#define EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_

#include <string_view>

#include "eglt/redis/chunk_store_ops/unindent.h"

namespace eglt::redis {

constexpr std::array<std::string, 3> kPopScriptKeys = {
    "{}:s",         "{}:seq_to_id", "{}:offset_to_seq"};

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

}

#endif  // EGLT_REDIS_CHUNK_STORE_OPS_POP_LUA_H_