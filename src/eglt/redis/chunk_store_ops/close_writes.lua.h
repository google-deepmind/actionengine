#ifndef EGLT_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_
#define EGLT_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_

#include <string_view>

#include "eglt/redis/chunk_store_ops/unindent.h"

namespace eglt::redis {

constexpr std::array<std::string, 4> kCloseWritesScriptKeys = {
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

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_OPS_CLOSE_WRITES_LUA_H_