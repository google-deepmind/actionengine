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

#ifndef ACTIONENGINE_REDIS_STREAMS_H_
#define ACTIONENGINE_REDIS_STREAMS_H_

#include <string>
#include <string_view>

#include <g3fiber/channel.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/conversion.h"
#include "actionengine/data/types.h"
#include "actionengine/redis/redis.h"
#include "actionengine/redis/reply_converters.h"
#include "actionengine/util/status_macros.h"

namespace act::redis {

namespace internal {
template <typename It>
concept StringViewKeyValueIterator =
    std::input_iterator<It> &&
    std::convertible_to<typename std::iterator_traits<It>::value_type,
                        std::pair<std::string_view, std::string_view>>;
}

struct StreamMessageId {
  int64_t millis = 0;
  int64_t sequence = 0;
  bool is_wildcard = false;

  StreamMessageId operator+(absl::Duration duration) const;

  bool operator==(const StreamMessageId& rhs) const {
    return millis == rhs.millis && sequence == rhs.sequence;
  }

  bool operator>=(const StreamMessageId& rhs) const {
    return millis > rhs.millis ||
           (millis == rhs.millis && sequence >= rhs.sequence);
  }

  static constexpr StreamMessageId Wildcard() {
    return StreamMessageId{.is_wildcard = true};
  }

  static absl::StatusOr<StreamMessageId> FromString(std::string_view id);

  std::string ToString() const;
};

struct StreamMessage {
  std::optional<std::string> stream_id;
  StreamMessageId id;
  absl::flat_hash_map<std::string, std::string> fields;

  bool operator==(const StreamMessage& rhs) const {
    return id == rhs.id && fields == rhs.fields;
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const StreamMessage& message) {
    absl::Format(&sink, "StreamMessage {\nid: %s\nfields: {\n",
                 message.id.ToString());
    for (const auto& [key, value] : message.fields) {
      absl::Format(&sink, "  %s: %s\n", key, value);
    }
    absl::Format(&sink, "}");
  }
};

absl::Status EgltAssignInto(Reply from, StreamMessage* to);

class RedisStream {
 public:
  RedisStream(redis::Redis* absl_nonnull redis, std::string_view key)
      : redis_(redis), key_(key) {
    absl::flat_hash_map<std::string, std::string> params;
    CHECK(redis != nullptr)
        << "RedisStream requires a non-null Redis instance.";
    CHECK(!key.empty()) << "RedisStream key must not be empty.";
  }

  RedisStream(const RedisStream&) = delete;
  RedisStream& operator=(const RedisStream&) = delete;

  ~RedisStream() = default;

  template <typename Iter>
  requires internal::StringViewKeyValueIterator<Iter>
      absl::StatusOr<StreamMessageId> XAdd(
          Iter fields_begin, Iter fields_end,
          StreamMessageId id = StreamMessageId::Wildcard()) const {
    const std::string str_id = id.ToString();

    std::vector<std::string_view> args;
    args.reserve(2 * (fields_end - fields_begin) + 2);
    args.push_back(key_);
    args.push_back(str_id);
    for (auto it = fields_begin; it != fields_end; ++it) {
      args.push_back(it->first);
      args.push_back(it->second);
    }
    ASSIGN_OR_RETURN(Reply reply, redis_->ExecuteCommand("XADD", args));
    return StreamMessageId::FromString(reply.ConsumeStringContentOrDie());
  }

  absl::StatusOr<StreamMessageId> XAdd(
      std::initializer_list<std::pair<std::string_view, std::string_view>>
          fields,
      std::string_view id = "*") const {
    ASSIGN_OR_RETURN(const auto message_id, StreamMessageId::FromString(id));
    return XAdd(fields.begin(), fields.end(), message_id);
  }

  absl::StatusOr<std::vector<StreamMessage>> XRead(
      std::string_view offset_id = "0", int count = -1,
      absl::Duration timeout = absl::ZeroDuration()) const;

  absl::StatusOr<std::vector<StreamMessage>> XRead(
      StreamMessageId offset_id = {}, int count = -1,
      absl::Duration timeout = absl::ZeroDuration()) const;

  absl::StatusOr<std::vector<StreamMessage>> XRange(
      const StreamMessageId& start_offset_id = {},
      const StreamMessageId& end_offset_id = {}, int count = -1) const;

  absl::StatusOr<std::vector<StreamMessage>> XRevRange(
      StreamMessageId start_offset_id = {}, StreamMessageId end_offset_id = {},
      int count = -1) const;

 private:
  Redis* const absl_nonnull redis_;
  const std::string key_;
};

}  // namespace act::redis

#endif  // ACTIONENGINE_REDIS_STREAMS_H_