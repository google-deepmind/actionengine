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

#ifndef EGLT_REDIS_REDIS_H_
#define EGLT_REDIS_REDIS_H_

#include <initializer_list>
#include <string_view>

#include <g3fiber/channel.h>
#include <hiredis/hiredis.h>

#include "eglt/concurrency/concurrency.h"
#include "eglt/redis/pubsub.h"
#include "eglt/redis/reply.h"
#include "eglt/redis/reply_converters.h"
#include "eglt/redis/reply_parsers.h"

namespace eglt::redis {

using CommandArgs = std::vector<std::string_view>;

namespace internal {
// A custom deleter for redisContext to ensure proper cleanup of C structures
// when the Redis object is destroyed.
struct RedisContextDeleter {
  void operator()(redisContext* context) const {
    if (context) {
      redisFree(context);
    }
  }
};

struct PrivateConstructorTag {};
}  // namespace internal

struct HelloReply {
  static absl::StatusOr<HelloReply> From(Reply reply);

  std::string server;
  std::string version;
  int protocol_version;
  int id;
  std::string mode;
  std::string role;
  std::vector<absl::flat_hash_map<std::string, std::string>> modules;
};

struct Script {
  std::string sha1;
  std::string code;
};

class Redis {
 public:
  Redis() = delete;

  static absl::StatusOr<std::unique_ptr<Redis>> Connect(std::string_view host,
                                                        int port = 6379);

  Redis(const Redis&) = delete;
  Redis& operator=(const Redis&) = delete;

  ~Redis();

  void SetKeyPrefix(std::string_view prefix) {
    eglt::MutexLock lock(&mu_);
    key_prefix_ = std::string(prefix);
  }

  [[nodiscard]] std::string_view GetKeyPrefix() const {
    eglt::MutexLock lock(&mu_);
    return key_prefix_;
  }

  std::string GetKey(std::string_view key) const {
    eglt::MutexLock lock(&mu_);
    return absl::StrCat(key_prefix_, key);
  }

  absl::StatusOr<HelloReply> Hello(int protocol_version = 3,
                                   std::string_view client_name = "",
                                   std::string_view username = "",
                                   std::string_view password = "") const;
  absl::Status Ping(std::string_view expected_message = "PONG") const;

  absl::StatusOr<Reply> Get(std::string_view key) const;
  template <typename T>
  absl::StatusOr<T> Get(std::string_view key) const;

  absl::Status Set(std::string_view key, std::string_view value) const;
  template <typename T>
  absl::Status Set(std::string_view key, T&& value) const {
    ASSIGN_OR_RETURN(const std::string value_str,
                     ConvertTo<std::string>(std::forward<T>(value)));
    return Set(key, std::string_view(value_str));
  }

  absl::Status Multi() const {
    eglt::MutexLock lock(&mu_);
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommandInternal("MULTI"));
    ASSIGN_OR_RETURN(auto status_string,
                     ConvertTo<std::string>(std::move(reply)));
    if (status_string != "OK") {
      return absl::InternalError(
          absl::StrCat("Expected OK after MULTI, got: ", status_string));
    }
    return absl::OkStatus();
  }

  template <typename... Args>
  absl::StatusOr<Reply> ExecuteCommand(std::string_view command,
                                       Args... args) const {
    eglt::MutexLock lock(&mu_);
    return ExecuteCommandInternal(command, std::forward<Args>(args)...);
  }

  absl::StatusOr<std::string> RegisterScript(std::string_view name,
                                             std::string_view code,
                                             bool overwrite_existing = true) {
    // Notice how overwrite_existing = false will NOT update the script code
    // if it's already registered, even if the code is different.
    eglt::MutexLock lock(&mu_);
    bool existed = scripts_.contains(name);
    if (existed && !overwrite_existing) {
      return scripts_.at(name).sha1;  // Return existing script SHA1.
    }
    ASSIGN_OR_RETURN(Reply reply,
                     ExecuteCommandInternal("SCRIPT", "LOAD", code));
    if (reply.type != ReplyType::String) {
      return absl::InternalError(absl::StrCat(
          "Expected a string reply after SCRIPT LOAD, got: ", reply.type));
    }
    ASSIGN_OR_RETURN(auto sha1, ConvertTo<std::string>(std::move(reply)));

    Script& script = existed ? scripts_.at(name)
                             : scripts_.emplace(name, Script{}).first->second;

    script.code = std::string(code);
    script.sha1 = std::move(sha1);

    return script.sha1;
  }

  absl::StatusOr<absl::flat_hash_map<std::string, std::optional<int64_t>>>
  ZRange(std::string_view key, int64_t start, int64_t end,
         bool withscores = false) const {
    eglt::MutexLock lock(&mu_);
    CommandArgs args;
    args.reserve(4);
    args.push_back(key);
    args.push_back(absl::StrCat(start));
    args.push_back(absl::StrCat(end));
    if (withscores) {
      args.push_back("WITHSCORES");
    }
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommandInternal("ZRANGE", args));
    if (reply.type != ReplyType::Array) {
      return absl::InternalError(absl::StrCat(
          "Expected an array reply after ZRANGE, got: ", reply.type));
    }
    absl::flat_hash_map<std::string, std::optional<int64_t>> result;
    if (withscores) {
      auto value_map = ConvertTo<absl::flat_hash_map<std::string, int64_t>>(
          std::move(reply));
      RETURN_IF_ERROR(value_map.status());
      result.reserve(value_map->size());
      for (const auto& [key, value] : *value_map) {
        result.emplace(key, value);
      }
    } else {
      auto value_array = ConvertTo<std::vector<int64_t>>(std::move(reply));
      RETURN_IF_ERROR(value_array.status());
      result.reserve(value_array->size());
      for (const auto& value : *value_array) {
        result.emplace(std::to_string(value), std::nullopt);
      }
    }
    return result;
  }

  absl::StatusOr<Reply> ExecuteScript(std::string_view name,
                                      CommandArgs script_keys = {},
                                      CommandArgs script_args = {}) const {
    eglt::MutexLock lock(&mu_);
    std::string sha1;
    if (!scripts_.contains(name)) {
      return absl::NotFoundError(absl::StrCat("Script not found: ", name));
    }
    const Script& script = scripts_.at(name);
    sha1 = script.sha1;

    const std::string num_keys_str = absl::StrCat(script_keys.size());
    CommandArgs evalsha_args;
    evalsha_args.reserve(script_keys.size() + script_args.size() + 2);
    evalsha_args.push_back(sha1);
    evalsha_args.push_back(num_keys_str);
    evalsha_args.insert(evalsha_args.end(),
                        std::make_move_iterator(script_keys.begin()),
                        std::make_move_iterator(script_keys.end()));
    evalsha_args.insert(evalsha_args.end(),
                        std::make_move_iterator(script_args.begin()),
                        std::make_move_iterator(script_args.end()));
    return ExecuteCommandInternal("EVALSHA", evalsha_args);
  }

  absl::StatusOr<Reply> ExecuteScript(
      std::string_view name, const CommandArgs& script_keys = {},
      std::initializer_list<std::string_view> script_args = {}) const {
    return ExecuteScript(name, script_keys, CommandArgs(script_args));
  }
  template <typename... Args>
  absl::StatusOr<Reply> ExecuteScript(std::string_view name,
                                      const CommandArgs& script_keys,
                                      std::string_view arg0,
                                      Args&&... args) const {
    return ExecuteScript(
        name, script_keys,
        std::vector<std::string_view>{arg0, std::forward<Args>(args)...});
  }

  absl::StatusOr<std::vector<Reply>> Exec() const {
    eglt::MutexLock lock(&mu_);
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommandInternal("EXEC"));
    if (reply.type == ReplyType::Nil) {
      return absl::InternalError(
          "EXEC returned a nil reply, which indicates that the transaction "
          "was "
          "discarded or no commands were executed.");
    }
    if (reply.type != ReplyType::Array) {
      return absl::InternalError(absl::StrCat(
          "Expected an array reply after EXEC, got: ", reply.type));
    }
    return ConvertTo<std::vector<Reply>>(std::move(reply));
  }

  absl::Status Watch(std::initializer_list<std::string_view> keys) const {
    eglt::MutexLock lock(&mu_);
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommandInternal("WATCH", keys));
    ASSIGN_OR_RETURN(auto status_string,
                     ConvertTo<std::string>(std::move(reply)));
    if (status_string != "OK") {
      return absl::InternalError(
          absl::StrCat("Expected OK after WATCH, got: ", status_string));
    }
    return absl::OkStatus();
  }

  absl::StatusOr<int> Publish(std::string_view channel,
                              std::string_view message) const;
  template <typename T>
  absl::StatusOr<int> Publish(std::string_view channel, T&& message) const {
    ASSIGN_OR_RETURN(const std::string message_str,
                     ToString(std::forward<T>(message)));
    return Publish(channel, std::string_view(message_str));
  }

  void Unsubscribe(std::string_view channel = "") {
    eglt::MutexLock lock(&mu_);
    if (channel.empty()) {
      CHECK_OK(ExecuteCommandInternal("UNSUBSCRIBE", {}).status());
    } else {
      CHECK_OK(ExecuteCommandInternal("UNSUBSCRIBE", channel).status());
    }
    auto it = subscriptions_.find(channel);
    if (it != subscriptions_.end()) {
      it->second->Unsubscribe();  // Notify the subscription.
      subscriptions_.erase(it);   // Remove the subscription.
    }
  }

  std::shared_ptr<Subscription> Subscribe(
      std::string_view channel,
      size_t capacity = Subscription::kDefaultChannelCapacity) {
    eglt::MutexLock lock(&mu_);
    if (const auto it = subscriptions_.find(channel);
        it != subscriptions_.end()) {
      return it->second;
    }

    auto subscription = std::make_shared<Subscription>(capacity);
    subscriptions_.emplace(channel,
                           subscription);  // Store the subscription.

    CHECK_OK(ExecuteCommandInternal("SUBSCRIBE", channel).status());
    return subscription;
  }

  std::shared_ptr<Subscription> Subscribe(
      std::string_view channel,
      absl::AnyInvocable<void(Reply)> on_message = {}) {
    eglt::MutexLock lock(&mu_);
    if (const auto it = subscriptions_.find(channel);
        it != subscriptions_.end()) {
      return it->second;
    }

    auto subscription = std::make_shared<Subscription>(std::move(on_message));
    subscriptions_.emplace(channel,
                           subscription);  // Store the subscription.

    CHECK_OK(ExecuteCommandInternal("SUBSCRIBE", channel).status());
    return subscription;
  }

  absl::Status GetPushReplyStatus() const;
  [[nodiscard]] thread::Reader<PushReplyData>* GetPushReplyReader();
  [[nodiscard]] redisContext* GetContext() const;

  explicit Redis(internal::PrivateConstructorTag _,
                 size_t push_reply_channel_capacity = 64);

 private:
  absl::StatusOr<Reply> ExecuteCommandInternal(std::string_view command,
                                               const CommandArgs& args) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<Reply> ExecuteCommandInternal(
      std::string_view command,
      std::initializer_list<std::string_view> args = {}) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  template <typename... Args>
  absl::StatusOr<Reply> ExecuteCommandInternal(std::string_view command,
                                               std::string_view arg0,
                                               Args&&... args) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void SetContext(
      std::unique_ptr<redisContext, internal::RedisContextDeleter> context);
  [[nodiscard]] std::string_view GetContextErrorMessage() const;

  static void HandlePushReply(void* absl_nonnull privdata,
                              void* absl_nonnull hiredis_reply)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  void ClosePushReplyWriter() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable eglt::Mutex mu_;
  std::unique_ptr<redisContext, internal::RedisContextDeleter> context_;

  thread::Channel<PushReplyData> push_reply_channel_;
  absl::Status push_reply_status_ ABSL_GUARDED_BY(mu_);
  bool push_replies_closed_ ABSL_GUARDED_BY(mu_) = false;
  std::string key_prefix_ ABSL_GUARDED_BY(mu_) = "eglt:redis:";

  absl::flat_hash_map<std::string, std::shared_ptr<Subscription>> subscriptions_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, Script> scripts_ ABSL_GUARDED_BY(mu_);
};

template <typename... Args>
absl::StatusOr<Reply> Redis::ExecuteCommandInternal(std::string_view command,
                                                    std::string_view arg0,
                                                    Args&&... args) const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  const std::initializer_list<std::string_view> command_args{
      arg0, std::forward<Args>(args)...};
  return ExecuteCommandInternal(command, command_args);
}

template <typename T>
absl::StatusOr<T> Redis::Get(std::string_view key) const {
  ASSIGN_OR_RETURN(Reply reply, Get(key));
  return ConvertTo<T>(reply);
}

template <>
inline absl::StatusOr<std::string> Redis::Get(std::string_view key) const {
  ASSIGN_OR_RETURN(Reply reply, Get(key));
  if (reply.type != ReplyType::String) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Expected reply type REDIS_REPLY_STRING, got ", reply.type));
  }
  return reply.ConsumeStringContentOrDie();
}

template <>
inline absl::Status Redis::Set(std::string_view key,
                               const std::string& value) const {
  return Set(key, std::string_view(value));
}

}  // namespace eglt::redis

#endif  // EGLT_REDIS_REDIS_H_