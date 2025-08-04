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
#include <uvw.hpp>

#include <absl/container/flat_hash_set.h>
#include <g3fiber/channel.h>
#include <hiredis/adapters/libuv.h>
#include <hiredis/async.h>
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
  void operator()(redisAsyncContext* context) const {
    if (context != nullptr) {
      redisAsyncFree(context);
    }
  }
};

struct PrivateConstructorTag {};

class EventLoop {
 public:
  EventLoop();

  ~EventLoop() {
    handle_->stop();
    thread_->join();
  }

  uvw::loop* Get() { return loop_.get(); }

 private:
  std::shared_ptr<uvw::idle_handle> handle_;
  std::shared_ptr<uvw::loop> loop_;
  std::unique_ptr<std::thread> thread_;
};

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

namespace internal {
struct ReplyFuture {
  Reply reply;
  absl::Status status;
  thread::PermanentEvent event;
};
}  // namespace internal

class Redis {
 public:
  // Static callbacks for hiredis async context events. They resolve to
  // instance methods to allow access to the instance state.
  static void ConnectCallback(const redisAsyncContext* absl_nonnull context,
                              int status);

  static void DisconnectCallback(const redisAsyncContext* absl_nonnull context,
                                 int status);

  static void PubsubCallback(redisAsyncContext* absl_nonnull context,
                             void* absl_nullable hiredis_reply, void* privdata);

  static void ReplyCallback(redisAsyncContext* absl_nonnull context,
                            void* absl_nonnull hiredis_reply, void* privdata);

  // ReSharper disable once CppParameterMayBeConstPtrOrRef
  static void PushReplyCallback(redisAsyncContext* absl_nonnull context,
                                void* absl_nonnull hiredis_reply);

  // Deleted default constructor to prevent instantiation without connection.
  Redis() = delete;

  static absl::StatusOr<std::unique_ptr<Redis>> Connect(std::string_view host,
                                                        int port = 6379);

  // Non-copyable, non-moveable.
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

  absl::StatusOr<Reply> Get(std::string_view key);
  template <typename T>
  absl::StatusOr<T> Get(std::string_view key);

  absl::Status Set(std::string_view key, std::string_view value);

  template <typename T>
  absl::Status Set(std::string_view key, T&& value) {
    ASSIGN_OR_RETURN(const std::string value_str,
                     ConvertTo<std::string>(std::forward<T>(value)));
    return Set(key, std::string_view(value_str));
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
                     ExecuteCommandWithGuards("SCRIPT", {"LOAD", code}));
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

  absl::StatusOr<Reply> ExecuteScript(std::string_view name,
                                      CommandArgs script_keys = {},
                                      CommandArgs script_args = {}) {
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
    return ExecuteCommandWithGuards("EVALSHA", evalsha_args);
  }

  absl::StatusOr<absl::flat_hash_map<std::string, std::optional<int64_t>>>
  ZRange(std::string_view key, int64_t start, int64_t end,
         bool withscores = false) {
    eglt::MutexLock lock(&mu_);
    CommandArgs args;
    args.reserve(4);
    args.push_back(key);
    args.push_back(absl::StrCat(start));
    args.push_back(absl::StrCat(end));
    if (withscores) {
      args.push_back("WITHSCORES");
    }
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommandWithGuards("ZRANGE", args));
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

  absl::StatusOr<std::shared_ptr<Subscription>> Subscribe(
      std::string_view channel,
      absl::AnyInvocable<void(Reply)> on_message = {});

  absl::Status Unsubscribe(std::string_view channel);

  void RemoveSubscription(std::string_view channel,
                          const std::shared_ptr<Subscription>& subscription) {
    eglt::MutexLock lock(&mu_);
    auto it = subscriptions_.find(channel);
    if (it != subscriptions_.end()) {
      it->second.erase(subscription);
    }
  }

  absl::StatusOr<HelloReply> Hello(int protocol_version = 3,
                                   std::string_view client_name = "",
                                   std::string_view username = "",
                                   std::string_view password = "");

  absl::StatusOr<Reply> ExecuteCommand(std::string_view command,
                                       const CommandArgs& args = {});

  explicit Redis(internal::PrivateConstructorTag _) {}

 private:
  bool ParseReply(redisReply* absl_nonnull hiredis_reply,
                  Reply* absl_nonnull reply_out)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status CheckConnected() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (!connected_) {
      return absl::FailedPreconditionError(
          "Redis is not connected to the Redis server.");
    }
    return absl::OkStatus();
  }

  absl::StatusOr<Reply> ExecuteCommandWithGuards(std::string_view command,
                                                 const CommandArgs& args = {})
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (command.empty()) {
      return absl::InvalidArgumentError("Command cannot be empty.");
    }
    RETURN_IF_ERROR(status_);
    RETURN_IF_ERROR(CheckConnected());

    ++num_pending_commands_;
    absl::StatusOr<Reply> reply = ExecuteCommandInternal(command, args);
    --num_pending_commands_;
    cv_.SignalAll();

    return reply;
  }

  absl::StatusOr<Reply> ExecuteCommandInternal(std::string_view command,
                                               const CommandArgs& args = {})
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Instance versions of the static callbacks.
  void OnConnect(int status) ABSL_LOCKS_EXCLUDED(mu_);

  void OnDisconnect(int status) ABSL_LOCKS_EXCLUDED(mu_);

  void OnPubsubReply(void* absl_nonnull hiredis_reply) ABSL_LOCKS_EXCLUDED(mu_);

  void OnPushReply(redisReply* absl_nonnull hiredis_reply)
      ABSL_LOCKS_EXCLUDED(mu_);

  mutable eglt::Mutex mu_;
  eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);

  internal::EventLoop event_loop_ ABSL_GUARDED_BY(mu_);

  std::string key_prefix_ ABSL_GUARDED_BY(mu_) = "eglt:";
  size_t num_pending_commands_ ABSL_GUARDED_BY(mu_) = 0;
  thread::PermanentEvent disconnect_event_ ABSL_GUARDED_BY(mu_);
  bool connected_ ABSL_GUARDED_BY(mu_) = false;
  absl::Status status_ ABSL_GUARDED_BY(mu_) = absl::OkStatus();

  absl::flat_hash_map<std::string,
                      absl::flat_hash_set<std::shared_ptr<Subscription>>>
      subscriptions_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, Script> scripts_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<redisAsyncContext, internal::RedisContextDeleter> context_;
};

template <typename T>
absl::StatusOr<T> Redis::Get(std::string_view key) {
  ASSIGN_OR_RETURN(Reply reply, Get(key));
  return ConvertTo<T>(reply);
}

template <>
inline absl::Status Redis::Set(std::string_view key, const std::string& value) {
  return Set(key, std::string_view(value));
}

}  // namespace eglt::redis

#endif  // EGLT_REDIS_REDIS_H_