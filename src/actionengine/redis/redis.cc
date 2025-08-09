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

#include "actionengine/redis/redis.h"

#include <iterator>
#include <memory>
#include <string_view>
#include <utility>
#include <variant>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>
#include <hiredis/adapters/libuv.h>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <uvw/handle.hpp>

#include "actionengine/redis/reply.h"
#include "actionengine/redis/reply_converters.h"
#include "actionengine/redis/reply_parsers.h"
#include "actionengine/util/map_util.h"
#include "actionengine/util/status_macros.h"

namespace act::redis {

internal::EventLoop::EventLoop() : loop_(uvw::loop::create()) {
  handle_ = loop_->resource<uvw::idle_handle>();
  handle_->init();
  handle_->on<uvw::idle_event>([](const uvw::idle_event&, uvw::idle_handle& h) {
    // TODO: this is a hack, and a better way to slash CPU usage should exist.
    act::SleepFor(absl::Milliseconds(2));
  });
  handle_->on<uvw::close_event>(
      [](const uvw::close_event&, uvw::idle_handle& h) { h.close(); });

  thread_ = std::make_unique<std::thread>([this]() {
    handle_->start();
    loop_->run();
  });
}

absl::StatusOr<HelloReply> HelloReply::From(Reply reply) {
  HelloReply hello_reply;
  if (reply.type != ReplyType::Array && reply.type != ReplyType::Map) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Expected an Array or Map reply for HELLO, got type coded as: ",
        reply.type));
  }

  absl::flat_hash_map<std::string, Reply> fields;
  if (reply.type == ReplyType::Map) {
    fields = std::get<MapReplyData>(std::move(reply).data).Consume();
  } else {
    fields =
        std::get<ArrayReplyData>(std::move(reply).data).ConsumeAsMapOrDie();
  }

  hello_reply.server =
      act::FindOrDie(fields, "server").ConsumeStringContentOrDie();
  hello_reply.version =
      act::FindOrDie(fields, "version").ConsumeStringContentOrDie();
  hello_reply.protocol_version = act::FindOrDie(fields, "proto").ToIntOrDie();
  hello_reply.id = act::FindOrDie(fields, "id").ToIntOrDie();
  hello_reply.mode = act::FindOrDie(fields, "mode").ConsumeStringContentOrDie();
  hello_reply.role = act::FindOrDie(fields, "role").ConsumeStringContentOrDie();

  // std::vector<Reply> modules_replies =
  //     std::get<ArrayReplyData>(act::FindOrDie(fields, "modules").data)
  //         .Consume();
  // hello_reply.modules.reserve(modules_replies.size());
  // for (Reply& module_reply : modules_replies) {
  //   auto module_reply_map =
  //       StatusOrConvertTo<absl::flat_hash_map<std::string, std::string>>(
  //           std::move(module_reply));
  //   if (!module_reply_map.ok()) {
  //     return module_reply_map.status();
  //   }
  //   hello_reply.modules.push_back(*std::move(module_reply_map));
  // }

  return hello_reply;
}

void Redis::ConnectCallback(const redisAsyncContext* context, int status) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::ConnectCallback called with redisAsyncContext not "
         "bound to Redis instance.";
  redis->OnConnect(status);
}

void Redis::DisconnectCallback(const redisAsyncContext* context, int status) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::DisconnectCallback called with redisAsyncContext not "
         "bound to Redis instance.";
  redis->OnDisconnect(status);
}

void Redis::PubsubCallback(redisAsyncContext* absl_nonnull context,
                           void* absl_nonnull hiredis_reply,
                           void* absl_nullable) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::PubsubCallback called with redisAsyncContext not "
         "bound to Redis instance.";

  if (hiredis_reply == nullptr) {
    return;
  }

  return redis->OnPubsubReply(hiredis_reply);
}

void Redis::ReplyCallback(redisAsyncContext* absl_nonnull context,
                          void* absl_nonnull hiredis_reply,
                          void* absl_nonnull privdata) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::ReplyCallback called with redisAsyncContext not "
         "bound to Redis instance.";

  const auto future = static_cast<internal::ReplyFuture*>(privdata);
  CHECK(future != nullptr)
      << "Redis::ReplyCallback called with null privdata (expected "
         "internal::ReplyFuture).";

  if (hiredis_reply == nullptr) {
    future->status = absl::InternalError(
        "Received null reply in ReplyCallback. "
        "This may indicate a connection error.");
    future->event.Notify();
    return;
  }

  absl::StatusOr<Reply> reply =
      ParseHiredisReply(static_cast<redisReply*>(hiredis_reply), /*free=*/true);
  if (!reply.ok()) {
    future->status = reply.status();
    LOG(ERROR) << "Failed to parse reply in ReplyCallback: "
               << future->status.message();
    future->event.Notify();
    return;
  }
  future->reply = std::move(*reply);
  future->status = absl::OkStatus();
  future->event.Notify();
}

void Redis::PushReplyCallback(redisAsyncContext* context, void* hiredis_reply) {
  DLOG(INFO) << "Redis::PushReplyCallback called with redisAsyncContext: "
             << context;
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::DisconnectCallback called with redisAsyncContext not "
         "bound to Redis instance.";
  redis->OnPushReply(static_cast<redisReply*>(hiredis_reply));
}

absl::StatusOr<std::unique_ptr<Redis>> Redis::Connect(std::string_view host,
                                                      int port) {
  auto redis = std::make_unique<Redis>(internal::PrivateConstructorTag{});

  redisOptions options{};
  options.options |= REDIS_OPT_NO_PUSH_AUTOFREE;
  options.options |= REDIS_OPT_NONBLOCK;
  options.options |= REDIS_OPT_NOAUTOFREEREPLIES;
  options.options |= REDIS_OPT_NOAUTOFREE;
  REDIS_OPTIONS_SET_TCP(&options, std::string(host).c_str(), port);
  REDIS_OPTIONS_SET_PRIVDATA(&options, redis.get(), [](void*) {});

  redisAsyncContext* context_ptr = redisAsyncConnectWithOptions(&options);
  std::unique_ptr<redisAsyncContext, internal::RedisContextDeleter> context(
      context_ptr);
  if (context == nullptr) {
    return absl::InternalError("Could not allocate async redis context.");
  }
  if (context->err) {
    return absl::InternalError(context->errstr);
  }

  context->data = redis.get();
  act::MutexLock lock(&redis->mu_);
  redis->context_ = std::move(context);

  redisLibuvAttach(redis->context_.get(), redis->event_loop_.Get()->raw());

  redisAsyncSetConnectCallback(redis->context_.get(), Redis::ConnectCallback);
  redisAsyncSetDisconnectCallback(redis->context_.get(),
                                  Redis::DisconnectCallback);
  redisAsyncSetPushCallback(redis->context_.get(), Redis::PushReplyCallback);

  while (!redis->connected_ && redis->status_.ok()) {
    redis->cv_.Wait(&redis->mu_);
  }

  redis->mu_.Unlock();
  RETURN_IF_ERROR(redis->Hello().status());
  redis->mu_.Lock();

  if (!redis->status_.ok()) {
    return redis->status_;
  }

  return redis;
}

Redis::~Redis() {
  act::MutexLock lock(&mu_);

  LOG(INFO) << "~Redis: Disconnecting from Redis server.";

  for (const auto& [channel, subscription] : subscriptions_) {
    ExecuteCommandWithGuards("UNSUBSCRIBE", {channel}).IgnoreError();
  }
  // for (auto& [channel, subscription_set] : subscriptions_) {
  //   for (const auto& subscription : subscription_set) {
  //     thread::Select({subscription->OnUnsubscribe()});
  //   }
  // }
  // subscriptions_.clear();

  while (num_pending_commands_ > 0) {
    cv_.Wait(&mu_);
  }
}

void Redis::SetKeyPrefix(std::string_view prefix) {
  act::MutexLock lock(&mu_);
  key_prefix_ = std::string(prefix);
}

std::string_view Redis::GetKeyPrefix() const {
  act::MutexLock lock(&mu_);
  return key_prefix_;
}

std::string Redis::GetKey(std::string_view key) const {
  act::MutexLock lock(&mu_);
  return absl::StrCat(key_prefix_, key);
}

absl::StatusOr<std::shared_ptr<Subscription>> Redis::Subscribe(
    std::string_view channel, absl::AnyInvocable<void(Reply)> on_message) {
  act::MutexLock lock(&mu_);

  if (channel.empty()) {
    LOG(ERROR) << "Subscribe called with empty channel.";
    return nullptr;
  }

  std::shared_ptr<Subscription> subscription;
  if (on_message) {
    // If a callback is provided, create a subscription with the callback.
    subscription = std::make_shared<Subscription>(std::move(on_message));
  } else {
    // Otherwise, create a subscription without a callback.
    subscription = std::make_shared<Subscription>();
  }

  subscriptions_[channel].insert(subscription);
  if (subscriptions_[channel].size() == 1) {
    const absl::StatusOr<Reply> reply =
        ExecuteCommandWithGuards("SUBSCRIBE", {channel});
    if (!reply.ok()) {
      subscriptions_[channel].erase(subscription);
      LOG(ERROR) << "Failed to subscribe to channel: " << channel
                 << ", error: " << reply.status().message();
      return reply.status();
    }
    if (reply->type != ReplyType::Nil) {
      LOG(ERROR) << "Unexpected reply type for SUBSCRIBE command: "
                 << static_cast<int>(reply->type);
      return absl::InternalError(
          "Unexpected reply type for SUBSCRIBE command.");
    }
  } else {
    subscription->Subscribe();
  }

  return subscription;
}

absl::Status Redis::Unsubscribe(std::string_view channel) {
  act::MutexLock lock(&mu_);
  return UnsubscribeInternal(channel);
}

void Redis::RemoveSubscription(
    std::string_view channel,
    const std::shared_ptr<Subscription>& subscription) {
  act::MutexLock lock(&mu_);
  auto it = subscriptions_.find(channel);
  if (it != subscriptions_.end()) {
    it->second.erase(subscription);
  }
  if (it->second.empty()) {
    // If no more subscriptions to this channel, unsubscribe from the channel.
    absl::Status status = UnsubscribeInternal(channel);
    subscription->Unsubscribe();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to unsubscribe from channel: " << channel
                 << ", error: " << status.message();
    }
  }
}

absl::Status Redis::UnsubscribeInternal(std::string_view channel)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (channel.empty()) {
    return absl::InvalidArgumentError("Channel cannot be empty.");
  }

  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandWithGuards("UNSUBSCRIBE", {channel}));

  if (reply.type != ReplyType::Nil) {
    LOG(ERROR) << "Unexpected reply type for UNSUBSCRIBE command: "
               << static_cast<int>(reply.type);
    return absl::InternalError(
        "Unexpected reply type for UNSUBSCRIBE command.");
  }

  return absl::OkStatus();
}

absl::StatusOr<Reply> Redis::ExecuteCommandWithGuards(std::string_view command,
                                                      const CommandArgs& args) {
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

absl::StatusOr<HelloReply> Redis::Hello(int protocol_version,
                                        std::string_view client_name,
                                        std::string_view username,
                                        std::string_view password) {
  act::MutexLock lock(&mu_);
  if (protocol_version != 2 && protocol_version != 3) {
    return absl::InvalidArgumentError(
        "Protocol version must be either 2 or 3.");
  }

  std::vector<std::string_view> args;
  std::string protocol_version_str = absl::StrCat(protocol_version);
  args.push_back(protocol_version_str);
  if (!client_name.empty()) {
    args.emplace_back("SETNAME");
    args.push_back(client_name);
  }
  if (!username.empty()) {
    args.emplace_back("AUTH");
    args.push_back(username);
    args.push_back(password);
  }

  ASSIGN_OR_RETURN(Reply reply, ExecuteCommandInternal("HELLO", args));
  if (reply.IsError()) {
    return GetStatusOrErrorFrom(reply);
  }

  ASSIGN_OR_RETURN(HelloReply hello_reply, HelloReply::From(std::move(reply)));
  return hello_reply;
}

absl::StatusOr<Reply> Redis::ExecuteCommand(std::string_view command,
                                            const CommandArgs& args) {
  act::MutexLock lock(&mu_);
  return ExecuteCommandWithGuards(command, args);
}

bool Redis::ParseReply(redisReply* hiredis_reply, Reply* reply_out) {
  mu_.Unlock();
  absl::StatusOr<Reply> reply = ParseHiredisReply(hiredis_reply, /*free=*/true);
  mu_.Lock();

  if (!status_.ok()) {
    LOG(ERROR) << "Cannot parse reply because Redis is in an error state: "
               << status_.message();
    return false;
  }

  if (!reply.ok()) {
    status_ = reply.status();
    LOG(ERROR) << "Failed to parse reply: " << reply.status().message();
    return false;
  }

  *reply_out = std::move(*reply);
  return true;
}

absl::Status Redis::CheckConnected() const {
  if (!connected_) {
    return absl::FailedPreconditionError(
        "Redis is not connected to the Redis server.");
  }
  return absl::OkStatus();
}

absl::StatusOr<Reply> Redis::ExecuteCommandInternal(std::string_view command,
                                                    const CommandArgs& args) {
  std::vector<size_t> arg_lengths;
  std::vector<const char*> arg_values;
  arg_lengths.reserve(args.size() + 1);
  arg_values.reserve(args.size() + 1);

  arg_lengths.push_back(command.size());
  arg_values.push_back(command.data());

  for (const auto& arg : args) {
    arg_lengths.push_back(arg.size());
    arg_values.push_back(arg.data());
  }

  bool subscribe = false;
  internal::ReplyFuture future;
  void* privdata = &future;
  redisCallbackFn* callback = Redis::ReplyCallback;

  // Subscription callbacks are handled differently: they are called
  // multiple times, once for each message received.
  if (command == "SUBSCRIBE" || command == "PSUBSCRIBE" ||
      command == "UNSUBSCRIBE" || command == "PUNSUBSCRIBE") {
    if (args.size() != 1) {
      return absl::InvalidArgumentError(
          "SUBSCRIBE and PSUBSCRIBE commands are only supported for one "
          "channel at a time.");
    }
    subscribe = true;
    privdata = nullptr;
    callback = Redis::PubsubCallback;
  }

  // mu_.Unlock();
  if (args.empty()) {
    redisAsyncCommand(context_.get(), callback, privdata, command.data());
  } else {
    redisAsyncCommandArgv(context_.get(), callback, privdata,
                          static_cast<int>(arg_values.size()),
                          arg_values.data(), arg_lengths.data());
  }
  // mu_.Lock();

  if (subscribe) {
    // // For subscription commands, we don't expect an immediate reply.
    // const auto subscription = static_cast<Subscription*>(privdata);
    // mu_.Unlock();
    // thread::Select({subscription->OnSubscribe()});
    // mu_.Lock();
    return Reply{.type = ReplyType::Nil, .data = NilReplyData{}};
  }

  mu_.Unlock();
  // Wait for the reply to be processed.
  thread::Select({thread::OnCancel(), future.event.OnEvent()});
  mu_.Lock();

  if (thread::Cancelled()) {
    return absl::CancelledError("Redis command was cancelled.");
  }
  RETURN_IF_ERROR(future.status);
  return std::move(future.reply);
}

void Redis::OnConnect(int status) {
  act::MutexLock lock(&mu_);
  if (status != REDIS_OK) {
    status_ = absl::InternalError("Failed to connect to Redis server.");
    connected_ = false;
  } else {
    status_ = absl::OkStatus();
    connected_ = true;
  }
  cv_.SignalAll();
  // TODO: attempt to reconnect
}

void Redis::OnDisconnect(int status) {
  act::MutexLock lock(&mu_);
  if (status != REDIS_OK) {
    status_ = absl::InternalError("Disconnected from Redis server.");
    connected_ = false;
    // TODO: attempt to reconnect
  } else {
    status_ = absl::OkStatus();
    connected_ = false;
  }
  cv_.SignalAll();
}

void Redis::OnPubsubReply(void* hiredis_reply) {
  act::MutexLock lock(&mu_);
  Reply reply;
  if (!ParseReply(static_cast<redisReply*>(hiredis_reply), &reply)) {
    LOG(ERROR) << "Failed to parse pubsub reply";
    return;
  }

  auto reply_elements = ConvertToOrDie<std::vector<Reply>>(std::move(reply));
  auto channel = ConvertToOrDie<std::string>(reply_elements[1]);
  if (reply_elements.empty()) {
    LOG(WARNING) << "Received empty reply in PubsubCallback for subscriptions "
                    "to channel: "
                 << channel;
    return;
  }

  if (const auto message_type = ConvertToOrDie<std::string>(reply_elements[0]);
      message_type == "subscribe" || message_type == "psubscribe") {
    for (const auto& subscription : subscriptions_[channel]) {
      // Notify all subscriptions about the new subscription.
      subscription->Subscribe();
    }
  } else if (message_type == "unsubscribe" || message_type == "punsubscribe") {
    for (const auto& subscription : subscriptions_[channel]) {
      // Notify all subscriptions about the unsubscription.
      subscription->Unsubscribe();
    }
  } else {
    // For regular messages, we pass the reply to the subscription.
    for (const auto& subscription : subscriptions_[channel]) {
      subscription->Message(reply_elements[2]);
    }
  }
}

void Redis::OnPushReply(redisReply* hiredis_reply) {
  act::MutexLock lock(&mu_);
  Reply reply;
  if (!ParseReply(hiredis_reply, &reply)) {
    LOG(ERROR) << "Failed to parse push reply.";
    return;
  }
  // Process the reply as needed.
}

absl::StatusOr<Reply> Redis::Get(std::string_view key) {
  act::MutexLock lock(&mu_);
  return ExecuteCommandWithGuards("GET", {key});
}

absl::Status Redis::Set(std::string_view key, std::string_view value) {
  act::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandWithGuards("SET", {key, value}));
  return GetStatusOrErrorFrom(reply);
}

absl::StatusOr<std::string> Redis::RegisterScript(std::string_view name,
                                                  std::string_view code,
                                                  bool overwrite_existing) {
  // Notice how overwrite_existing = false will NOT update the script code
  // if it's already registered, even if the code is different.
  act::MutexLock lock(&mu_);
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

absl::StatusOr<Reply> Redis::ExecuteScript(std::string_view name,
                                           CommandArgs script_keys,
                                           CommandArgs script_args) {
  act::MutexLock lock(&mu_);
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
Redis::ZRange(std::string_view key, int64_t start, int64_t end,
              bool withscores) {
  act::MutexLock lock(&mu_);
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
    auto value_map =
        ConvertTo<absl::flat_hash_map<std::string, int64_t>>(std::move(reply));
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

}  // namespace act::redis