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

#include "eglt/redis/redis.h"

#include <memory>
#include <string_view>
#include <utility>

#include <g3fiber/channel.h>
#include <hiredis/hiredis.h>

#include "eglt/redis/reply.h"
#include "eglt/redis/reply_converters.h"
#include "eglt/redis/reply_parsers.h"
#include "eglt/util/map_util.h"
#include "eglt/util/status_macros.h"

namespace eglt::redis {

static constexpr redisOptions GetDefaultRedisOptions() {
  constexpr redisOptions options{};
  return options;
}

internal::EventLoop::EventLoop() : loop_(uvw::loop::create()) {
  handle_ = loop_->resource<uvw::idle_handle>();
  handle_->init();

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
      eglt::FindOrDie(fields, "server").ConsumeStringContentOrDie();
  hello_reply.version =
      eglt::FindOrDie(fields, "version").ConsumeStringContentOrDie();
  hello_reply.protocol_version = eglt::FindOrDie(fields, "proto").ToIntOrDie();
  hello_reply.id = eglt::FindOrDie(fields, "id").ToIntOrDie();
  hello_reply.mode =
      eglt::FindOrDie(fields, "mode").ConsumeStringContentOrDie();
  hello_reply.role =
      eglt::FindOrDie(fields, "role").ConsumeStringContentOrDie();

  // std::vector<Reply> modules_replies =
  //     std::get<ArrayReplyData>(eglt::FindOrDie(fields, "modules").data)
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

void Redis::PubsubCallback(redisAsyncContext* context, void* hiredis_reply,
                           void* privdata) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::PubsubCallback called with redisAsyncContext not "
         "bound to Redis instance.";

  const auto subscription = static_cast<Subscription*>(privdata);
  if (hiredis_reply == nullptr) {
    return;
  }

  return redis->OnPubsubReply(hiredis_reply, subscription);
}

void Redis::ReplyCallback(redisAsyncContext* context, void* hiredis_reply,
                          void* privdata) {
  const auto redis = static_cast<Redis*>(context->data);
  CHECK(redis != nullptr)
      << "Redis::ReplyCallback called with redisAsyncContext not "
         "bound to Redis instance.";

  auto future = static_cast<internal::ReplyFuture*>(privdata);
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
  eglt::MutexLock lock(&redis->mu_);
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
  eglt::MutexLock lock(&mu_);
  while (num_pending_commands_ > 0) {
    cv_.Wait(&mu_);
  }
}

absl::StatusOr<std::shared_ptr<Subscription>> Redis::Subscribe(
    std::string_view channel, absl::AnyInvocable<void(Reply)> on_message) {
  eglt::MutexLock lock(&mu_);

  if (channel.empty()) {
    LOG(ERROR) << "Subscribe called with empty channel.";
    return nullptr;
  }

  if (subscriptions_.contains(channel)) {
    LOG(INFO) << "Already subscribed to channel: " << channel;
    return subscriptions_[channel];
  }

  std::shared_ptr<Subscription> subscription;
  if (on_message) {
    // If a callback is provided, create a subscription with the callback.
    subscription = std::make_shared<Subscription>(std::move(on_message));
  } else {
    // Otherwise, create a subscription without a callback.
    subscription = std::make_shared<Subscription>();
  }
  subscriptions_[channel] = subscription;

  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandInternal("SUBSCRIBE", {channel}));
  if (reply.type != ReplyType::Nil) {
    LOG(ERROR) << "Unexpected reply type for SUBSCRIBE command: "
               << static_cast<int>(reply.type);
    return absl::InternalError("Unexpected reply type for SUBSCRIBE command.");
  }

  return subscription;
}

absl::StatusOr<HelloReply> Redis::Hello(int protocol_version,
                                        std::string_view client_name,
                                        std::string_view username,
                                        std::string_view password) {
  eglt::MutexLock lock(&mu_);
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
  eglt::MutexLock lock(&mu_);
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
  if (command == "SUBSCRIBE" || command == "PSUBSCRIBE") {
    if (args.size() != 1) {
      return absl::InvalidArgumentError(
          "SUBSCRIBE and PSUBSCRIBE commands are only supported for one "
          "channel at a time.");
    }
    subscribe = true;
    if (subscriptions_.contains(args[0])) {
      privdata = subscriptions_[args[0]].get();
    } else {
      // Create a new subscription.
      auto subscription = std::make_shared<Subscription>();
      privdata = subscription.get();
      subscriptions_[args[0]] = std::move(subscription);
    }
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
    // For subscription commands, we don't expect an immediate reply.
    const auto subscription = static_cast<Subscription*>(privdata);
    mu_.Unlock();
    thread::Select({subscription->OnSubscribe()});
    mu_.Lock();
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
  eglt::MutexLock lock(&mu_);
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
  eglt::MutexLock lock(&mu_);
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

void Redis::OnPubsubReply(void* hiredis_reply, Subscription* subscription) {
  eglt::MutexLock lock(&mu_);

  Reply reply;
  if (!ParseReply(static_cast<redisReply*>(hiredis_reply), &reply)) {
    LOG(ERROR) << "Failed to parse pubsub reply for subscription at "
               << subscription;
    return;
  }

  auto reply_elements = ConvertToOrDie<std::vector<Reply>>(std::move(reply));
  if (reply_elements.empty()) {
    LOG(WARNING)
        << "Received empty reply in PubsubCallback for subscription at "
        << subscription;
    return;
  }

  if (const auto message_type = ConvertToOrDie<std::string>(reply_elements[0]);
      message_type == "subscribe" || message_type == "psubscribe") {
    subscription->Subscribe();
  } else if (message_type == "unsubscribe" || message_type == "punsubscribe") {
    subscription->Unsubscribe();
  } else {
    // For regular messages, we pass the reply to the subscription.
    subscription->Message(std::move(reply_elements[2]));
  }
}

void Redis::OnPushReply(redisReply* hiredis_reply) {
  eglt::MutexLock lock(&mu_);
  Reply reply;
  if (!ParseReply(hiredis_reply, &reply)) {
    LOG(ERROR) << "Failed to parse push reply.";
    return;
  }
  // Process the reply as needed.
}

absl::StatusOr<Reply> Redis::Get(std::string_view key) {
  eglt::MutexLock lock(&mu_);
  return ExecuteCommandWithGuards("GET", {key});
}

absl::Status Redis::Set(std::string_view key, std::string_view value) {
  eglt::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandWithGuards("SET", {key, value}));
  return GetStatusOrErrorFrom(reply);
}

}  // namespace eglt::redis