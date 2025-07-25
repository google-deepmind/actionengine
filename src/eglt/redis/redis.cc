#include <memory>
#include <string_view>
#include <utility>

#include <g3fiber/channel.h>
#include <hiredis/hiredis.h>

#include "eglt/absl_headers.h"
#include "eglt/redis/redis.h"
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

Redis::Redis(internal::PrivateConstructorTag _,
             size_t push_reply_channel_capacity)
    : push_reply_channel_(push_reply_channel_capacity) {}

Redis::~Redis() {
  eglt::MutexLock lock(&mu_);
  ClosePushReplyWriter();
}

absl::StatusOr<Reply> Redis::ExecuteCommandInternal(
    std::string_view command, const CommandArgs& args) const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
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

  if (args.size() == 0) {
    redisAppendCommand(context_.get(), command.data());
  } else {
    redisAppendCommandArgv(context_.get(), static_cast<int>(arg_values.size()),
                           arg_values.data(), arg_lengths.data());
  }

  if (command == "SUBSCRIBE" || command == "PSUBSCRIBE" ||
      command == "UNSUBSCRIBE" || command == "PUNSUBSCRIBE") {
    // For subscription commands, we don't expect an immediate reply.
    return Reply{.type = ReplyType::Nil, .data = NilReplyData{}};
  }

  void* hiredis_reply = nullptr;
  if (redisGetReply(context_.get(), &hiredis_reply) != REDIS_OK) {
    return absl::InternalError(
        absl::StrCat("Failed to execute command: ", command, ". ",
                     GetContextErrorMessage()));
  }

  if (hiredis_reply == nullptr) {
    return absl::InternalError(
        absl::StrCat("Failed to execute command: ", command, ". ",
                     GetContextErrorMessage()));
  }

  return ParseHiredisReply(static_cast<redisReply*>(hiredis_reply));
}

absl::StatusOr<Reply> Redis::ExecuteCommandInternal(
    std::string_view command,
    std::initializer_list<std::string_view> args) const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  return ExecuteCommandInternal(command, std::vector(std::move(args)));
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

absl::StatusOr<HelloReply> Redis::Hello(int protocol_version,
                                        std::string_view client_name,
                                        std::string_view username,
                                        std::string_view password) const {
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

absl::StatusOr<std::unique_ptr<Redis>> Redis::Connect(std::string_view host,
                                                      int port) {
  auto redis = std::make_unique<Redis>(internal::PrivateConstructorTag{});

  redisOptions options = GetDefaultRedisOptions();
  options.options |= REDIS_OPT_NO_PUSH_AUTOFREE;
  REDIS_OPTIONS_SET_TCP(&options, std::string(host).c_str(), port);
  REDIS_OPTIONS_SET_PRIVDATA(&options, redis.get(), [](void*) {});

  redisContext* context = redisConnectWithOptions(&options);
  if (context == nullptr) {
    return absl::InternalError("Could not allocate redis context.");
  }
  if (context->err) {
    return absl::InternalError(context->errstr);
  }

  redis->SetContext(
      std::unique_ptr<redisContext, internal::RedisContextDeleter>(context));

  redisSetPushCallback(context, Redis::HandlePushReply);

  return redis;
}

absl::Status Redis::Ping(std::string_view expected_message) const {
  eglt::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(Reply reply,
                   ExecuteCommandInternal("PING", expected_message));
  const std::string ping_reply = reply.ConsumeStringContentOrDie();
  if (ping_reply != expected_message) {
    return absl::InternalError(
        absl::StrCat("Unexpected PING reply: ", ping_reply));
  }
  return absl::OkStatus();
}

absl::StatusOr<Reply> Redis::Get(std::string_view key) const {
  eglt::MutexLock lock(&mu_);
  return ExecuteCommandInternal("GET", key);
}

absl::Status Redis::Set(std::string_view key, std::string_view value) const {
  eglt::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandInternal("SET", key, value));
  return GetStatusOrErrorFrom(reply);
}

absl::StatusOr<int> Redis::Publish(std::string_view channel,
                                   std::string_view message) const {
  eglt::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(const Reply reply,
                   ExecuteCommandInternal("PUBLISH", channel, message));
  return reply.ToInt();
}

absl::Status Redis::GetPushReplyStatus() const {
  eglt::MutexLock lock(&mu_);
  return push_reply_status_;
}

thread::Reader<PushReplyData>* Redis::GetPushReplyReader() {
  return push_reply_channel_.reader();
}

redisContext* Redis::GetContext() const {
  return context_.get();
}

std::string_view Redis::GetContextErrorMessage() const {
  if (context_->err) {
    return {context_->errstr};
  }
  return "";
}

void Redis::SetContext(
    std::unique_ptr<redisContext, internal::RedisContextDeleter> context) {
  context_ = std::move(context);
}

void Redis::HandlePushReply(void* absl_nonnull privdata,
                            void* absl_nonnull hiredis_reply)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  const auto redis = static_cast<Redis*>(privdata);
  CHECK(redis != nullptr);

  if (hiredis_reply == nullptr) {
    return;
  }
  absl::StatusOr<Reply> reply =
      ParseHiredisReply(static_cast<redisReply*>(hiredis_reply));

  if (!redis->push_reply_status_.ok()) {
    // If we have a previous error, we don't process further replies.
    return;
  }

  if (redis->push_replies_closed_) {
    LOG(WARNING) << "Push reply received after push replies were closed. "
                 << "This is unexpected and may indicate a bug in the client.";
  }

  if (!reply.ok()) {
    // An error means no further replies will be processed.
    LOG(ERROR) << "Error processing push reply: " << reply.status();
    redis->push_reply_status_ = reply.status();
    redis->ClosePushReplyWriter();
    return;
  }

  DCHECK(reply->type == ReplyType::Push)
      << "Expected a Push reply, got type coded as: "
      << static_cast<int>(reply->type);

  const auto& [_, value_array] = std::get<PushReplyData>(reply->data);
  const std::string_view message_type =
      std::get<StringReplyData>(value_array.values[0].data).value;

  // Route to subscription channels if any.
  if (message_type == "message" || message_type == "subscribe" ||
      message_type == "unsubscribe") {
    const std::string_view channel_name =
        std::get<StringReplyData>(value_array.values[1].data).value;
    const std::shared_ptr<Subscription> subscription =
        FindOrDie(redis->subscriptions_, channel_name);

    if (message_type == "subscribe") {
      subscription->Subscribe();
      return;
    }
    if (message_type == "unsubscribe") {
      subscription->Unsubscribe();
      return;
    }
    Reply message_content = std::move(value_array.values[2]);
    subscription->Message(std::move(message_content));
    return;
  }

  // If not a subscription message, we push the reply to the push reply channel.
  redis->push_reply_channel_.writer()->WriteUnlessCancelled(
      std::move(std::get<PushReplyData>(reply->data)));
}

void Redis::ClosePushReplyWriter() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (push_replies_closed_) {
    return;
  }
  push_replies_closed_ = true;
  push_reply_channel_.writer()->Close();
}

}  // namespace eglt::redis