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
  std::vector<std::string> modules;
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

  absl::StatusOr<Reply> ExecuteCommand(std::string_view command,
                                       const CommandArgs& args) const;

  absl::StatusOr<Reply> ExecuteCommand(
      std::string_view command,
      std::initializer_list<std::string_view> args = {}) const;

  template <typename... Args>
  absl::StatusOr<Reply> ExecuteCommand(std::string_view command,
                                       std::string_view arg0,
                                       Args&&... args) const;

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
                     StatusOrConvertTo<std::string>(std::forward<T>(value)));
    return Set(key, std::string_view(value_str));
  }

  absl::Status Multi() const {
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommand("MULTI"));
    ASSIGN_OR_RETURN(auto status_string,
                     StatusOrConvertTo<std::string>(std::move(reply)));
    if (status_string != "OK") {
      return absl::InternalError(
          absl::StrCat("Expected OK after MULTI, got: ", status_string));
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::vector<Reply>> Exec() const {
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommand("EXEC"));
    if (reply.type == ReplyType::Nil) {
      return absl::InternalError(
          "EXEC returned a nil reply, which indicates that the transaction was "
          "discarded or no commands were executed.");
    }
    if (reply.type != ReplyType::Array) {
      return absl::InternalError(absl::StrCat(
          "Expected an array reply after EXEC, got: ", reply.type));
    }
    return StatusOrConvertTo<std::vector<Reply>>(std::move(reply));
  }

  absl::Status Watch(std::initializer_list<std::string_view> keys) const {
    ASSIGN_OR_RETURN(Reply reply, ExecuteCommand("WATCH", std::move(keys)));
    ASSIGN_OR_RETURN(auto status_string,
                     StatusOrConvertTo<std::string>(std::move(reply)));
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
    if (channel.empty()) {
      CHECK_OK(ExecuteCommand("UNSUBSCRIBE", {}).status());
    } else {
      CHECK_OK(ExecuteCommand("UNSUBSCRIBE", channel).status());
    }
    eglt::MutexLock lock(&mu_);
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

    CHECK_OK(ExecuteCommand("SUBSCRIBE", channel).status());
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

    CHECK_OK(ExecuteCommand("SUBSCRIBE", channel).status());
    return subscription;
  }

  absl::Status GetPushReplyStatus() const;
  [[nodiscard]] thread::Reader<PushReplyData>* GetPushReplyReader();
  [[nodiscard]] redisContext* GetContext() const;

  explicit Redis(internal::PrivateConstructorTag _,
                 size_t push_reply_channel_capacity = 64);

 private:
  void SetContext(
      std::unique_ptr<redisContext, internal::RedisContextDeleter> context);
  [[nodiscard]] std::string_view GetContextErrorMessage() const;

  static void HandlePushReply(void* absl_nonnull privdata,
                              void* absl_nonnull hiredis_reply);
  void ClosePushReplyWriter() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable eglt::Mutex mu_;
  std::unique_ptr<redisContext, internal::RedisContextDeleter> context_;

  thread::Channel<PushReplyData> push_reply_channel_;
  absl::Status push_reply_status_ ABSL_GUARDED_BY(mu_);
  bool push_replies_closed_ ABSL_GUARDED_BY(mu_) = false;
  std::string key_prefix_ ABSL_GUARDED_BY(mu_) = "eglt:redis:";

  absl::flat_hash_map<std::string, std::shared_ptr<Subscription>> subscriptions_
      ABSL_GUARDED_BY(mu_);
};

template <typename... Args>
absl::StatusOr<Reply> Redis::ExecuteCommand(std::string_view command,
                                            std::string_view arg0,
                                            Args&&... args) const {
  const std::initializer_list<std::string_view> command_args{
      arg0, std::forward<Args>(args)...};
  return ExecuteCommand(command, command_args);
}

template <typename T>
absl::StatusOr<T> Redis::Get(std::string_view key) const {
  ASSIGN_OR_RETURN(Reply reply, Get(key));
  return StatusOrConvertTo<T>(reply);
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