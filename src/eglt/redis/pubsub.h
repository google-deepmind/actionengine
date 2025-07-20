#ifndef EGLT_REDIS_PUBSUB_H_
#define EGLT_REDIS_PUBSUB_H_

#include <g3fiber/channel.h>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/redis/reply.h"

namespace eglt::redis {

class Subscription {
 public:
  static constexpr size_t kDefaultChannelCapacity = 16;

  explicit Subscription(size_t capacity = kDefaultChannelCapacity);
  explicit Subscription(absl::AnyInvocable<void(Reply)> on_message);

  ~Subscription();

  thread::Case OnSubscribe() const;
  thread::Case OnUnsubscribe() const;
  thread::Reader<Reply>* GetReader() { return channel_.reader(); }

 private:
  friend class Redis;

  void Message(Reply reply);
  void Subscribe();
  void Unsubscribe();

  void CloseWriter() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (writer_closed_) {
      return;
    }
    writer_closed_ = true;
    channel_.writer()->Close();
  }

  mutable eglt::Mutex mu_;
  bool writer_closed_ ABSL_GUARDED_BY(mu_) = false;

  thread::Channel<Reply> channel_;
  absl::AnyInvocable<void(Reply)> on_message_;
  thread::PermanentEvent subscribe_event_;
  thread::PermanentEvent unsubscribe_event_;
};

}  // namespace eglt::redis

#endif  // EGLT_REDIS_PUBSUB_H_