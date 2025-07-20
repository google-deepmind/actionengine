#include "eglt/redis/pubsub.h"

namespace eglt::redis {

Subscription::Subscription(size_t capacity) : channel_(capacity) {}

Subscription::Subscription(absl::AnyInvocable<void(Reply)> on_message)
    : channel_(1), on_message_(std::move(on_message)) {
  CHECK(on_message_)
      << "on_message must be set for Subscription with a callback";
  CloseWriter();
}

Subscription::~Subscription() {
  eglt::MutexLock lock(&mu_);
  CloseWriter();
}

thread::Case Subscription::OnSubscribe() const {
  return subscribe_event_.OnEvent();
}

thread::Case Subscription::OnUnsubscribe() const {
  return unsubscribe_event_.OnEvent();
}

void Subscription::Message(Reply reply) {
  if (on_message_) {
    // If a callback is set, invoke it with the reply.
    on_message_(std::move(reply));
    return;
  }

  // If no callback is set, we write the reply to the channel.
  channel_.writer()->Write(std::move(reply));
}

void Subscription::Subscribe() {
  subscribe_event_.Notify();
}

void Subscription::Unsubscribe() {
  eglt::MutexLock lock(&mu_);
  DLOG(INFO) << "Unsubscribing from channel.";
  CloseWriter();
  unsubscribe_event_.Notify();
}

}  // namespace eglt::redis