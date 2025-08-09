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

#include "actionengine/redis/pubsub.h"

namespace act::redis {

Subscription::Subscription(size_t capacity) : channel_(capacity) {}

Subscription::Subscription(absl::AnyInvocable<void(Reply)> on_message)
    : channel_(1), on_message_(std::move(on_message)) {
  CHECK(on_message_)
      << "on_message must be set for Subscription with a callback";
  CloseWriter();
}

Subscription::~Subscription() {
  act::MutexLock lock(&mu_);
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
  act::MutexLock lock(&mu_);
  CloseWriter();
  unsubscribe_event_.Notify();
}

}  // namespace act::redis