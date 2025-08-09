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

#ifndef ACTIONENGINE_REDIS_PUBSUB_H_
#define ACTIONENGINE_REDIS_PUBSUB_H_

#include <g3fiber/channel.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/redis/reply.h"

namespace act::redis {

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

  mutable act::Mutex mu_;
  bool writer_closed_ ABSL_GUARDED_BY(mu_) = false;

  thread::Channel<Reply> channel_;
  absl::AnyInvocable<void(Reply)> on_message_;
  thread::PermanentEvent subscribe_event_;
  thread::PermanentEvent unsubscribe_event_;
};

}  // namespace act::redis

#endif  // ACTIONENGINE_REDIS_PUBSUB_H_