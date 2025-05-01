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

/**
 * @file
 * @brief
 *   Concurrency utilities for Evergreen.
 *
 * This file provides a set of concurrency utilities for use in Evergreen.
 * It includes classes and functions for managing fibers, channels, and
 * synchronization primitives.
 */

#ifndef EGLT_CONCURRENCY_CONCURRENCY_H_
#define EGLT_CONCURRENCY_CONCURRENCY_H_

#include "thread_on_boost/concurrency.h"

namespace eglt::concurrency {
using Case = impl::Case;
using CaseArray = impl::CaseArray;
using TreeOptions = impl::TreeOptions;

template <typename T>
using ChannelReader = impl::ChannelReader<T>;

template <typename T>
using ChannelWriter = impl::ChannelWriter<T>;

template <typename T>
using Channel = impl::Channel<T>;

using Fiber = impl::Fiber;
using PermanentEvent = impl::PermanentEvent;

using CondVar = impl::CondVar;
using Mutex = impl::Mutex;
using MutexLock = impl::MutexLock;

inline void SleepFor(absl::Duration duration) {
  impl::SleepFor(duration);
}

class ABSL_SCOPED_LOCKABLE TwoMutexLock {
 public:
  explicit TwoMutexLock(Mutex* absl_nonnull mu1, Mutex* absl_nonnull mu2)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu1, mu2)
      : mu1_(mu1), mu2_(mu2) {
    if (ABSL_PREDICT_FALSE(mu1_ == mu2_)) {
      mu1_->Lock();
      return;
    }

    if (mu1 < mu2) {
      mu1_->Lock();
      mu2_->Lock();
    } else {
      mu2_->Lock();
      mu1_->Lock();
    }
  }

  TwoMutexLock(const TwoMutexLock&) = delete;  // NOLINT(runtime/mutex)
  TwoMutexLock(TwoMutexLock&&) = delete;       // NOLINT(runtime/mutex)
  TwoMutexLock& operator=(const TwoMutexLock&) = delete;
  TwoMutexLock& operator=(TwoMutexLock&&) = delete;

  ~TwoMutexLock() ABSL_UNLOCK_FUNCTION() {
    if (ABSL_PREDICT_FALSE(mu1_ == mu2_)) {
      mu1_->Unlock();
      return;
    }

    if (mu1_ < mu2_) {
      mu2_->Unlock();
      mu1_->Unlock();
    } else {
      mu1_->Unlock();
      mu2_->Unlock();
    }
  }

 private:
  Mutex* absl_nonnull const mu1_;
  Mutex* absl_nonnull const mu2_;
};

inline void JoinOptimally(Fiber* fiber) {
  impl::JoinOptimally(fiber);
}

inline bool Cancelled() {
  return impl::Cancelled();
}

/**
 * @brief Gets the current fiber's cancellation case.
 *
 * This case can be passed to Select() to wait for cancellation.
 *
 * @return The cancellation case.
 */
inline Case OnCancel() {
  return impl::OnCancel();
}

inline int Select(const CaseArray& cases) {
  return impl::Select(cases);
}

inline int SelectUntil(const absl::Time deadline, const CaseArray& cases) {
  return impl::SelectUntil(deadline, cases);
}

inline void Detach(std::unique_ptr<Fiber> fiber) {
  impl::Detach(std::move(fiber));
}

inline void Detach(const TreeOptions& options,
                   absl::AnyInvocable<void()>&& fn) {
  impl::Detach(options, std::move(fn));
}

inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options,
                                      absl::AnyInvocable<void()>&& fn) {
  return impl::NewTree(options, std::move(fn));
}

// If fn() is void, result holder cannot be created
template <typename Invocable, typename = std::enable_if_t<!std::is_void_v<
                                  std::invoke_result_t<Invocable>>>>
auto RunInFiber(Invocable&& fn) {
  decltype(fn()) result;
  auto fiber = Fiber(
      [&result, fn = std::forward<Invocable>(fn)]() mutable { result = fn(); });
  fiber.Join();
  return result;
}

inline auto RunInFiber(absl::AnyInvocable<void()>&& fn) {
  auto fiber = Fiber(std::move(fn));
  fiber.Join();
}
}  // namespace eglt::concurrency

#endif  // EGLT_CONCURRENCY_CONCURRENCY_H_
