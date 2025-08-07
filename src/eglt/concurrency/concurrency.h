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
 *   Concurrency utilities for ActionEngine.
 *
 * This file provides a set of concurrency utilities for use in ActionEngine.
 * It includes classes and functions for managing fibers, channels, and
 * synchronization primitives.
 */

#ifndef EGLT_CONCURRENCY_CONCURRENCY_H_
#define EGLT_CONCURRENCY_CONCURRENCY_H_

#include <type_traits>
#include <utility>

#include <absl/base/attributes.h>
#include <absl/base/nullability.h>
#include <absl/base/optimization.h>
#include <absl/base/thread_annotations.h>
#include <absl/functional/any_invocable.h>
#include <absl/log/check.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

// IWYU pragma: begin_exports
#if GOOGLE3
#include "eglt/google3_concurrency_headers.h"
#else
#include "g3fiber/concurrency.h"
#endif  // GOOGLE3
// IWYU pragma: end_exports

namespace eglt::concurrency {

#if GOOGLE3
using CondVar = absl::CondVar;
using Mutex = absl::Mutex;
using MutexLock = absl::MutexLock;

inline void SleepFor(absl::Duration duration) {
  absl::SleepFor(duration);
}
#else
using CondVar = impl::CondVar;
using Mutex = impl::Mutex;
using MutexLock = impl::MutexLock;

void SleepFor(absl::Duration duration);
#endif  // GOOGLE3

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
  TwoMutexLock& operator=(const TwoMutexLock&) = delete;

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

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED ExclusiveAccessGuard {
 public:
  ExclusiveAccessGuard(Mutex* absl_nonnull mutex, CondVar* absl_nonnull cv)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex)
      : mu_(mutex), cv_(cv) {}

  ExclusiveAccessGuard(const ExclusiveAccessGuard&) = delete;
  ExclusiveAccessGuard& operator=(const ExclusiveAccessGuard&) = delete;

  ~ExclusiveAccessGuard() {
    CHECK(pending_operations_ == 0) << "FinalizationGuard destroyed with "
                                       "pending operations";
  }

  void Wait() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION() {
    while (pending_operations_ > 0) {
      cv_->Wait(mu_);
    }
  }

  [[nodiscard]] bool WaitWithTimeout(absl::Duration timeout) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION() {
    bool timed_out = false;
    while (pending_operations_ > 0) {
      if (timed_out = cv_->WaitWithTimeout(mu_, timeout); timed_out) {
        break;
      }
    }
    return timed_out;
  }

  [[nodiscard]] bool WaitWithDeadline(absl::Time deadline) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION() {
    bool timed_out = false;
    while (pending_operations_ > 0) {
      if (timed_out = cv_->WaitWithDeadline(mu_, deadline); timed_out) {
        break;
      }
    }
    return timed_out;
  }

  void StartPendingOperation() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_)
      ABSL_UNLOCK_FUNCTION(mu_) {
    ++pending_operations_;
    mu_->Unlock();
  }

  void StartBlockingPendingOperation() { ++pending_operations_; }

  void FinishPendingOperation() ABSL_LOCKS_EXCLUDED(mu_)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu_) {
    mu_->Lock();
    --pending_operations_;
    cv_->SignalAll();
  }

  void FinishBlockingPendingOperation() {
    --pending_operations_;
    cv_->SignalAll();
  }

 private:
  friend class EnsureExclusiveAccess;
  friend class PreventExclusiveAccess;

  Mutex* absl_nonnull const mu_;
  CondVar* absl_nonnull const cv_;
  int pending_operations_ = 0;
};

class ABSL_SCOPED_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED EnsureExclusiveAccess {
 public:
  explicit EnsureExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(guard)
      : EnsureExclusiveAccess(guard, absl::InfiniteFuture()) {}

  explicit EnsureExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard,
                                 absl::Time deadline)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(guard)
      : guard_(guard) {
    timed_out_ = guard_->WaitWithDeadline(deadline);
  }

  EnsureExclusiveAccess(const EnsureExclusiveAccess&) = delete;
  EnsureExclusiveAccess& operator=(const EnsureExclusiveAccess&) = delete;

  ~EnsureExclusiveAccess() ABSL_UNLOCK_FUNCTION() { guard_->cv_->SignalAll(); }

  static EnsureExclusiveAccess WithDeadline(
      ExclusiveAccessGuard* absl_nonnull guard,
      absl::Time deadline) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return EnsureExclusiveAccess(guard, deadline);
  }

  static EnsureExclusiveAccess WithTimeout(
      ExclusiveAccessGuard* absl_nonnull guard,
      absl::Duration timeout) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return EnsureExclusiveAccess(guard, absl::Now() + timeout);
  }

  [[nodiscard]] bool TimedOut() const { return timed_out_; }

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
  bool timed_out_{false};
};

class ABSL_SCOPED_LOCKABLE PreventExclusiveAccess {
 public:
  explicit PreventExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard,
                                  bool retain_lock = false)
      ABSL_SHARED_LOCK_FUNCTION(guard)
      : guard_(guard), retain_lock_(retain_lock) {
    if (retain_lock_) {
      guard_->StartBlockingPendingOperation();
    } else {
      guard_->StartPendingOperation();
    }
  }

  PreventExclusiveAccess(const PreventExclusiveAccess&) = delete;
  PreventExclusiveAccess& operator=(const PreventExclusiveAccess&) = delete;

  ~PreventExclusiveAccess() ABSL_UNLOCK_FUNCTION() {
    if (retain_lock_) {
      guard_->FinishBlockingPendingOperation();
    } else {
      guard_->FinishPendingOperation();
    }
  }

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
  bool retain_lock_;
};

// If fn() is void, result holder cannot be created
template <typename Invocable, typename = std::enable_if_t<!std::is_void_v<
                                  std::invoke_result_t<Invocable>>>>
auto RunInFiber(Invocable&& fn) {
  decltype(fn()) result;
  auto fiber = thread::Fiber(
      [&result, fn = std::forward<Invocable>(fn)]() mutable { result = fn(); });
  fiber.Join();
  return result;
}

inline auto RunInFiber(absl::AnyInvocable<void()>&& fn) {
  auto fiber = thread::Fiber(std::move(fn));
  fiber.Join();
}
}  // namespace eglt::concurrency

namespace eglt {

using concurrency::CondVar;
using concurrency::Mutex;
using concurrency::MutexLock;

inline void SleepFor(absl::Duration duration) {
  concurrency::SleepFor(duration);
}

}  // namespace eglt

#endif  // EGLT_CONCURRENCY_CONCURRENCY_H_
