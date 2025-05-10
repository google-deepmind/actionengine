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

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED ExclusiveAccessGuard {
 public:
  ExclusiveAccessGuard(Mutex* absl_nonnull mutex, CondVar* absl_nonnull cv)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex)
      : mutex_(mutex), cv_(cv) {}

  ~ExclusiveAccessGuard() {
    CHECK(pending_operations_ == 0) << "FinalizationGuard destroyed with "
                                       "pending operations";
  }

  void Wait() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      ABSL_UNLOCK_FUNCTION() {
    while (pending_operations_ > 0) {
      cv_->Wait(mutex_);
    }
  }

  [[nodiscard]] bool WaitWithTimeout(absl::Duration timeout) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) ABSL_UNLOCK_FUNCTION() {
    bool timed_out = false;
    while (pending_operations_ > 0) {
      if (timed_out = cv_->WaitWithTimeout(mutex_, timeout); timed_out) {
        break;
      }
    }
    return timed_out;
  }

  [[nodiscard]] bool WaitWithDeadline(absl::Time deadline) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) ABSL_UNLOCK_FUNCTION() {
    bool timed_out = false;
    while (pending_operations_ > 0) {
      if (timed_out = cv_->WaitWithDeadline(mutex_, deadline); timed_out) {
        break;
      }
    }
    return timed_out;
  }

  void StartPendingOperation() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      ABSL_UNLOCK_FUNCTION(mutex_) {
    ++pending_operations_;
    mutex_->Unlock();
  }

  void FinishPendingOperation() ABSL_LOCKS_EXCLUDED(mutex_)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mutex_) {
    mutex_->Lock();
    --pending_operations_;
    cv_->SignalAll();
  }

 private:
  friend class EnsureExclusiveAccess;

  Mutex* absl_nonnull const mutex_;
  CondVar* absl_nonnull const cv_ ABSL_GUARDED_BY(mutex_);
  int pending_operations_ ABSL_GUARDED_BY(mutex_) = 0;
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

  ~EnsureExclusiveAccess() ABSL_UNLOCK_FUNCTION() { guard_->cv_->SignalAll(); }

  [[nodiscard]] bool TimedOut() const { return timed_out_; }

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
  bool timed_out_{false};
};

class ABSL_SCOPED_LOCKABLE PreventExclusiveAccess {
 public:
  explicit PreventExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard)
      ABSL_SHARED_LOCK_FUNCTION(guard)
      : guard_(guard) {
    guard_->StartPendingOperation();
  }

  ~PreventExclusiveAccess() ABSL_UNLOCK_FUNCTION() {
    guard_->FinishPendingOperation();
  }

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
};

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

inline Case NonSelectableCase() {
  return impl::NonSelectableCase();
}

inline Case AlwaysSelectableCase() {
  return impl::AlwaysSelectableCase();
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
