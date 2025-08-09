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

#include <absl/base/attributes.h>
#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/time/time.h>

#include "g3fiber/concurrency.h"

// IWYU pragma: begin_exports
#if GOOGLE3
#include "eglt/google3_concurrency_headers.h"
#else
#include "g3fiber/concurrency.h"
#endif  // GOOGLE3
// IWYU pragma: end_exports

/** @file
 *  @brief
 *    Concurrency utilities for ActionEngine.
 *
 *  This file provides a set of concurrency utilities for use in ActionEngine.
 *  That includes classes and functions for managing fibers, channels, and
 *  basic synchronization primitives: mutexes and condition variables.
 *
 *  This implementation is based on Boost::fiber, but it is designed to mimic
 *  closely the `thread::Fiber` and `thread::Channel` interfaces used
 *  internally at Google. A fully compatible implementation is neither a
 *  guarantee nor a goal, but the API is designed to be similar enough
 *  to be used interchangeably in most cases. The `Mutex` and `CondVar` classes
 *  provide the same basic functionality as their counterparts in Abseil.
 *
 *  @headerfile eglt/concurrency/concurrency.h
 */

/** @private */
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

/** @brief
 *    A lock that locks two mutexes at once.
 *
 *  This class is used to lock two mutexes at once, ensuring that they are
 *  locked in a deadlock-free manner. It is useful for situations where two
 *  mutexes need to be locked together, such as when accessing shared data
 *  structures that are protected by multiple mutexes, or in move constructors
 *  where two mutexes need to be held to ensure thread safety.
 *
 *  There is no sophisticated deadlock prevention logic in this class, but
 *  rather a simple ordering of mutexes based on their addresses.
 */
class ABSL_SCOPED_LOCKABLE TwoMutexLock {
 public:
  explicit TwoMutexLock(Mutex* absl_nonnull mu1, Mutex* absl_nonnull mu2)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu1, mu2);

  // This class is not copyable or movable.
  TwoMutexLock(const TwoMutexLock&) = delete;  // NOLINT(runtime/mutex)
  TwoMutexLock& operator=(const TwoMutexLock&) = delete;

  ~TwoMutexLock() ABSL_UNLOCK_FUNCTION();

 private:
  Mutex* absl_nonnull const mu1_;
  Mutex* absl_nonnull const mu2_;
};

/** @private */
class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED ExclusiveAccessGuard {
 public:
  ExclusiveAccessGuard(Mutex* absl_nonnull mutex, CondVar* absl_nonnull cv)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex);

  // This class is not copyable or movable.
  ExclusiveAccessGuard(const ExclusiveAccessGuard&) = delete;
  ExclusiveAccessGuard& operator=(const ExclusiveAccessGuard&) = delete;

  ~ExclusiveAccessGuard();

  void Wait() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION();

  [[nodiscard]] bool WaitWithTimeout(absl::Duration timeout) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION();

  [[nodiscard]] bool WaitWithDeadline(absl::Time deadline) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) ABSL_UNLOCK_FUNCTION();

  void StartPendingOperation() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_)
      ABSL_UNLOCK_FUNCTION(mu_);

  void StartBlockingPendingOperation();

  void FinishPendingOperation() ABSL_LOCKS_EXCLUDED(mu_)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu_);

  void FinishBlockingPendingOperation();

 private:
  friend class EnsureExclusiveAccess;
  friend class PreventExclusiveAccess;

  Mutex* absl_nonnull const mu_;
  CondVar* absl_nonnull const cv_;
  int pending_operations_ = 0;
};

/** @private */
class ABSL_SCOPED_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED EnsureExclusiveAccess {
 public:
  explicit EnsureExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(guard);

  explicit EnsureExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard,
                                 absl::Time deadline)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(guard);

  // This class is not copyable or movable.
  EnsureExclusiveAccess(const EnsureExclusiveAccess&) = delete;
  EnsureExclusiveAccess& operator=(const EnsureExclusiveAccess&) = delete;

  ~EnsureExclusiveAccess() ABSL_UNLOCK_FUNCTION();

  static EnsureExclusiveAccess WithDeadline(
      ExclusiveAccessGuard* absl_nonnull guard,
      absl::Time deadline) ABSL_NO_THREAD_SAFETY_ANALYSIS;

  static EnsureExclusiveAccess WithTimeout(
      ExclusiveAccessGuard* absl_nonnull guard,
      absl::Duration timeout) ABSL_NO_THREAD_SAFETY_ANALYSIS;

  [[nodiscard]] bool TimedOut() const;

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
  bool timed_out_{false};
};

/** @private */
class ABSL_SCOPED_LOCKABLE PreventExclusiveAccess {
 public:
  explicit PreventExclusiveAccess(ExclusiveAccessGuard* absl_nonnull guard,
                                  bool retain_lock = false)
      ABSL_SHARED_LOCK_FUNCTION(guard);

  // This class is not copyable or movable.
  PreventExclusiveAccess(const PreventExclusiveAccess&) = delete;
  PreventExclusiveAccess& operator=(const PreventExclusiveAccess&) = delete;

  ~PreventExclusiveAccess() ABSL_UNLOCK_FUNCTION();

 private:
  ExclusiveAccessGuard* absl_nonnull const guard_;
  bool retain_lock_;
};
}  // namespace eglt::concurrency

namespace eglt {

using concurrency::CondVar;
using concurrency::Mutex;
using concurrency::MutexLock;

/** @brief
 *    Sleeps for the given duration, allowing other fibers to proceed on the
 *    rest of the thread's quantum, and other threads to run.
 *
 * @param duration
 *   The duration to sleep for.
 */
inline void SleepFor(absl::Duration duration) {
  concurrency::SleepFor(duration);
}

}  // namespace eglt

#endif  // EGLT_CONCURRENCY_CONCURRENCY_H_
