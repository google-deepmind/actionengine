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

#ifndef ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_
#define ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_

#include <absl/base/attributes.h>
#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/time/time.h>

#include "thread/concurrency.h"

// IWYU pragma: begin_exports
#if GOOGLE3
#include "actionengine/google3_concurrency_headers.h"
#else
#include "thread/concurrency.h"
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
 *  @headerfile actionengine/concurrency/concurrency.h
 */

/** @private */
namespace act::concurrency {

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
}  // namespace act::concurrency

namespace act {

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

}  // namespace act

#endif  // ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_
