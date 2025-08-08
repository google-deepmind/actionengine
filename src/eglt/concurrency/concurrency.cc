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

#include "eglt/concurrency/concurrency.h"

namespace eglt::concurrency {

/** @brief
 *    Sleeps for the given duration, allowing other fibers to proceed on the
 *    rest of the thread's quantum, and other threads to run.
 *
 * @param duration
 *   The duration to sleep for.
 */
void SleepFor(absl::Duration duration) {
  impl::SleepFor(duration);
}

TwoMutexLock::TwoMutexLock(Mutex* mu1, Mutex* mu2) : mu1_(mu1), mu2_(mu2) {
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

TwoMutexLock::~TwoMutexLock() {
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

ExclusiveAccessGuard::ExclusiveAccessGuard(Mutex* mutex, CondVar* cv)
    : mu_(mutex), cv_(cv) {}

ExclusiveAccessGuard::~ExclusiveAccessGuard() {
  CHECK(pending_operations_ == 0) << "FinalizationGuard destroyed with "
                                     "pending operations";
}

void ExclusiveAccessGuard::Wait() const {
  while (pending_operations_ > 0) {
    cv_->Wait(mu_);
  }
}

bool ExclusiveAccessGuard::WaitWithTimeout(absl::Duration timeout) const {
  bool timed_out = false;
  while (pending_operations_ > 0) {
    if (timed_out = cv_->WaitWithTimeout(mu_, timeout); timed_out) {
      break;
    }
  }
  return timed_out;
}

bool ExclusiveAccessGuard::WaitWithDeadline(absl::Time deadline) const {
  bool timed_out = false;
  while (pending_operations_ > 0) {
    if (timed_out = cv_->WaitWithDeadline(mu_, deadline); timed_out) {
      break;
    }
  }
  return timed_out;
}

void ExclusiveAccessGuard::StartPendingOperation() {
  ++pending_operations_;
  mu_->Unlock();
}

void ExclusiveAccessGuard::StartBlockingPendingOperation() {
  ++pending_operations_;
}

void ExclusiveAccessGuard::FinishPendingOperation() {
  mu_->Lock();
  --pending_operations_;
  cv_->SignalAll();
}

void ExclusiveAccessGuard::FinishBlockingPendingOperation() {
  --pending_operations_;
  cv_->SignalAll();
}

EnsureExclusiveAccess::EnsureExclusiveAccess(ExclusiveAccessGuard* guard)
    : EnsureExclusiveAccess(guard, absl::InfiniteFuture()) {}

EnsureExclusiveAccess::EnsureExclusiveAccess(ExclusiveAccessGuard* guard,
                                             absl::Time deadline)
    : guard_(guard) {
  timed_out_ = guard_->WaitWithDeadline(deadline);
}

EnsureExclusiveAccess::~EnsureExclusiveAccess() {
  guard_->cv_->SignalAll();
}

EnsureExclusiveAccess EnsureExclusiveAccess::WithDeadline(
    ExclusiveAccessGuard* guard, absl::Time deadline) {
  return EnsureExclusiveAccess(guard, deadline);
}

EnsureExclusiveAccess EnsureExclusiveAccess::WithTimeout(
    ExclusiveAccessGuard* guard, absl::Duration timeout) {
  return EnsureExclusiveAccess(guard, absl::Now() + timeout);
}

bool EnsureExclusiveAccess::TimedOut() const {
  return timed_out_;
}

PreventExclusiveAccess::PreventExclusiveAccess(ExclusiveAccessGuard* guard,
                                               bool retain_lock)
    : guard_(guard), retain_lock_(retain_lock) {
  if (retain_lock_) {
    guard_->StartBlockingPendingOperation();
  } else {
    guard_->StartPendingOperation();
  }
}

PreventExclusiveAccess::~PreventExclusiveAccess() {
  if (retain_lock_) {
    guard_->FinishBlockingPendingOperation();
  } else {
    guard_->FinishPendingOperation();
  }
}

}  // namespace eglt::concurrency