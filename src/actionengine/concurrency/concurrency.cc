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

#include "actionengine/concurrency/concurrency.h"

#include <absl/base/optimization.h>
#include <absl/log/check.h>
#include <absl/time/clock.h>

namespace act::concurrency {

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

}  // namespace act::concurrency