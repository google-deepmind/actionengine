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

#include "thread/boost_primitives.h"

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/system/system_error.hpp>

#include "thread/fiber.h"

namespace act::concurrency::impl {
void Mutex::Lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() {
  try {
    mu_.lock();
  } catch (boost::fibers::lock_error& error) {
    LOG(FATAL) << "Mutex lock failed. " << error.what();
    ABSL_ASSUME(false);
  }
}

void Mutex::Unlock() noexcept ABSL_UNLOCK_FUNCTION() {
  try {
    mu_.unlock();
  } catch (boost::fibers::lock_error& error) {
    LOG(FATAL) << "Mutex unlock failed. " << error.what();
    ABSL_ASSUME(false);
  }
}

boost::fibers::mutex& Mutex::GetImpl() {
  return mu_;
}

void CondVar::Wait(Mutex* mu) noexcept {
  thread::FiberProperties* props = thread::GetCurrentFiberProperties();

  if (ABSL_PREDICT_TRUE(props != nullptr)) {
    MutexLock lock(&props->fiber_->mu_);
    props->waiting_on_ = this;
  }

  std::string error_message;
  try {
    cv_.wait(mu->GetImpl());
  } catch (boost::fibers::lock_error& error) {
    error_message = error.what();
  }

  if (ABSL_PREDICT_TRUE(props != nullptr)) {
    MutexLock lock(&props->fiber_->mu_);
    CHECK(props->waiting_on_ == this || props->waiting_on_ == nullptr)
        << "CondVar::Wait() called on a different CondVar than the one we "
           "waited on. This is a bug in the implementation.";
    props->waiting_on_ = nullptr;
  }

  if (ABSL_PREDICT_FALSE(!error_message.empty())) {
    // If we caught an error, we should not continue.
    LOG(FATAL) << "Error in underlying implementation: " << error_message;
    ABSL_ASSUME(false);
  }
}

bool CondVar::WaitWithDeadline(Mutex* mu, const absl::Time& deadline) noexcept {
  if (ABSL_PREDICT_TRUE(deadline == absl::InfiniteFuture())) {
    Wait(mu);
    return false;
  }

  thread::FiberProperties* props = thread::GetCurrentFiberProperties();

  if (ABSL_PREDICT_TRUE(props != nullptr)) {
    MutexLock lock(&props->fiber_->mu_);
    props->waiting_on_ = this;
  }

  bool timed_out = false;
  std::string error_message;
  try {
    timed_out =
        cv_.wait_for(mu->GetImpl(),
                     absl::ToChronoNanoseconds(deadline - absl::Now())) ==
        boost::fibers::cv_status::timeout;
  } catch (boost::fibers::lock_error& error) {
    error_message = error.what();
  }

  if (ABSL_PREDICT_TRUE(props != nullptr)) {
    MutexLock lock(&props->fiber_->mu_);
    CHECK(props->waiting_on_ == this || props->waiting_on_ == nullptr)
        << "CondVar::WaitWithDeadline() called on a different CondVar than the "
           "one we waited on. This is a bug in the implementation.";
    props->waiting_on_ = nullptr;
  }

  if (ABSL_PREDICT_FALSE(!error_message.empty())) {
    // If we caught an error, we should not continue.
    LOG(FATAL) << "Error in underlying implementation: " << error_message;
    ABSL_ASSUME(false);
  }

  return timed_out;
}
}  // namespace act::concurrency::impl