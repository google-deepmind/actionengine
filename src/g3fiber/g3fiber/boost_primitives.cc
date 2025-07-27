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

#include "g3fiber/boost_primitives.h"

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>

namespace eglt::concurrency::impl {

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

}  // namespace eglt::concurrency::impl