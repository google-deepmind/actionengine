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

#ifndef ACTIONENGINE_DISTRIBUTED_SINGLEFLIGHT_H_
#define ACTIONENGINE_DISTRIBUTED_SINGLEFLIGHT_H_

#include <any>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/status/statusor.h>

#include "actionengine/concurrency/concurrency.h"

namespace act::distributed {

class WaitGroup {
 public:
  WaitGroup() = default;

  WaitGroup(const WaitGroup&) = delete;
  WaitGroup& operator=(const WaitGroup&) = delete;

  WaitGroup(WaitGroup&& other) noexcept;
  WaitGroup& operator=(WaitGroup&& other) noexcept;

  ~WaitGroup();

  void Add(int delta);

  void Done();

  void Wait();

 private:
  mutable act::Mutex mu_;
  act::CondVar cv_ ABSL_GUARDED_BY(mu_);
  int counter_ ABSL_GUARDED_BY(mu_) = 0;
};

struct Call {
  absl::StatusOr<std::any> val;
  WaitGroup wg;
};

class FlightGroup {
 public:
  ~FlightGroup() { act::MutexLock lock(&mu_); }

  absl::StatusOr<std::any> Do(
      std::string_view key, absl::AnyInvocable<absl::StatusOr<std::any>()> fn);

  template <typename T>
  absl::StatusOr<T> Do(std::string_view key,
                       absl::AnyInvocable<absl::StatusOr<T>()> fn);

 private:
  mutable act::Mutex mu_;
  absl::flat_hash_map<std::string, std::shared_ptr<Call>> calls_
      ABSL_GUARDED_BY(mu_);
};

template <typename T>
absl::StatusOr<T> FlightGroup::Do(std::string_view key,
                                  absl::AnyInvocable<absl::StatusOr<T>()> fn) {
  absl::StatusOr<std::any> result =
      Do(key, [fn = std::move(fn)]() mutable -> absl::StatusOr<std::any> {
        absl::StatusOr<T> val = std::move(fn)();
        if (!val.ok()) {
          return val.status();
        }
        LOG(INFO) << "SingleFlight: computed value: " << val.value();
        return std::any(std::move(val.value()));
      });
  if (!result.ok()) {
    return result.status();
  }
  if (std::any_cast<T>(&result.value()) == nullptr) {
    return absl::InternalError("Type assertion failed in SingleFlight::Do");
  }
  return std::any_cast<T>(std::move(result.value()));
}

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_SINGLEFLIGHT_H_