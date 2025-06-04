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

#ifndef EGLT_SDK_BOOST_ASIO_UTILS_H_
#define EGLT_SDK_BOOST_ASIO_UTILS_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <thread>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>

#include "eglt/concurrency/concurrency.h"

namespace eglt::sdk {

boost::asio::thread_pool* GetDefaultAsioExecutionContext();

// If fn() is void, result holder cannot be created
template <typename Invocable, typename ExecutionContext,
          typename = std::enable_if_t<
              !std::is_void_v<std::invoke_result_t<Invocable>>>>
auto RunInAsioContext(ExecutionContext& context, Invocable&& fn,
                      concurrency::impl::CaseArray additional_cases = {}) {
  std::optional<decltype(fn())> result;
  concurrency::PermanentEvent done;
  boost::asio::post(context,
                    [&done, &result, fn = std::forward<Invocable>(fn)]() {
                      result = fn();
                      done.Notify();
                    });

  auto cases = std::move(additional_cases);
  cases.push_back(done.OnEvent());
  concurrency::Select(cases);

  return *result;
}

// If fn() is void, result holder cannot be created
template <typename Invocable, typename = std::enable_if_t<!std::is_void_v<
                                  std::invoke_result_t<Invocable>>>>
auto RunInAsioContext(Invocable&& fn,
                      concurrency::impl::CaseArray additional_cases = {}) {
  return RunInAsioContext(*GetDefaultAsioExecutionContext(),
                          std::forward<Invocable>(fn),
                          std::move(additional_cases));
}

template <typename ExecutionContext>
void RunInAsioContext(ExecutionContext& context,
                      absl::AnyInvocable<void()>&& fn,
                      concurrency::impl::CaseArray additional_cases = {}) {
  concurrency::PermanentEvent done;
  boost::asio::post(context, [&done, &fn]() {
    fn();
    done.Notify();
  });

  auto cases = std::move(additional_cases);
  cases.push_back(done.OnEvent());
  concurrency::Select(cases);
}

void RunInAsioContext(absl::AnyInvocable<void()>&& fn,
                      concurrency::impl::CaseArray additional_cases = {});

}  // namespace eglt::sdk

#endif  // EGLT_SDK_BOOST_ASIO_UTILS_H_