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

#ifndef THREAD_FIBER_SELECT_H_
#define THREAD_FIBER_SELECT_H_

#include <absl/log/check.h>

#include "thread/cases.h"

/** @file
 *  @brief
 *    Provides the Select and SelectUntil functions for selecting a fiber to
 *    proceed if one of several cases is ready.
 *
 * This file defines the thread::Select and thread::SelectUntil functions,
 * which allow a fiber to wait for (block until) one of several cases happens
 * to proceed, either until a specified deadline or indefinitely.
 */

namespace thread {
/** @brief
 *    Returns the index of the first case that is ready, or -1 if the deadline
 *    has expired without any case becoming ready.
 *
 *  Example:
 *  @code{.cc}
 *  // Try to write against the channel "c", waiting for up to 1ms when there is
 *  // no space immediately available. Returns true if the write was
 *  // successfully enqueued, false otherwise.
 *  bool TryPut(thread::Channel<int>* c, int v) {
 *    return thread::SelectUntil(
 *        absl::Now() + absl::Milliseconds(1),
 *        {c->writer()->OnWrite(v)}) == 0;
 *    }
 *  @endcode
 *
 *  @note
 *    If a Case transitions into a ready state after the deadline has expired,
 *    but before SelectUntil returns, the return value is not guaranteed to be
 *    -1 and may instead be the index of that case. However, if -1 is returned,
 *    it is guaranteed that no case has proceeded.
 *
 * @param deadline
 *   The absolute time until which to wait for a case to become ready.
 * @param cases
 *   The array of cases to select from. Usually passed as an initializer list.
 * @return
 *   The index of the first case that is ready, or -1 if the deadline has
 *   expired.
 */
int SelectUntil(absl::Time deadline, const CaseArray& cases);

/** @brief
 *    Returns the index of the first case that is ready, blocking until one is.
 *
 *  This is equivalent to calling SelectUntil with an infinite deadline.
 *
 * @param cases
 *   The array of cases to select from. Usually passed as an initializer list.
 * @return
 *   The index of the first case that is ready, or -1 if no case is ready.
 */
inline int Select(const CaseArray& cases) {
  CHECK(!cases.empty()) << "No cases provided";
  return SelectUntil(absl::InfiniteFuture(), cases);
}
}  // namespace thread

#endif  // THREAD_FIBER_SELECT_H_