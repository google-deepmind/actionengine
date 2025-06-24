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

#include "thread_on_boost/absl_headers.h"

#include "thread_on_boost/select-internal.h"

namespace thread {

int Select(const CaseArray& cases);

// SelectUntil waits at most until the absolute time value specified by the
// deadline parameter.  If a case completes before then, its index is returned.
// Otherwise, a special index of -1 is returned, representing expiration of the
// deadline.
//
// All other semantics are equivalent to Select(...).
//
// Example:
//  // Try to write against the channel "c", waiting for up to 1ms when there is
//  // no space immediately available.  Returns true if the write was
//  // successfully enqueued, false otherwise.
//  bool TryPut(thread::Channel<int>* c, int v) {
//    return thread::SelectUntil(
//        absl::Now() + absl::Milliseconds(1),
//        {c->writer()->OnWrite(v)}) == 0;
//  }
//
// If a Case transitions into a ready state after the deadline has elapsed it is
// not specified whether -1 or that case will be returned. It is however
// guaranteed when -1 is returned that no case may have proceeded.
int SelectUntil(absl::Time deadline, const CaseArray& cases);

inline int Select(const CaseArray& cases) {
  CHECK(!cases.empty()) << "No cases provided";
  return SelectUntil(absl::InfiniteFuture(), cases);
}

}  // namespace thread

#endif  // THREAD_FIBER_SELECT_H_
