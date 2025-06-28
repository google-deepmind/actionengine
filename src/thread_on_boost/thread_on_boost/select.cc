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

#include "thread_on_boost/select.h"

#include <atomic>
#include <cstdint>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/util.h"

namespace thread {

inline bool CvBlock(absl::Time deadline, internal::Selector* sel)
    ABSL_SHARED_LOCKS_REQUIRED(sel->mu) {
  while (sel->picked == internal::Selector::kNonePicked) {
    if (sel->cv.WaitWithDeadline(&sel->mu, deadline) &&
        sel->picked == internal::Selector::kNonePicked) {
      return false;
    }
  }
  return true;
}

int SelectUntil(absl::Time deadline, const CaseArray& cases) {
  internal::Selector sel;
  sel.picked = internal::Selector::kNonePicked;
  const int num_cases = cases.size();

  // Initialize internal representation of passed Cases
  absl::FixedArray<internal::CaseState, 4> case_states(num_cases);

  // Use inside-out Fisher-Yates shuffle to combine initialization and
  // permutation.
  if (num_cases > 0) {
    case_states[0].index = 0;
  }
  for (int i = 1; i < num_cases; i++) {
    const int swap = Rand32() % (i + 1);
    case_states[i].index = case_states[swap].index;
    case_states[swap].index = i;
  }

  const bool need_to_block = deadline != absl::InfinitePast();
  bool ready = false;
  int registered_limit;
  for (registered_limit = 0; registered_limit < num_cases; ++registered_limit) {
    internal::CaseState* case_state = &case_states[registered_limit];
    const Case* assoc_case = &cases[case_state->index];
    case_state->params = assoc_case;
    case_state->prev = nullptr;  // Not on any list.
    case_state->sel = &sel;
    if (assoc_case->selectable->Handle(case_state, /*enqueue=*/need_to_block)) {
      ready = true;
      break;
    }
  }

  if (!need_to_block) {
    // Do not wait. Also, no need to Unregister() any cases since
    // we passed enqueue=false to each Handle() above.
    const int picked = ready ? sel.picked : -1;
    return picked;
  }

  if (!ready) {
    const bool expirable = deadline != absl::InfiniteFuture();

    eglt::concurrency::impl::MutexLock l(&sel.mu);
    const bool expired = !CvBlock(deadline, &sel);
    DCHECK(expirable || !expired);
    if (expired) {
      // Deadline expiry. Ensure nothing is picked from this point.
      sel.picked = num_cases;
    }
  }

  // Unregister from all events with which we are registered.  We know that
  // there was no non-blocking index and that we attempted to enqueue against
  // all cases with index smaller than registered_limit.
  for (int i = 0; i < registered_limit; i++) {
    if (internal::CaseState* case_state = &case_states[i];
        case_state->index != sel.picked) {
      // sel.picked was unregistered by the notifier.
      case_state->params->selectable->Unregister(case_state);
    }
  }

  // sel.picked == num_cases denotes expiry
  const int picked = sel.picked < num_cases ? sel.picked : -1;
  return picked;
}

}  // namespace thread
