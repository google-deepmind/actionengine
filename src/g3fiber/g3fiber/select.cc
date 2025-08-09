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

#include "g3fiber/select.h"

#include <atomic>
#include <cstdint>

#include <absl/log/check.h>

#include "g3fiber/boost_primitives.h"
#include "g3fiber/util.h"

namespace thread {

internal::CaseStateArray MakeShuffledCaseStateArray(size_t num_cases) {
  internal::CaseStateArray case_states(num_cases);

  // Use inside-out Fisher-Yates shuffle to combine initialization and
  // permutation.
  if (num_cases > 0) {
    case_states[0].index = 0;
  }
  for (int i = 1; i < num_cases; i++) {
    const uint32_t swap = Rand32() % (i + 1);
    case_states[i].index = case_states[swap].index;
    case_states[swap].index = i;
  }

  return case_states;
}

int SelectUntil(absl::Time deadline, const CaseArray& cases) {
  internal::Selector selector;
  selector.picked_case_index = internal::Selector::kNonePicked;

  internal::CaseStateArray case_states =
      MakeShuffledCaseStateArray(cases.size());

  const bool need_to_block = deadline != absl::InfinitePast();
  bool ready = false;
  int registered_case_states = 0;
  for (int case_state_idx = 0; case_state_idx < cases.size();
       ++case_state_idx) {
    internal::CaseInSelectClause* case_state = &case_states[case_state_idx];

    case_state->case_ptr = &cases[case_state->index];
    case_state->prev = nullptr;  // Not on any list so far.
    case_state->selector = &selector;

    ++registered_case_states;

    if (case_state->Handle(/*enqueue=*/need_to_block)) {
      ready = true;
      break;
    }
  }

  // Either the deadline is in the past, or at least one case was ready
  // (therefore there is one picked)
  if (!need_to_block) {
    // No need to Unregister() any cases since we passed enqueue=false
    // to each Handle() above.
    return ready ? selector.picked_case_index : -1;
  }

  bool expired = false;
  if (!ready) {
    const bool expirable = deadline != absl::InfiniteFuture();

    act::concurrency::impl::MutexLock lock(&selector.mu);
    expired = !selector.WaitForPickUntil(deadline);
    DCHECK(expirable || !expired);
    if (expired) {
      // Deadline expiry. Ensure nothing is picked from this point.
      selector.picked_case_index = static_cast<int>(cases.size());
    }
  }

  // Unregister from all events with which we are registered. We know that
  // all registered case states were Handle()d with enqueue=true.
  for (int i = 0; i < registered_case_states; i++) {
    if (internal::CaseInSelectClause* case_state = &case_states[i];
        case_state->index != selector.picked_case_index) {
      // sel.picked was unregistered by the notifier.
      case_state->GetCase()->selectable->Unregister(case_state);
    }
  }

  return expired ? -1 : selector.picked_case_index;
}

}  // namespace thread
