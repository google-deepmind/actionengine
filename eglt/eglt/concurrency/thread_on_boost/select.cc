// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
// Author: pjt@google.com (Paul Turner)

#include "thread_on_boost/select.h"

#include <atomic>
#include <cstdint>

#include <eglt/absl_headers.h>

namespace thread {

// TODO(pjt): if we ever see contention on last_rand32 we'll want to make this
// per-thread or per-cpu.
static std::atomic<int32_t> last_rand32;
static absl::once_flag init_rand32_once;

static void InitRand32() {
  // GoogleOnceInit is an acquire barrier on remote-cpus.
  uint32_t seed = absl::Uniform<uint32_t>(absl::BitGen());
  // Avoid 0 which generates a sequence of 0s.
  if (seed == 0)
    seed = 1;
  last_rand32.store(seed, std::memory_order_release);
}

// Pseudo-random number generator using Linear Shift Feedback Register (LSFB)
static uint32_t Rand32() {
  // Primitive polynomial: x^32+x^22+x^2+x^1+1
  static const uint32_t poly = (1 << 22) | (1 << 2) | (1 << 1) | (1 << 0);

  absl::call_once(init_rand32_once, InitRand32);
  uint32_t r = last_rand32.load(std::memory_order_relaxed);
  r = (r << 1) ^
      ((static_cast<int32_t>(r) >> 31) & poly);  // shift sign-extends
  last_rand32.store(r, std::memory_order_relaxed);
  return r;
}

// Block until a case has been picked or the deadline passes. If deadline is
// a nullptr, block until a case has been picked. Returns whether a case
// was picked before the deadline passed.
//
// This blocking is handled using sel's internal condition variable.
// Note that we always use this path when the Select call is non-expiring.
inline bool CvBlock(absl::Time deadline, internal::Selector* sel)
    ABSL_SHARED_LOCKS_REQUIRED(sel->mu) {
  // We must first check that no notification occurred between registration
  // with Handle and reaching here.
  while (sel->picked == internal::Selector::kNonePicked) {
    // Note that implementations of WaitGeneric below are allowed to return
    // true (indicating timeout) when racing with Signal().  To handle this we
    // re-check against sel.picked before returning expiring_index.
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
  int num_cases = cases.size();

  // Initialize internal representation of passed Cases
  absl::FixedArray<internal::CaseState, 4> case_states(num_cases);

  // Use inside-out Fisher-Yates shuffle to combine initialization and
  // permutation.
  if (num_cases > 0) {
    case_states[0].index = 0;
  }
  for (int i = 1; i < num_cases; i++) {
    int swap = Rand32() % (i + 1);
    case_states[i].index = case_states[swap].index;
    case_states[swap].index = i;
  }

  bool blocking = deadline != absl::InfinitePast();
  bool ready = false;
  int registered_limit;
  for (registered_limit = 0; registered_limit < num_cases; registered_limit++) {
    internal::CaseState* case_state = &case_states[registered_limit];
    const Case* assoc_case = &cases[case_state->index];
    case_state->params = assoc_case;
    case_state->prev = nullptr;  // Not on any list.
    case_state->sel = &sel;
    if (assoc_case->event->Handle(case_state, /*enqueue=*/blocking)) {
      ready = true;
      break;
    }
  }

  if (!blocking) {
    // Do not wait.  Also, no need to Unregister() any cases since
    // we passed enqueue=false to each Handle() above.
    const int picked = ready ? sel.picked : -1;
    return picked;
  }

  if (!ready) {
    const bool expirable = deadline != absl::InfiniteFuture();

    MutexLock l(&sel.mu);
    bool expired = !CvBlock(deadline, &sel);
    // DCHECK(expirable || !expired);
    if (expired) {
      // Deadline expiry. Ensure nothing is picked from this point.
      sel.picked = num_cases;
    }
  }

  // Unregister from all events with which we are registered.  We know that
  // there was no non-blocking index and that we attempted to enqueue against
  // all cases with index smaller than registered_limit.
  for (int i = 0; i < registered_limit; i++) {
    internal::CaseState* case_state = &case_states[i];
    if (case_state->index != sel.picked) {
      // sel.picked was unregistered by the notifier.
      case_state->params->event->Unregister(case_state);
    }
  }

  // sel.picked == num_cases denotes expiry
  const int picked = sel.picked < num_cases ? sel.picked : -1;
  return picked;
}

}  // namespace thread
