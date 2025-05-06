// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
// Author: pjt@google.com (Paul Turner)

#ifndef THREAD_FIBER_SELECT_H_
#define THREAD_FIBER_SELECT_H_

#include "thread_on_boost/absl_headers.h"

#include "thread_on_boost/select-internal.h"

namespace thread {

inline static std::atomic<int32_t> last_rand32;
inline static absl::once_flag init_rand32_once;

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
  static constexpr uint32_t poly = (1 << 22) | (1 << 2) | (1 << 1) | (1 << 0);

  absl::call_once(init_rand32_once, InitRand32);
  uint32_t r = last_rand32.load(std::memory_order_relaxed);
  r = (r << 1) ^
      ((static_cast<int32_t>(r) >> 31) & poly);  // shift sign-extends
  last_rand32.store(r, std::memory_order_relaxed);
  return r;
}

// Select waits until an event corresponding to one of the specified Cases
// becomes ready, processes that event, and returns its index within the
// container of arguments passed to Select.
//
// Cases have synchronous semantics and should be considered as blocking, only
// returning when the condition they specify is ready. If multiple cases are
// simultaneously ready to complete, exactly one is chosen arbitrarily and only
// it is allowed to proceed. Unselected Cases are always free of side-effects.
//
// Lists of Cases may be pre-declared using the thread::CaseArray type, which is
// guaranteed to be compatible with an initializer list of cases.
//
// Example:
//  thread::CaseArray cases = {case1, case2, ...};
//  thread::Select(cases);
//
// The wait can be assigned a logical name, e.g.:
//   thread::Select(cases, "FirstResponse");
//
// THREAD SAFETY:
// Select and Cases are typically thread-safe. More detailed notes may be found
// below.
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

// TrySelect is a non-blocking version of Select. It returns immediately, with
// an index if any of the cases are ready or -1 otherwise.
//
// Example:
//  bool TryRead(thread::Channel<int>* c, int* val) {
//    bool ok = false;
//    if (thread::TrySelect({c->reader()->OnRead(val, &ok)}) == 0) {
//      return ok;  // May still be false if the channel was closed.
//    } else {
//      return false;
//    }
//  }
//
// It is guaranteed that no case will proceed if -1 is returned.
inline int TrySelect(const CaseArray& cases) {
  // Break into a variable to prevent inlining with the below overload.
  constexpr absl::Time deadline = absl::InfinitePast();
  return SelectUntil(deadline, cases);
}

#if ABSL_HAVE_ATTRIBUTE(enable_if)
ABSL_DEPRECATE_AND_INLINE()
inline int SelectUntil(absl::Time deadline, const CaseArray& cases)
    __attribute__((enable_if(deadline == absl::InfinitePast(),
                             "Use TrySelect instead."))) {
  return TrySelect(cases);
}
#endif  // ABSL_HAVE_ATTRIBUTE(enable_if)

inline int Select(const CaseArray& cases) {
  CHECK_GT(cases.size(), 0U) << "No cases provided";
  return SelectUntil(absl::InfiniteFuture(), cases);
}

}  // namespace thread

#endif  // THREAD_FIBER_SELECT_H_
