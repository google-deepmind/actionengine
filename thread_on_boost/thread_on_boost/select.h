// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
// Author: pjt@google.com (Paul Turner)

#ifndef THREAD_FIBER_SELECT_H_
#define THREAD_FIBER_SELECT_H_

#include "thread_on_boost/absl_headers.h"

#include "thread_on_boost/select-internal.h"

namespace thread {

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
// The provided name is then available to tracing implementations based on
// the TraceEventListener API such as Dapper PE. The default value for `name`
// is the source location of the call site.
//
// THREAD SAFETY:
// Select and Cases are typically thread-safe. More detailed notes may be found
// below.
int Select(const CaseArray& cases);

// SelectUntil waits at most until the absolute time value specified by the
// deadline parameter.  If a case completes before then, its index is returned.
// Otherwise a special index of -1 is returned, representing expiration of the
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
  absl::Time deadline = absl::InfinitePast();
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
