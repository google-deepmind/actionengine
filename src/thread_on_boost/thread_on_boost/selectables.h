// Copyright 2012 Google Inc. All Rights Reserved.
// Author: pjt@google.com (Paul Turner)
//
// Provides generically useful Selectables for use with Select.

#ifndef THREAD_FIBER_SELECTABLES_H_
#define THREAD_FIBER_SELECTABLES_H_

#include <atomic>

#include "thread_on_boost/absl_headers.h"

#include "thread_on_boost/select-internal.h"

namespace thread {

// PermanentEvent
// ---------------
// Provides a level-triggered event which may be added to a Select statement.
// PermanentEvents may only transition into a notified state.  Selecting
// against OnEvent() for an event that has already been signalled will always
// return immediately.
//
// Memory ordering: For any threads X and Y, if X calls `Notify()`, then any
// action taken by X before it calls `Notify()` is visible to thread Y after:
//  * Y selects OnEvent(), or
//  * Y receives a `true` return value from `HasBeenNotified()`
//
// Example usage:
//     void ProduceValues(thread::Channel<int>* queue,
//                        thread::PermanentEvent* quit) {
//       int i = 0;
//       while (true) {
//         switch (thread::Select({ queue->writer()->OnWrite(i),
//                                  quit->OnEvent() })) {
//           case 0: {
//             ++i;  // Prepare next write
//             break;
//           }
//           case 1: {
//             ...  // quit was signalled, stop producing
//             return;
//           }
//         }
//       }
//     }
class PermanentEvent final : public internal::Selectable {
 public:
  PermanentEvent() = default;

  ~PermanentEvent() override {
    // We acquire the lock here so that PermanentEvent can synchronize
    // its own deletion.
    lock_.lock();
    DCHECK(enqueued_list_ == nullptr);
  }

  PermanentEvent(const PermanentEvent&) = delete;
  PermanentEvent& operator=(const PermanentEvent&) = delete;

  // Signal that the event has occurred.  Any Selectors on this event will be
  // immediately notified, future Select statements against this event will be
  // non-blocking.  May only be called once.
  void Notify();

  // Returns true if Notify() has been called.  False otherwise.
  bool HasBeenNotified() const;

  // May be passed to Select.  Will always evaluate immediately for an event
  // that has already been notified.  Once the case has been signalled, then
  // deleting the PermanentEvent will not interfere with the caller of Notify().
  Case OnEvent() const {
    Case c = {const_cast<PermanentEvent*>(this)};
    return c;
  }

  // Implementation of Selectable interface.
  bool Handle(internal::CaseState* c, bool enqueue) override;
  void Unregister(internal::CaseState* c) override;

 private:
  friend class Fiber;

  boost::fibers::detail::spinlock lock_;
  std::atomic<bool> notified_{false};

  internal::CaseState* enqueued_list_ = nullptr;
};

// NonSelectableCase()
// -------------------
// Provides a 'null' case which will never evaluate as ready by Select.  This
// may be used to substitute a Selectable that is no longer of interest within a
// set, without re-labeling adjacent elements.
//
// Example:
//   int item;
//   bool ok;
//   thread::CaseArray cases = { chan1.reader()->OnRead(&item, &ok),
//                               chan2.reader()->OnRead(&item, &ok) };
//
//   while (1) {
//     int index = thread::Select(cases);
//     if (!ok) {
//       // Channel has been closed
//       cases[index] = NonSelectableCase();
//     } else {
//       ...  process item
//     }
Case NonSelectableCase();

// AlwaysSelectableCase()
// ----------------------
// Provides case which will always evaluate as ready by Select.  This may be
// used when returning a Case for an event that is already known to be ready.
//
// Example:
//   class WorkItem {
//    public:
//     // Start the work and return a case that will become ready when
//     // the work has completed.
//     thread::Case Start() {
//       if (!status_.ok()) {
//         // An error has already been detected, so the work has completed.
//         return thread::AlwaysSelectableCase();
//       } else {
//         ... start real work ...
//       }
//     }
//    private:
//     util::Status status_;
//   };
Case AlwaysSelectableCase();

}  // namespace thread

#endif  // THREAD_FIBER_SELECTABLES_H_
