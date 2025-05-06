// Copyright 2012 Google Inc. All Rights Reserved.
// Author: pjt@google.com (Paul Turner)

#include "thread_on_boost/selectables.h"

#include <atomic>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/select.h"

namespace thread {

// PermanentEvent
bool PermanentEvent::Handle(internal::CaseState* c, bool enqueue) {
  if (cancellation_event_) {
    internal::CheckActiveCancellationColor();
  }

  SpinLockHolder l1(&lock_);

  if (notified_.load(std::memory_order_relaxed)) {  // Synchronized by lock_
    MutexLock l2(&c->sel->mu);
    // Consider that in the presence of a race with another Selectable,
    // c->Pick() may return false in this case.  This is safe as we are not
    // required to maintain an active list after notification has been
    // delivered.
    return c->Pick();
  } else if (enqueue) {
    internal::PushBack(&enqueued_list_, c);
  }

  return false;
}

void PermanentEvent::Unregister(internal::CaseState* c) {
  SpinLockHolder l1(&lock_);
  if (!notified_.load(std::memory_order_relaxed)) {
    // We only maintain lists of active cases up until notification.
    internal::RemoveFromList(&enqueued_list_, c);
  }
}

void PermanentEvent::Notify() {
  // If traced, record the causality of the event being notified (signaled).

  SpinLockHolder l(&lock_);

  DCHECK(!notified_.load(std::memory_order_relaxed))
      << "Notify() method called more than once for "
      << "PermanentEvent object " << static_cast<void*>(this);
  notified_.store(true, std::memory_order_release);

  // The transition to a notified state is a permanent one, so we tear down any
  // enqueued cases.  We must be careful to synchronize against this in the
  // future in both the Handle(..., true) and Unregister cases.
  while (enqueued_list_) {
    MutexLock l2(&enqueued_list_->sel->mu);
    enqueued_list_->Pick();
    // Continued storage of enqueued_list_ after Pick() is guaranteed by sel->mu
    internal::RemoveFromList(&enqueued_list_, enqueued_list_);
  }
}

bool PermanentEvent::HasBeenNotified() const {
  if (cancellation_event_) {
    internal::CheckActiveCancellationColor();
  }

  return HasBeenNotified(SuppressCancellationColorCheckTag{});
}

// NonSelectable: an ironic implementation of a Selectable.
class NonSelectable : public internal::Selectable {
 public:
  NonSelectable() = default;
  ~NonSelectable() override = default;

  // Implementation of Selectable interface.
  bool Handle(internal::CaseState* c, bool enqueue) override { return false; }
  void Unregister(internal::CaseState* c) override {}
};

Case NonSelectableCase() {
  // TODO(pjt): Select could be specialized against NonSelectable?
  static absl::NoDestructor<NonSelectable> non_selectable;
  return {non_selectable.get()};
}

// AlwaysSelectable: a trivial implementation of a Selectable.
class AlwaysSelectable : public internal::Selectable {
 public:
  AlwaysSelectable() = default;
  ~AlwaysSelectable() override = default;

  // Implementation of Selectable interface.
  bool Handle(internal::CaseState* c, bool enqueue) override {
    MutexLock lock(&c->sel->mu);
    // This selectable is always ready, so ask the selector to pick it.
    // Note: it may not be picked in case another selectable has already
    // been picked.
    return c->Pick();
  }
  void Unregister(internal::CaseState* c) override {}
};

Case AlwaysSelectableCase() {
  static absl::NoDestructor<AlwaysSelectable> always_selectable;
  return {always_selectable.get()};
}

void internal::CheckActiveCancellationColor() {}

}  // namespace thread
