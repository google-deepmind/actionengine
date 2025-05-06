// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
//
// IWYU pragma: private, include "thread/fiber/select.h"
// IWYU pragma: friend thread/fiber/select*

#ifndef THREAD_FIBER_SELECT_INTERNAL_H_
#define THREAD_FIBER_SELECT_INTERNAL_H_

#include <cstdint>

#include "thread_on_boost/absl_headers.h"

#include "thread_on_boost/boost_primitives.h"

namespace thread {

// The state kept by Select and modified by the selectables tracking which
// selectable winds up being selected. Declared upfront so that lock-safety
// annotations may be added.
namespace internal {
struct Selector {
  Mutex mu;
  int picked;  // kNonePicked until a case is picked, or index of picked case
  CondVar cv;  // Select() call waits on this until picked >= 0

  // The implementation reserves picked values <0 for internal use.
  // External callers (e.g. Selectable::Handle)) may set (non-negative) picked
  // iff
  //  i. mu is held
  //  ii. picked == Selector::kNonePicked
  static constexpr int kNonePicked = -1;
};

class Selectable;
}  // namespace internal

// A token given to the user to be passed to Select, containing the selectable
// and untyped arguments for it specific to this case. Not in the internal
// namespace since callers can pass around and copy objects of this type, though
// they are not allowed to assume anything about its internals.
struct ABSL_MUST_USE_RESULT Case {
  internal::Selectable* event;
  intptr_t arg1;  // Optional
  intptr_t arg2;  // Optional

  // Provide constructors to initialize Case to disallow undesired implicit
  // conversions (e.g., bool used to convert to a Case silently).
  Case(internal::Selectable* e = nullptr, intptr_t a = 0, intptr_t b = 0)
      : event(e), arg1(a), arg2(b) {}
  Case(const Case&) = default;
  Case& operator=(const Case&) = default;

  // Disallow casting from bool
  Case(bool, intptr_t = 0, intptr_t = 0) = delete;
};

// An array of cases; the type supplied to Select. Must be initializer list
// compatible.
typedef absl::InlinedVector<Case, 4> CaseArray;

namespace internal {

// A CaseState is the encapsulation of a Case that is passed back to
// Selectable::Handle(...) from Select (specifically, the selectable identified
// by params->event).
//
// A CaseState represents the per-Select call information kept for a Case.
// This separation from Case allows a particular Case to be safely passed to
// multiple Select calls concurrently.
//
// CaseStates contain an intrusive, circular, doubly-linked list, to be used by
// selectables for enqueuing cases that are waiting on some condition. See the
// notes on Selectable::Handle below. Once enqueued, the Selectable is
// responsible for synchronizing any modifications.
struct CaseState {
  const Case* params;       // Initialized by Select()
  int index;                // Provided by Select(): index in parameter list.
  internal::Selector* sel;  // Provided by Select(): owning selector.
  CaseState* prev;          // Initialized by Select(), nullptr -> not on list.
  CaseState* next;

  // Attempt to cause the owning Selector to choose this case. Returns true if
  // and only if this case will be the one chosen, because no other case has
  // already become ready and been chosen.
  //
  // After the caller releases sel->mu, this object is no longer guaranteed to
  // continue to exist.
  bool Pick() ABSL_EXCLUSIVE_LOCKS_REQUIRED(sel->mu) {
    DCHECK(sel != nullptr);
    if (sel->picked != internal::Selector::kNonePicked) {
      return false;
    }

    sel->picked = index;
    sel->cv.Signal();
    return true;
  }
};

// The interface implemented by objects that can be used with Select().  Note
// that a single Selectable may be enqueued against multiple Select statements,
// this indirection is represented using Case tokens above. Case tokens allow
// for passing arguments to a single selectable used in multiple different ways.
class Selectable {
 public:
  virtual ~Selectable() = default;

  // If this selectable is ready to be picked up by c's Select, call c->Pick()
  // (which may or may not pick this selectable), and return true.
  // If not ready to be picked: enqueue the case (if enqueue is true) and return
  // false.
  //
  // The selectable should implement the following algorithm:
  //   if (currently ready) {
  //     c->sel->mu.Lock();
  //     if (c->Pick()) {
  //       ... perform any side effects of being picked ...
  //     }
  //     c->sel->mu.Unlock();
  //     return true;
  //   } else {
  //     if (enqueue) {
  //       ... enqueue the CaseState ...
  //     }
  //     return false;
  //   }
  //
  // If the CaseState is enqueued and the selectable later becomes ready before
  // Unregister is called, it should again lock c->sel->mu, call c->Pick(),
  // and perform side effects iff picked, while c->sel->mu is still held.
  virtual bool Handle(CaseState* c, bool enqueue) = 0;

  // Unregister a case against future transitions for this Selectable.
  //
  // Called for all cases c1 where a previous call Handle(c1, true) returned
  // false and another case c2 was successfully selected (c2->Pick() returned
  // true), or a timeout occurred.
  virtual void Unregister(CaseState* c) = 0;
};

// Shared linked list code. Implements a doubly-linked list where the list head
// is a pointer to the oldest element added to the list.
inline void PushBack(CaseState** head, CaseState* elem) {
  if (*head == nullptr) {
    // Queue is empty; make singleton queue.
    elem->next = elem;
    elem->prev = elem;
    *head = elem;
  } else {
    // Add just before oldest element (*head).
    elem->next = *head;
    elem->prev = elem->next->prev;
    elem->prev->next = elem;
    elem->next->prev = elem;
  }
}

// Remove the supplied element from the list, updating the head pointer if
// necessary. Guarantees that afterward elem->prev will be nullptr, and
// elem->next will be unchanged.
inline void RemoveFromList(CaseState** head, CaseState* elem) {
  if (elem->next == elem) {
    // Single entry; clear list
    *head = nullptr;
  } else {
    // Remove from list
    elem->next->prev = elem->prev;
    elem->prev->next = elem->next;
    if (*head == elem) {
      *head = elem->next;
    }
  }
  // Maintaining this state in "prev" allows the safe removal of the current
  // element while iterating forwards.
  elem->prev = nullptr;
}

}  // namespace internal
}  // namespace thread

#endif  // THREAD_FIBER_SELECT_INTERNAL_H_
