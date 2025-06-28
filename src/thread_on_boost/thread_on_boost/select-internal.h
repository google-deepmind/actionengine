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

#ifndef THREAD_FIBER_SELECT_INTERNAL_H_
#define THREAD_FIBER_SELECT_INTERNAL_H_

#include <concepts>
#include <cstdint>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/boost_primitives.h"

namespace thread {
namespace internal {

template <typename T>
concept IsPointer = std::is_pointer_v<T>;

template <typename T>
concept IsNonConstPointer =
    IsPointer<T> && !std::is_const_v<std::remove_pointer_t<T>>;

template <typename T>
concept IsConstPointer =
    IsPointer<T> && std::is_const_v<std::remove_pointer_t<T>>;

struct Selector {
  static constexpr int kNonePicked = -1;

  bool Pick(int index) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu) {
    if (picked != kNonePicked) {
      return false;  // Already picked.
    }

    picked = index;
    cv.Signal();
    return true;
  }

  // kNonePicked until a case is picked, or the index of picked case
  int picked{kNonePicked};

  eglt::concurrency::impl::Mutex mu;
  eglt::concurrency::impl::CondVar cv ABSL_GUARDED_BY(mu);
};

class Selectable;
}  // namespace internal

struct [[nodiscard]] Case {
  internal::Selectable* selectable;
  absl::InlinedVector<void*, 2> arguments;

  // ReSharper disable once CppNonExplicitConvertingConstructor
  Case(internal::Selectable* s = nullptr) : selectable(s) {}
  // ReSharper disable once CppNonExplicitConvertingConstructor
  Case(internal::Selectable* s, internal::IsPointer auto... args)
      : selectable(s) {
    AddArgs(args...);
  }

  Case(const Case&) = default;
  Case& operator=(const Case&) = default;

  // // Disallow casting from bool
  // // ReSharper disable once CppNonExplicitConvertingConstructor
  // template <typename... Args>
  // Case(bool, Args... args) = delete;

  void AddArgs(internal::IsNonConstPointer auto arg) {
    arguments.push_back(static_cast<void*>(arg));
  }

  void AddArgs(internal::IsConstPointer auto arg) {
    arguments.push_back(const_cast<void*>(static_cast<const void*>(arg)));
  }

  void AddArgs(internal::IsPointer auto... args) { (AddArgs(args), ...); }

  [[nodiscard]] void* absl_nonnull GetArgPtrOrDie(int index) const {
    if (index < 0 || index >= arguments.size()) {
      LOG(FATAL) << "Case::GetArgOrDie: index out of bounds: " << index
                 << ", arguments.size() = " << arguments.size();
      ABSL_ASSUME(false);
    }
    return arguments[index];
  }

  template <typename T>
  [[nodiscard]] T* absl_nonnull GetArgPtrOrDie(int index) const {
    return static_cast<T*>(GetArgPtrOrDie(index));
  }

  [[nodiscard]] size_t GetNumArgs() const { return arguments.size(); }
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
  const Case* params;  // Initialized by Select()
  int index;           // Provided by Select(): index in parameter list.
  internal::Selector* absl_nonnull
      sel;          // Provided by Select(): owning selector.
  CaseState* prev;  // Initialized by Select(), nullptr -> not on list.
  CaseState* next;

  // Attempt to cause the owning Selector to choose this case. Returns true if
  // and only if this case will be the one chosen, because no other case has
  // already become ready and been chosen.
  //
  // After the caller releases sel->mu, this object is no longer guaranteed to
  // continue to exist.
  bool Pick() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(sel->mu) {
    return sel->Pick(index);
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
    // Add just before the oldest element (*head).
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
