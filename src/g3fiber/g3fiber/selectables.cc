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

#include "g3fiber/selectables.h"

#include <atomic>
#include <mutex>

#include "g3fiber/absl_headers.h"
#include "g3fiber/boost_primitives.h"
#include "g3fiber/select.h"

namespace thread {

// PermanentEvent
bool PermanentEvent::Handle(internal::CaseInSelectClause* c, bool enqueue) {
  boost::fibers::detail::spinlock_lock l1(splk_);

  if (notified_.load(std::memory_order_relaxed)) {  // Synchronized by lock_
    eglt::concurrency::impl::MutexLock l2(&c->selector->mu);
    // Consider that in the presence of a race with another Selectable,
    // c->TryPick() may return false in this case. This is safe as we are not
    // required to maintain an active list after notification has been
    // delivered.
    return c->TryPick();
  }
  if (enqueue) {
    internal::PushBack(&cases_to_be_selected_, c);
  }

  return false;
}

void PermanentEvent::Unregister(internal::CaseInSelectClause* c) {
  boost::fibers::detail::spinlock_lock lock(splk_);
  if (!notified_.load(std::memory_order_relaxed)) {
    // We only maintain lists of active cases up until notification.
    internal::UnlinkFromList(&cases_to_be_selected_, c);
  }
}

void PermanentEvent::Notify() {
  boost::fibers::detail::spinlock_lock lock(splk_);

  DCHECK(!notified_.load(std::memory_order_relaxed))
      << "Notify() method called more than once for "
      << "PermanentEvent object " << static_cast<void*>(this);
  notified_.store(true, std::memory_order_release);

  // The transition to a notified state is a permanent one, so we tear down any
  // enqueued cases. We must be careful to synchronize against this in the
  // future in both the Handle(..., true) and Unregister cases.
  while (cases_to_be_selected_) {
    internal::CaseInSelectClause* case_in_select_clause = cases_to_be_selected_;
    eglt::concurrency::impl::MutexLock l2(&case_in_select_clause->selector->mu);
    case_in_select_clause->TryPick();
    // Continued storage of enqueued_list_ after TryPick() is guaranteed by selector->mu
    internal::UnlinkFromList(&cases_to_be_selected_, case_in_select_clause);
  }
}

bool PermanentEvent::HasBeenNotified() const {
  return notified_.load(std::memory_order_acquire);
}

// Handle() always returns false, so no Select clause will select this case.
class NonSelectable final : public internal::Selectable {
 public:
  NonSelectable() = default;
  ~NonSelectable() override = default;

  bool Handle(internal::CaseInSelectClause* c, bool enqueue) override {
    return false;
  }
  void Unregister(internal::CaseInSelectClause* c) override {}
};

Case NonSelectableCase() {
  static absl::NoDestructor<NonSelectable> non_selectable;
  return {non_selectable.get()};
}

// AlwaysSelectable: a trivial implementation of a Selectable.
class AlwaysSelectable final : public internal::Selectable {
 public:
  AlwaysSelectable() = default;
  ~AlwaysSelectable() override = default;

  bool Handle(internal::CaseInSelectClause* c, bool enqueue) override {
    eglt::concurrency::impl::MutexLock lock(&c->selector->mu);
    // This selectable is always ready, so ask the selector to pick it.
    // Note: the selector still does not *have to* pick it if there are
    // other ready candidates.
    return c->TryPick();
  }
  void Unregister(internal::CaseInSelectClause* c) override {}
};

Case AlwaysSelectableCase() {
  static absl::NoDestructor<AlwaysSelectable> always_selectable;
  return {always_selectable.get()};
}

}  // namespace thread
