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

#ifndef THREAD_FIBER_FIBER_H_
#define THREAD_FIBER_FIBER_H_

#include <atomic>
#include <concepts>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <absl/log/check.h>

#include "g3fiber/boost_primitives.h"
#include "g3fiber/selectables.h"

namespace thread {

class TreeOptions {};

using InvocableWork = absl::AnyInvocable<void() &&>;

template <typename F>
concept InvocableWithNoArgsAndReturnsVoid =
    std::is_invocable_v<std::decay_t<F>> &&
    std::is_same_v<std::invoke_result_t<F>, void>;

template <typename F>
InvocableWork MakeInvocable(F&& f) requires
    InvocableWithNoArgsAndReturnsVoid<F> {
  return InvocableWork(std::forward<F>(f));
}

class Fiber;

namespace internal {
std::unique_ptr<Fiber> CreateTree(InvocableWork f, TreeOptions&& tree_options);
}

Fiber* GetPerThreadFiberPtr();

class Fiber {
 public:
  friend class FiberProperties;

  template <typename F>
  explicit Fiber(F&& f) requires InvocableWithNoArgsAndReturnsVoid<F>
      : Fiber(Unstarted{}, {std::forward<F>(f)}) {
    Start();
  }

  Fiber(const Fiber&) = delete;
  Fiber& operator=(const Fiber&) = delete;

  // REQUIRES: Join() must have been called.
  ~Fiber();

  // Return a pointer to the currently running fiber.
  static Fiber* Current();

  void Cancel();
  void Join();

  bool Cancelled() const { return cancellation_.HasBeenNotified(); }

  Case OnCancel() const { return cancellation_.OnEvent(); }

  Case OnJoinable() const { return joinable_.OnEvent(); }

 private:
  // No internal constructor starts the fiber. It is the caller's responsibility
  // to call Start() on the fiber.
  struct Unstarted {};

  // Internal constructor for root fibers.
  explicit Fiber(Unstarted, InvocableWork work, TreeOptions&& tree_options);
  // Internal constructor for child fibers.
  explicit Fiber(Unstarted, InvocableWork work,
                 Fiber* absl_nonnull parent = Current());

  void Start();
  bool MarkFinished();
  void MarkJoined();
  void InternalJoin();

  void PushBackChild(Fiber* absl_nonnull child)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (first_child_ == nullptr) {
      first_child_ = child;
    } else {
      child->prev_sibling_ = child;
      child->next_sibling_ = first_child_;
      first_child_->prev_sibling_ = child;
    }
  }

  void UnlinkChild(const Fiber* absl_nonnull child)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (child->next_sibling_ == child) {
      DCHECK(first_child_ == child)
          << "Unlinking a child that's the \"only\" sibling on its level, but "
             "is not the first child.";
      first_child_ = nullptr;
      return;
    }

    child->next_sibling_->prev_sibling_ = child->prev_sibling_;
    child->prev_sibling_->next_sibling_ = child->next_sibling_;
    if (first_child_ == child) {
      first_child_ = child->next_sibling_;
    }
  }

  mutable eglt::concurrency::impl::Mutex mu_;

  InvocableWork work_;
  boost::intrusive_ptr<boost::fibers::context> context_;

  // Whether this Fiber is self-joining. This is always set under lock, but is
  // an atomic to allow for reads during stats collection which cannot acquire
  // mutexes.
  std::atomic<bool> detached_ ABSL_GUARDED_BY(mu_) = false;

  enum State : uint8_t { RUNNING, FINISHED, JOINED };

  State state_ ABSL_GUARDED_BY(mu_) = RUNNING;

  Fiber* const parent_;
  Fiber* first_child_ ABSL_GUARDED_BY(mu_) = nullptr;
  Fiber* absl_nonnull next_sibling_;
  Fiber* absl_nonnull prev_sibling_;

  PermanentEvent cancellation_;
  PermanentEvent joinable_;

  friend std::unique_ptr<Fiber> internal::CreateTree(
      InvocableWork f, TreeOptions&& tree_options);

  friend class eglt::concurrency::impl::CondVar;

  friend struct ThreadLocalFiber;
  friend bool IsFiberDetached(const Fiber* fiber);
  friend void Detach(std::unique_ptr<Fiber> fiber);
};

class FiberProperties final : public boost::fibers::fiber_properties {
 public:
  friend class Fiber;
  friend class eglt::concurrency::impl::CondVar;

  explicit FiberProperties(boost::fibers::context* ctx) = delete;

  explicit FiberProperties(Fiber* fiber)
      : boost::fibers::fiber_properties(nullptr), fiber_(fiber) {}

  [[nodiscard]] Fiber* GetFiber() const { return fiber_; }

 private:
  Fiber* fiber_ = nullptr;
  eglt::concurrency::impl::CondVar* waiting_on_ ABSL_GUARDED_BY(fiber_->mu_) =
      nullptr;
};

namespace internal {
inline std::unique_ptr<Fiber> CreateTree(InvocableWork f,
                                         TreeOptions&& tree_options) {
  const auto fiber =
      new Fiber(Fiber::Unstarted{}, std::move(f), std::move(tree_options));
  fiber->Start();
  return absl::WrapUnique(fiber);
}
}  // namespace internal

template <typename F>
[[nodiscard]] std::unique_ptr<Fiber> NewTree(TreeOptions tree_options, F&& f) {
  return internal::CreateTree(MakeInvocable(std::forward<F>(f)),
                              std::move(tree_options));
}

inline void Detach(std::unique_ptr<Fiber> fiber) {
  {
    eglt::concurrency::impl::MutexLock lock(&fiber->mu_);
    DCHECK(!fiber->detached_.load(std::memory_order_relaxed))
        << "Detach() called on already detached fiber, this should not be "
           "possible without calling WrapUnique or similar on a Fiber* you do "
           "not own.";
    // If the fiber is FINISHED, we need to join it since it has passed the
    // point where it would be self joined and deleted if detached.
    if (fiber->state_ != Fiber::FINISHED) {
      fiber->detached_.store(true, std::memory_order_relaxed);
      fiber.release();  // Fiber will delete itself.
    }
  }
  if (ABSL_PREDICT_FALSE(fiber != nullptr)) {
    fiber->InternalJoin();
  }
}

FiberProperties* GetCurrentFiberProperties();

template <typename F>
void Detach(TreeOptions tree_options, F&& f) {
  Detach(NewTree(std::move(tree_options), std::forward<F>(f)));
}

inline bool Cancelled() {
  const Fiber* fiber_ptr = GetPerThreadFiberPtr();
  if (fiber_ptr == nullptr) {
    // Only threads which are already fibers could be cancelled.
    return false;
  }
  return fiber_ptr->Cancelled();
}

inline Case OnCancel() {
  const Fiber* current_fiber = Fiber::Current();
  if (current_fiber == nullptr) {
    return NonSelectableCase();
  }
  return current_fiber->OnCancel();
}

}  // namespace thread

#endif  // THREAD_FIBER_FIBER_H_
