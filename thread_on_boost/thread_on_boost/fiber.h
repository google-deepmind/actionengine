// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
// Author: pjt@google.com (Paul Turner)

#ifndef THREAD_FIBER_FIBER_H_
#define THREAD_FIBER_FIBER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <boost/asio/io_context.hpp>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/intrusive_list.h"
#include "thread_on_boost/selectables.h"

namespace thread {

class TreeOptions {};

// The basic unit of work.
using Invocable = absl::AnyInvocable<void() &&>;

// Creates an Invocable, checking that it actually returns void.
template <typename F, typename = std::invoke_result_t<F>>
Invocable MakeInvocable(F&& f) {
  static_assert(std::is_void_v<std::invoke_result_t<std::decay_t<F>>>,
                "Functions passed to MakeInvocable must return void.");
  return Invocable(std::forward<F>(f));
}

// A private scope to ensure that only Fiber internals can access the intrusive
// cancellation list.
class CancellationList {
  struct Tag {};
  friend class Fiber;
};

template <typename Algo, typename... Args>
static void EnsureThreadHasScheduler(Args&&... args) {
  thread_local bool kThreadHasScheduler = false;
  // DLOG(INFO) << "EnsureThreadHasScheduler, thread_id="
  //            << std::this_thread::get_id()
  //            << ", kThreadHasScheduler=" << kThreadHasScheduler;
  if (kThreadHasScheduler) {
    return;
  }

  boost::fibers::use_scheduling_algorithm<Algo>(std::forward<Args>(args)...);
  kThreadHasScheduler = true;
}

class Fiber;

class FiberProperties final : public boost::fibers::fiber_properties {
 public:
  explicit FiberProperties(boost::fibers::context* ctx) = delete;

  explicit FiberProperties(Fiber* fiber)
      : boost::fibers::fiber_properties(nullptr), fiber_(fiber) {}

  [[nodiscard]] Fiber* GetFiber() const { return fiber_; }

 private:
  Fiber* fiber_ = nullptr;
};

namespace internal {
std::unique_ptr<Fiber> CreateTree(Invocable f, TreeOptions&& tree_options);
}

Fiber* GetPerThreadFiberPtr();

class Fiber : gtl::intrusive_link<Fiber, CancellationList::Tag> {
 public:
  template <typename F,
            // Avoid binding Fiber(const Fiber&) or Fiber(Fiber):
            typename = std::invoke_result_t<F>>
  explicit Fiber(F&& f)
      : Fiber(Unstarted{}, MakeInvocable(std::forward<F>(f))) {
    Start();
  }

  Fiber(const Fiber&) = delete;
  Fiber& operator=(const Fiber&) = delete;

  // REQUIRES: Join() must have been called.
  ~Fiber();

  // Return a pointer to the currently running fiber.
  static Fiber* Current();

  // Wait until *this and all of its descendants have finished running. When a
  // fiber is created with one of the constructors above, its Join() method must
  // be called before it may be destructed. Second and further calls to Join()
  // are no-ops.
  //
  // REQUIRES: Must be called by the parent, unless this is a root fiber.
  void Join();

  void Cancel();

  bool Cancelled() const { return cancellation_.HasBeenNotified(); }
  Case OnCancel() const { return cancellation_.OnEvent(); }

  Case OnJoinable() const { return joinable_.OnEvent(); }

 private:
  // No internal constructor starts the fiber. It is the caller's responsibility
  // to call Start() on the fiber.
  struct Unstarted {};

  // Internal c'tor for Fibers.
  explicit Fiber(Unstarted, Invocable invocable, Fiber* parent = Current());
  // Internal c'tor for root fibers.
  explicit Fiber(Unstarted, Invocable invocable, TreeOptions&& tree_options);

  void Start();
  void Body();
  bool MarkFinished();
  void MarkJoined();
  void InternalJoin();

  mutable Mutex mu_;

  Invocable work_;
  boost::intrusive_ptr<boost::fibers::context> context_;

  // Whether this Fiber is self-joining. This is always set under lock, but is
  // an atomic to allow for reads during stats collection which cannot acquire
  // mutexes.
  std::atomic<bool> detached_ ABSL_GUARDED_BY(mu_) = false;

  enum State : uint8_t { RUNNING, FINISHED, JOINED };
  State state_ ABSL_GUARDED_BY(mu_) = RUNNING;

  Fiber* const parent_;
  // List of child fibers.
  gtl::intrusive_list<Fiber, CancellationList::Tag> children_
      ABSL_GUARDED_BY(mu_);

  PermanentEvent cancellation_{PermanentEvent::CancellationEventTag()};
  PermanentEvent joinable_;

  friend std::unique_ptr<Fiber> internal::CreateTree(
      Invocable f, TreeOptions&& tree_options);

  friend struct PerThreadDynamicFiber;
  friend bool IsFiberDetached(const Fiber* fiber);
  friend class gtl::intrusive_list<Fiber, CancellationList::Tag>;
  friend class gtl::intrusive_link<Fiber, CancellationList::Tag>;
  friend void Detach(std::unique_ptr<Fiber> fiber);
};

namespace internal {
inline std::unique_ptr<Fiber> CreateTree(Invocable f,
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
    MutexLock l(&fiber->mu_);
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
  // This calls InternalJoin to bypass the check that the current fiber is a
  // parent. It may be a non-parent ancestor in the case of bundle fibers.
  if (ABSL_PREDICT_FALSE(fiber != nullptr)) {
    fiber->InternalJoin();
  };
}

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
