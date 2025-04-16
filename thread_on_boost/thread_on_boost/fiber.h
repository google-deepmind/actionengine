// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)
// Author: pjt@google.com (Paul Turner)
//
// A Fiber is a light-weight thread.  It supports cancellations, deadlines,
// priorities, etc.  A typical use is that a Fiber is created when an RPC
// arrives at a server, and the RPC is considered finished when the fiber stops
// running.
//
// Fibers are arranged in a tree.  E.g., when an RPC handler needs to
// run several computations (or sub-RPCs) in parallel, it should
// create a set of child fibers and run the sub-computations in those
// child fibers. It must Join() them before it finishes.
//
//   void ServerHandler(...) {
//     thread::Fiber worker1([] { ... });  // Lambdas work great.
//     thread::Fiber worker2(BindFront(SubComputation, ...));
//     worker1.Join();
//     worker2.Join();
//   }
//
// Note that ServerHandler() itself is running within some fiber and the two
// fibers (worker1 and worker2) are created as children of that fiber.
//
// Methods on the same fiber can be safely called concurrently from multiple
// threads (any exceptions will be documented on a per-method basis).
//
// Cancellations
// -------------
// A fiber can be cancelled.  The cancellation is automatically
// propagated to all descendants of the fiber.  I.e., sub-computations
// are also cancelled.  This cancellation should be considered a
// request to the code to stop quickly.  Nothing is killed
// automatically; it is up to the cancelled code to detect the
// cancellation and stop doing what it was doing.
//
// Example:
//    // Start a child fiber and cancel it after a second.
//    void Parent(...) {
//      thread::Fiber child(absl::bind_front(Child, ...));
//      absl::SleepFor(absl::Seconds(1));
//      child.Cancel();
//      child.Join();  // Wait for "child" to observe cancellation and complete.
//    }
//
//    // The child code that loops doing stuff until it is cancelled
//    void Child(...) {
//      while (!thread::Cancelled()) {
//        DoSomeWork();
//      }
//    }
//
// A child may want to detect cancellation while reading from some channels.  To
// support this, fibers provide a Case (see thread::OnCancel) that can be
// included in a Select statement (in addition to the ability to pass Reader or
// Writer objects to Select).  The Case becomes ready when cancellation is
// requested.
//
// Example: read and process data from a channel until cancelled.
//
//   void Process(thread::Reader<Value>* r, ...) {
//     Value v;
//     bool ok;
//     thread::CaseArray cases =
//         {r->OnRead(&v, &ok), thread::OnCancel()};
//
//     while (thread::Select(cases) == 0 && ok) {
//       Process(v);
//     }
//   }
//
// Note that since cancellation is automatically propagated to the entire
// sub-tree of fibers, a fiber that is just waiting for child fibers to finish
// does not need to have any special cancellation handling.  It is up to the
// code running in the child fibers to stop in response to the cancellation,
// which will automatically allow the parent to finish. This feature should
// allow most code that uses fibers to automatically be cancellation aware
// without requiring any custom cancellation handling code.
//
// Parallelism
// -----------
// The parallelism of the fiber's tree determines the parallelism of the
// children. For example, if ServerHandler() above runs within the scope of an
// inbound Stubby4 RPC, the above fibers will typically run in parallel (exactly
// how many run at once depends on Stubby's configuration). But if
// ServerHandler() is run from the scope of main, the above fibers may or may
// not run in parallel since main has an unspecified parallelism.
//
// You can use thread::NewTree() to create a fiber tree with parallelism
// specified by thread::TreeOptions().
//
// TODO:
// . request flow tracing
// . properties of fibers

#ifndef THREAD_FIBER_FIBER_H_
#define THREAD_FIBER_FIBER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <boost/asio/io_context.hpp>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/asio_round_robin.h"
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
inline std::unique_ptr<Fiber> CreateTree(Invocable f,
                                         TreeOptions&& tree_options);
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

  void Start() const;
  void Body();
  bool MarkFinished();
  void MarkJoined();
  void InternalJoin();

  mutable Mutex mu_;

  Invocable work_;
  boost::intrusive_ptr<boost::fibers::context> ctx_{};

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

inline static thread_local boost::asio::io_context* thread_io_context = nullptr;
inline static thread_local boost::asio::executor_work_guard<
    boost::asio::io_context::executor_type>* thread_asio_work_guard;

inline boost::asio::io_context& GetThreadLocalIOContext() {
  if (ABSL_PREDICT_FALSE(thread_io_context == nullptr)) {
    thread_io_context = new boost::asio::io_context();
    thread_asio_work_guard = new boost::asio::executor_work_guard(
        boost::asio::make_work_guard(*thread_io_context));
  }
  return *thread_io_context;
}

}  // namespace thread

#endif  // THREAD_FIBER_FIBER_H_
