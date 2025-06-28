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

#include "thread_on_boost/fiber.h"

#include <latch>

#include <boost/fiber/all.hpp>
#include <boost/fiber/context.hpp>
#include <boost/intrusive_ptr.hpp>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/select.h"
#include "thread_on_boost/thread_pool.h"

namespace thread {

bool IsFiberDetached(const Fiber* fiber) {
  return ABSL_TS_UNCHECKED_READ(fiber->detached_)
      .load(std::memory_order_relaxed);
}

struct PerThreadDynamicFiber {
  Fiber* f = nullptr;

  ~PerThreadDynamicFiber() {
    // This destructor is called while destroying thread-local storage. If it is
    // null, there is no dynamic fiber for this thread.
    DVLOG(2) << "PerThreadDynamicFiber destructor called: " << f;
    if (f != nullptr) {
      f->MarkFinished();
      f->InternalJoin();
      delete f;
    }
  }
};

static thread_local PerThreadDynamicFiber kPerThreadDynamicFiber;

Fiber::Fiber(Unstarted, InvocableWork work, Fiber* parent)
    : work_(std::move(work)),
      parent_(parent),
      next_sibling_(this),
      prev_sibling_(this) {
  // Note: We become visible to cancellation as soon as we're added to parent.
  eglt::concurrency::impl::MutexLock l(&parent_->mu_);
  CHECK_EQ(parent_->state_, RUNNING);
  parent->PushBackChild(this);
  if (parent_->cancellation_.HasBeenNotified()) {
    // Fibers adjoined to a cancelled tree inherit implicit cancellation.
    DVLOG(2) << "F " << this << " joining cancelled sub-tree.";
    Cancel();
  }
}

Fiber::Fiber(Unstarted, InvocableWork work, TreeOptions&& tree_options)
    : work_(std::move(work)),
      parent_(nullptr),
      next_sibling_(this),
      prev_sibling_(this) {}

void Fiber::Start() {
  EnsureWorkerThreadPool();
  EnsureThreadHasScheduler<boost::fibers::algo::round_robin>();

  auto body = [this]() {
    std::move(work_)();
    work_ = nullptr;

    if (MarkFinished()) {
      // MarkFinished returns whether the fiber was detached when finished.
      // Detached fibers are self-joining.
      InternalJoin();
      context_.detach();
      delete this;
    }
  };

  // FiberProperties get destroyed when the underlying context is
  // destroyed. We do not care about the lifetime of the raw pointer that
  // is made here.
  context_ = boost::fibers::make_worker_context_with_properties(
      boost::fibers::launch::post, new FiberProperties(this),
      boost::fibers::make_stack_allocator_wrapper<
          boost::fibers::pooled_fixedsize_stack>(),
      std::move(body));
  WorkerThreadPool::Instance().Schedule(context_.get());
}

Fiber::~Fiber() {
  CHECK_EQ(JOINED, state_) << "F " << this << " attempting to destroy an "
                           << "unjoined Fiber.  (Did you forget to Join() "
                           << "on a child?)";
  DCHECK(first_child_ == nullptr);

  DVLOG(2) << "F " << this << " destroyed";
}

Fiber* GetPerThreadFiberPtr() {
  const boost::fibers::context* ctx = boost::fibers::context::active();
  // If we do not have an internal boost::fibers::context at all,
  // then something is wrong. We should never be called outside a fiber context.
  if (ctx == nullptr) {
    LOG(FATAL) << "Current() called outside of a fiber context.";
    ABSL_ASSUME(false);
  }

  // If we have been created through thread_on_boost API, there will be properties
  // associated with the context. We can use them to get the fiber.
  if (const FiberProperties* props =
          dynamic_cast<FiberProperties*>(ctx->get_properties());
      ABSL_PREDICT_TRUE(props != nullptr)) {
    return props->GetFiber();
  }

  // Otherwise, return the thread-local no-op fiber (not caring if it has been
  // created or not).
  return kPerThreadDynamicFiber.f;
}

Fiber* Fiber::Current() {
  if (Fiber* current_fiber = GetPerThreadFiberPtr();
      ABSL_PREDICT_TRUE(current_fiber != nullptr)) {
    return current_fiber;
  }

  // We only reach here if we're 1) not under any Fiber, 2) this thread does not
  // yet have a thread-local fiber. We can (and should) create and return it.
  kPerThreadDynamicFiber.f =
      new Fiber(Unstarted{}, InvocableWork(), TreeOptions{});
  DVLOG(2) << "Current() called (new static thread-local fiber created): "
           << kPerThreadDynamicFiber.f;

  return kPerThreadDynamicFiber.f;
}

void Fiber::Join() {
  // Join must be externally called and so can never be valid when detached.  It
  // is important to detect this since we may not safely proceed beyond Select()
  // in this case.
  DCHECK(!IsFiberDetached(this)) << "Join() on detached fiber.";

  {
    eglt::concurrency::impl::MutexLock l(&mu_);
    CHECK(state_ != JOINED) << "Join() called on already joined fiber.";
  }

  const Fiber* current_fiber = GetPerThreadFiberPtr();
  CHECK(this != current_fiber) << "Fiber trying to join itself!";
  if (parent_ != nullptr) {
    CHECK(parent_ == current_fiber) << "Join() called from non-parent fiber";
  }

  InternalJoin();
  context_->join();
}

// Update *this to a FINISHED state.  Preparing it to be Join()-ed (and
// notifying any waiters) when applicable. Returns whether the fiber was
// detached when marked finished.
//
// REQUIRES: *this has not already been marked finished.
bool Fiber::MarkFinished() {
  eglt::concurrency::impl::MutexLock l(&mu_);
  DCHECK_EQ(state_, RUNNING);

  state_ = FINISHED;

  // Any fiber can have detached children.
  if (first_child_ == nullptr) {
    joinable_.Notify();
    // Although joinable_ is true, any foreign call to Join() also needs to
    // acquire mu_, thus we can't be deleted yet.
  }
  return detached_.load(std::memory_order_relaxed);
}

// Record that the Join() requirement has been satisfied.  In the case of a
// detached fiber this may have been internally generated.
//
// If *this was a child fiber it will be removed from its parent's active
// children.
//
// No-op if *this has already been Join()-ed.
void Fiber::MarkJoined() {
  DCHECK(joinable_.HasBeenNotified());

  bool has_parent;
  {
    eglt::concurrency::impl::MutexLock l(&mu_);
    DCHECK(first_child_ == nullptr);
    if (state_ == JOINED)
      return;  // Already joined.
    DCHECK_EQ(state_, FINISHED);
    DVLOG(2) << "F " << this << " joined";
    state_ = JOINED;
    has_parent = parent_ != nullptr;
  }
  if (has_parent) {
    eglt::concurrency::impl::MutexLock l(&parent_->mu_);
    parent_->UnlinkChild(this);
    if (parent_->first_child_ == nullptr && parent_->state_ == FINISHED) {
      parent_->joinable_.Notify();
    }
  } else {
    // // We were joined and have no parent. All of our children must already be
    // // joined. Release our ref on the scheduler.
    // tree_scheduler_.Unref();
  }
}

void Fiber::InternalJoin() {
  Select({joinable_.OnEvent()});
  MarkJoined();
}

void Fiber::Cancel() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto current = this;
  while (true) {
    DCHECK(current != nullptr);
    // We visit nodes in post-order, traversing each child sub-tree by sibling
    // position before operating on the parent.  We hold all "mu_"s up to and
    // including the initiating parent fiber node.
    current->mu_.Lock();

    // Check whether the fiber we're currently visiting has already been
    // cancelled.
    bool cancelled = current->cancellation_.HasBeenNotified();

    // If we have children, and we're already cancelled, then they must be also.
    // We don't need to re-walk our children as future descendants will be
    // spawned into a cancelled state.
    // If we have children, and we're not cancelled, we must visit them before
    // operating on "fiber".
    if (!cancelled && current->first_child_ != nullptr) {
      // Equivalent recursion note: recursive call.
      current = current->first_child_;
      continue;
    }

    while (true) {
      if (!cancelled) {
        current->cancellation_.Notify();
      }

      class ScopedMutexUnlocker {
       public:
        explicit ScopedMutexUnlocker(eglt::concurrency::impl::Mutex* mu)
            : mu_(*mu) {}
        ~ScopedMutexUnlocker() { mu_.Unlock(); }

       private:
        eglt::concurrency::impl::Mutex& mu_;
      };
      ScopedMutexUnlocker unlock_mu(&current->mu_);

      // Once we reach the fiber (*this) parenting cancellation, we're finished.
      if (current == this)
        return;

      DCHECK(current->parent_ != nullptr);
      DCHECK(current->parent_->first_child_ != nullptr);

      // If there is an unvisited sibling, we go there to process it.
      // Equivalent recursion note: recursive call return and recursive call.
      if (current->next_sibling_ != current) {
        current = current->next_sibling_;
        break;
      }

      // We've reached the final sibling in this subtree.  Continue traversing
      // from our parent, who has no more unvisited children and is next in the
      // traversal order.
      // Equivalent recursion note: recursive call return.
      current = current->parent_;

      // Reached child => traversal spans our parent, which must need
      // cancellation.
      cancelled = false;
    }
  }
}

}  // namespace thread
