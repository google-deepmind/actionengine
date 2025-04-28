#include "thread_on_boost/fiber.h"

#include <latch>

#include <boost/fiber/all.hpp>
#include <boost/fiber/context.hpp>
#include <boost/intrusive_ptr.hpp>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/select.h"

namespace thread {

// static
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
      // TODO(b/384529493) Identify what to do if not currently joinable.
      f->InternalJoin();
      delete f;
    }
  }
};

static thread_local PerThreadDynamicFiber kPerThreadDynamicFiber;

class WorkerThreadPool {
 public:
  explicit WorkerThreadPool() = default;

  void Start(size_t num_threads = std::thread::hardware_concurrency()) {
    schedulers_.resize(num_threads);
    std::latch latch(num_threads);
    for (size_t idx = 0; idx < num_threads; ++idx) {
      Worker worker{
          .thread = std::thread([this, idx, &latch] {
            EnsureThreadHasScheduler<boost::fibers::algo::shared_work>(
                /*suspend=*/true);
            schedulers_[idx] =
                boost::fibers::context::active()->get_scheduler();
            latch.count_down();
            // TODO: cancellation
            while (!Cancelled()) {
              boost::fibers::context::active()->suspend();
            }
            DLOG(INFO) << absl::StrFormat("Worker %zu exiting.", idx);
          }),
      };
      workers_.push_back(std::move(worker));
    }
    latch.wait();

    for (auto& [thread] : workers_) {
      thread.detach();
    }
  }

  void Schedule(boost::fibers::context* ctx) {
    const size_t worker_idx =
        worker_idx_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    schedulers_[worker_idx]->attach_worker_context(ctx);

    if (schedulers_[worker_idx] ==
        boost::fibers::context::active()->get_scheduler()) {
      schedulers_[worker_idx]->schedule(ctx);
    } else {
      schedulers_[worker_idx]->schedule_from_remote(ctx);
    }
  }

  static WorkerThreadPool& Instance() {
    static WorkerThreadPool instance;
    if (instance.workers_.empty()) {
      instance.Start();
    }
    return instance;
  }

 private:
  struct Worker {
    std::thread thread;
  };

  std::atomic<size_t> worker_idx_{0};
  absl::InlinedVector<Worker, 16> workers_;
  absl::InlinedVector<boost::fibers::scheduler*, 16> schedulers_;
};

static absl::once_flag kInitWorkerThreadPoolFlag;
static void InitWorkerThreadPool() {
  WorkerThreadPool::Instance();
}

Fiber::Fiber(Unstarted, Invocable invocable, Fiber* parent)
    : work_(std::move(invocable)), parent_(parent) {
  // Note: We become visible to cancellation as soon as we're added to parent.
  MutexLock l(&parent_->mu_);
  CHECK_EQ(parent_->state_, RUNNING);
  parent_->children_.push_back(this);
  if (parent_->cancellation_.HasBeenNotified()) {
    // Fibers adjoined to a cancelled tree inherit implicit cancellation.
    DVLOG(2) << "F " << this << " joining cancelled sub-tree.";
    Cancel();
  }
}

Fiber::Fiber(Unstarted, Invocable invocable, TreeOptions&& tree_options)
    : work_(std::move(invocable)), parent_(nullptr) {}

void Fiber::Start() {
  absl::call_once(kInitWorkerThreadPoolFlag, InitWorkerThreadPool);
  // FiberProperties get destroyed when the underlying context is
  // destroyed. We do not care about the lifetime of the raw pointer that
  // is made here.
  context_ = boost::fibers::make_worker_context_with_properties(
      boost::fibers::launch::post, new FiberProperties(this),
      boost::fibers::make_stack_allocator_wrapper<
          boost::fibers::default_stack>(),
      absl::bind_front(&Fiber::Body, this));

  WorkerThreadPool::Instance().Schedule(context_.get());
}

void Fiber::Body() {
  std::move(work_)();
  work_ = nullptr;

  if (MarkFinished()) {
    // MarkFinished returns whether the fiber was detached when finished.
    // Detached fibers are self-joining.
    InternalJoin();
    delete this;
  }
}

Fiber::~Fiber() {
  CHECK_EQ(JOINED, state_) << "F " << this << " attempting to destroy an "
                           << "unjoined Fiber.  (Did you forget to Join() "
                           << "on a child?)";
  CHECK(context_ == nullptr)
      << "Fiber::Join() must be called before destroying a fiber.";
  DCHECK(children_.empty());

  DVLOG(2) << "F " << this << " destroyed";
}

Fiber* GetPerThreadFiberPtr() {
  const boost::fibers::context* ctx = boost::fibers::context::active();
  // If we do not have an internal boost::fibers::context at all,
  // then something is wrong. We should never be called outside a fiber context.
  if (ctx == nullptr) {
    LOG(FATAL) << "Current() called outside of a fiber context.";
    return nullptr;
  }

  // If we have been created through thread_on_boost API, there will be properties
  // associated with the context. We can use them to get the fiber.
  if (const FiberProperties* props =
          static_cast<FiberProperties*>(ctx->get_properties());
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
  kPerThreadDynamicFiber.f = new Fiber(Unstarted{}, Invocable(), TreeOptions{});
  DVLOG(2) << "Current() called (new static thread-local fiber created): "
           << kPerThreadDynamicFiber.f;

  return kPerThreadDynamicFiber.f;
}

void Fiber::Join() {
  // Join must be externally called and so can never be valid when detached.  It
  // is important to detect this since we may not safely proceed beyond Select()
  // in this case.
  DCHECK(!IsFiberDetached(this)) << "Join() on detached fiber.";

  Fiber* current_fiber = GetPerThreadFiberPtr();
  CHECK(this != current_fiber) << "Fiber trying to join itself!";
  if (parent_ != nullptr) {
    CHECK(parent_ == current_fiber) << "Join() called from non-parent fiber";
  }

  InternalJoin();
}

// Update *this to a FINISHED state.  Preparing it to be Join()-ed (and
// notifying any waiters) when applicable. Returns whether the fiber was
// detached when marked finished.
//
// REQUIRES: *this has not already been marked finished.
bool Fiber::MarkFinished() {
  MutexLock l(&mu_);
  DCHECK_EQ(state_, RUNNING);

  state_ = FINISHED;

  // Any fiber can have detached children.
  if (children_.empty()) {
    // We have finished execution associated with this Fiber.  Free any
    // associated TraceContext now, rather than waiting for our parent to
    // Join() and release us.
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
    MutexLock l(&mu_);
    DCHECK(children_.empty());
    if (state_ == JOINED)
      return;  // Already joined.
    DCHECK_EQ(state_, FINISHED);
    DVLOG(2) << "F " << this << " joined";
    state_ = JOINED;
    has_parent = parent_ != nullptr;
  }
  if (has_parent) {
    MutexLock l(&parent_->mu_);
    parent_->children_.erase(this);
    if (parent_->children_.empty() && parent_->state_ == FINISHED) {
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
  // if (context_ != nullptr) {
  //   context_->detach();
  // }
  context_.reset();
  MarkJoined();
}

void Fiber::Cancel() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  Fiber* fiber = this;
  while (true) {
    DCHECK(fiber != nullptr);
    // We visit nodes in post-order, traversing each child sub-tree by sibling
    // position before operating on the parent.  We hold all "mu_"s up to and
    // including the initiating parent fiber node.
    fiber->mu_.Lock();

    // Check whether the fiber we're currently visiting has already been
    // cancelled.
    //
    // We don't want to do a cancellation coloring check here because the
    // currently-running thread is checking the cancellation status of a
    // potentially different fiber, not its own. It's fine to call Cancel on
    // some other fiber.
    //
    // Indeed as far as the user is concerned we're not even checking the
    // cancellation status, we're setting it -- this check is here only for use
    // in an optimization below.
    bool cancelled = fiber->cancellation_.HasBeenNotified(
        PermanentEvent::SuppressCancellationColorCheckTag{});

    // If we have children, and we're already cancelled, then they must be also.
    // We don't need to re-walk our children as future descendants will be
    // spawned into a cancelled state.
    // If we have children, and we're not cancelled, we must visit them before
    // operating on "fiber".
    if (!cancelled && !fiber->children_.empty()) {
      // Equivalent recursion note: recursive call.
      fiber = &fiber->children_.front();
      continue;
    }

    while (true) {
      if (!cancelled) {
        fiber->cancellation_.Notify();
      }

      class ScopedMutexUnlocker {
       public:
        explicit ScopedMutexUnlocker(thread::Mutex* mu) : mu_(*mu) {}
        ~ScopedMutexUnlocker() { mu_.Unlock(); }

       private:
        thread::Mutex& mu_;
      };
      ScopedMutexUnlocker unlock_mu(&fiber->mu_);

      // Once we reach the fiber (*this) parenting cancellation, we're finished.
      if (fiber == this)
        return;

      DCHECK(fiber->parent_ != nullptr);
      DCHECK(!fiber->parent_->children_.empty());

      // Construct an iterator to the child list this is a part of.
      gtl::intrusive_list<Fiber, CancellationList::Tag>::iterator it(fiber);
      ++it;

      // If there is a unvisited sibling, we go there to process it.
      // Equivalent recursion note: recursive call return and recursive call.
      if (it != fiber->parent_->children_.end()) {
        fiber = &*it;
        break;
      }

      // We've reached the final sibling in this subtree.  Continue traversing
      // from our parent, whom has no more unvisited children and is next in the
      // traversal order.
      // Equivalent recursion note: recursive call return.
      fiber = fiber->parent_;

      // Reached child => traversal spans our parent, which must need
      // cancellation.
      cancelled = false;
    }
  }
}

}  // namespace thread
