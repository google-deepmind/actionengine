#ifndef EGLT_CONCURRENCY_CONCURRENCY_H_
#define EGLT_CONCURRENCY_CONCURRENCY_H_

#include <memory>

#include "eglt/absl_headers.h"

#if !defined(__EGLT_CONCURRENCY_IMPLEMENTATION__)
#include "eglt/concurrency/not_implemented.h"
#elif defined(__EGLT_CONCURRENCY_IMPLEMENTATION_BOOST_FIBER__)
#include "eglt/concurrency/boost_fiber.h"
#endif

namespace eglt::concurrency {
using Case = impl::Case;
using CaseArray = impl::CaseArray;
using TreeOptions = impl::TreeOptions;

template <typename T>
using ChannelReader = impl::ChannelReader<T>;

template <typename T>
using ChannelWriter = impl::ChannelWriter<T>;

template <typename T>
using Channel = impl::Channel<T>;

using Fiber = impl::Fiber;
using PermanentEvent = impl::PermanentEvent;

using Mutex = impl::Mutex;
using MutexLock = impl::MutexLock;

class ABSL_SCOPED_LOCKABLE TwoMutexLock {
 public:
  explicit TwoMutexLock(Mutex* absl_nonnull mu1, Mutex* absl_nonnull mu2)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu1, mu2)
      : mu1_(mu1), mu2_(mu2) {
    if (ABSL_PREDICT_FALSE(mu1_ == mu2_)) {
      mu1_->Lock();
      return;
    }

    if (mu1 < mu2) {
      mu1_->Lock();
      mu2_->Lock();
    } else {
      mu2_->Lock();
      mu1_->Lock();
    }
  }

  TwoMutexLock(const TwoMutexLock&) = delete;  // NOLINT(runtime/mutex)
  TwoMutexLock(TwoMutexLock&&) = delete;       // NOLINT(runtime/mutex)
  TwoMutexLock& operator=(const TwoMutexLock&) = delete;
  TwoMutexLock& operator=(TwoMutexLock&&) = delete;

  ~TwoMutexLock() ABSL_UNLOCK_FUNCTION() {
    if (ABSL_PREDICT_FALSE(mu1_ == mu2_)) {
      mu1_->Unlock();
      return;
    }

    if (mu1_ < mu2_) {
      mu2_->Unlock();
      mu1_->Unlock();
    } else {
      mu1_->Unlock();
      mu2_->Unlock();
    }
  }

 private:
  Mutex* absl_nonnull const mu1_;
  Mutex* absl_nonnull const mu2_;
};

inline void JoinOptimally(Fiber* fiber) {
  impl::JoinOptimally(fiber);
}

inline bool Cancelled() {
  return impl::Cancelled();
}

inline Case OnCancel() {
  return impl::OnCancel();
}

inline int Select(const CaseArray& cases) {
  return impl::Select(cases);
}

inline int SelectUntil(const absl::Time deadline, const CaseArray& cases) {
  return impl::SelectUntil(deadline, cases);
}

inline void Detach(const TreeOptions& options,
                   absl::AnyInvocable<void()>&& fn) {
  impl::Detach(options, std::move(fn));
}

inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options,
                                      absl::AnyInvocable<void()>&& fn) {
  return impl::NewTree(options, std::move(fn));
}
}  // namespace eglt::concurrency

#endif  // EGLT_CONCURRENCY_CONCURRENCY_H_
