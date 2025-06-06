#ifndef THREAD_ON_BOOST_BOOST_PRIMITIVES_H_
#define THREAD_ON_BOOST_BOOST_PRIMITIVES_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/fiber/all.hpp>

#include "thread_on_boost/absl_headers.h"

namespace eglt::concurrency::impl {

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  void Lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    try {
      mu_.lock();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Mutex lock failed. " << error.what();
      ABSL_ASSUME(false);
    }
  }
  void Unlock() noexcept ABSL_UNLOCK_FUNCTION() {
    try {
      mu_.unlock();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Mutex unlock failed. " << error.what();
      ABSL_ASSUME(false);
    }
  }

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() { Lock(); }
  void unlock() noexcept ABSL_UNLOCK_FUNCTION() { Unlock(); }

  friend class CondVar;

 private:
  boost::fibers::mutex& GetImpl() { return mu_; }
  boost::fibers::mutex mu_;
};

class ABSL_SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(Mutex* absl_nonnull mu) ABSL_EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu) {
    mu_->Lock();
  }

  MutexLock(const MutexLock&) = delete;  // NOLINT(runtime/mutex)
  MutexLock(MutexLock&&) = delete;       // NOLINT(runtime/mutex)
  MutexLock& operator=(const MutexLock&) = delete;
  MutexLock& operator=(MutexLock&&) = delete;

  ~MutexLock() ABSL_UNLOCK_FUNCTION() { mu_->Unlock(); }

 private:
  Mutex* absl_nonnull const mu_;
};

class CondVar {
 public:
  CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait(Mutex* absl_nonnull mu) noexcept ABSL_SHARED_LOCKS_REQUIRED(mu) {
    try {
      cv_.wait(mu->GetImpl());
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Error in underlying implementation: " << error.what();
      ABSL_ASSUME(false);
    }
  }

  bool WaitWithTimeout(Mutex* absl_nonnull mu, absl::Duration timeout) noexcept
      ABSL_SHARED_LOCKS_REQUIRED(mu) {
    return WaitWithDeadline(mu, absl::Now() + timeout);
  }

  bool WaitWithDeadline(Mutex* absl_nonnull mu,
                        const absl::Time& deadline) noexcept
      ABSL_SHARED_LOCKS_REQUIRED(mu) {
    if (ABSL_PREDICT_TRUE(deadline == absl::InfiniteFuture())) {
      Wait(mu);
      return false;
    }

    try {
      return cv_.wait_for(mu->GetImpl(),
                          absl::ToChronoNanoseconds(deadline - absl::Now())) ==
             boost::fibers::cv_status::timeout;
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Error in underlying implementation: " << error.what();
      ABSL_ASSUME(false);
    }
  }

  void Signal() noexcept {
    try {
      cv_.notify_one();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Error in underlying implementation: " << error.what();
      ABSL_ASSUME(false);
    }
  }

  void SignalAll() noexcept {
    try {
      cv_.notify_all();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Error in underlying implementation: " << error.what();
      ABSL_ASSUME(false);
    }
  }

 private:
  boost::fibers::condition_variable_any cv_;
};

inline void SleepFor(absl::Duration duration) {
  boost::fibers::context* active_ctx = boost::fibers::context::active();
  active_ctx->wait_until(std::chrono::steady_clock::now() +
                         absl::ToChronoNanoseconds(duration));
}

}  // namespace eglt::concurrency::impl

#endif  // THREAD_ON_BOOST_BOOST_PRIMITIVES_H_