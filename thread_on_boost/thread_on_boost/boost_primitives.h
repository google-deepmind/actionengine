#ifndef THREAD_ON_BOOST_BOOST_PRIMITIVES_H_
#define THREAD_ON_BOOST_BOOST_PRIMITIVES_H_

#include <boost/fiber/all.hpp>

#include "thread_on_boost/absl_headers.h"

namespace thread {

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  void Lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    try {
      mu_.lock();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Mutex lock failed. " << error.what();
    }
  }
  void Unlock() ABSL_UNLOCK_FUNCTION() {
    try {
      mu_.unlock();
    } catch (boost::fibers::lock_error& error) {
      LOG(FATAL) << "Mutex unlock failed. " << error.what();
    }
  }

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() { Lock(); }
  void unlock() ABSL_UNLOCK_FUNCTION() { Unlock(); }

  friend class CondVar;

 private:
  boost::fibers::mutex& GetImpl() { return mu_; }
  boost::fibers::mutex mu_;
};

class ABSL_SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(Mutex* absl_nonnull mu) ABSL_EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu) {
    this->mu_->Lock();
  }

  MutexLock(const MutexLock&) = delete;  // NOLINT(runtime/mutex)
  MutexLock(MutexLock&&) = delete;       // NOLINT(runtime/mutex)
  MutexLock& operator=(const MutexLock&) = delete;
  MutexLock& operator=(MutexLock&&) = delete;

  ~MutexLock() ABSL_UNLOCK_FUNCTION() { this->mu_->Unlock(); }

 private:
  Mutex* absl_nonnull const mu_;
};

class CondVar {
 public:
  CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait(Mutex* absl_nonnull mu) { return cv_.wait(mu->GetImpl()); }

  bool WaitWithTimeout(Mutex* absl_nonnull mu, const absl::Duration timeout) {
    const absl::Time deadline = absl::Now() + timeout;
    return WaitWithDeadline(mu, deadline);
  }

  bool WaitWithDeadline(Mutex* absl_nonnull mu, const absl::Time deadline) {
    const boost::fibers::cv_status status =
        cv_.wait_until(mu->GetImpl(), absl::ToChronoTime(deadline));

    if (status == boost::fibers::cv_status::timeout &&
        deadline != absl::InfiniteFuture()) {
      return true;
    }
    return false;
  }

  void Signal() { cv_.notify_one(); }

  void SignalAll() { cv_.notify_all(); }

 private:
  boost::fibers::condition_variable_any cv_;
};

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED SpinLock {
 public:
  void Lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() { spinlock_.lock(); }

  void Unlock() noexcept ABSL_UNLOCK_FUNCTION() { spinlock_.unlock(); }

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() { Lock(); }

  void unlock() noexcept ABSL_UNLOCK_FUNCTION() { Unlock(); }

 private:
  boost::fibers::detail::spinlock spinlock_;
};

class ABSL_SCOPED_LOCKABLE SpinLockHolder {
 public:
  explicit SpinLockHolder(SpinLock* lock) ABSL_EXCLUSIVE_LOCK_FUNCTION(lock)
      : lock_(lock) {
    lock_->Lock();
  }

  ~SpinLockHolder() ABSL_UNLOCK_FUNCTION() { lock_->Unlock(); }

  SpinLockHolder(const SpinLockHolder&) = delete;
  SpinLockHolder& operator=(const SpinLockHolder&) = delete;

 private:
  SpinLock* lock_;
};

}  // namespace thread

#endif  // THREAD_ON_BOOST_BOOST_PRIMITIVES_H_