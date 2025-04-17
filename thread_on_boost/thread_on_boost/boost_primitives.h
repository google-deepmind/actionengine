#ifndef THREAD_ON_BOOST_BOOST_PRIMITIVES_H_
#define THREAD_ON_BOOST_BOOST_PRIMITIVES_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <os/os_sync_wait_on_address.h>
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

  bool WaitWithDeadline(Mutex* absl_nonnull mu, const absl::Time& deadline) {
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

#ifdef BOOST_OS_MACOS
class spinlock_ttas_futex {
 private:
  template <typename FBSplk>
  friend class spinlock_rtm;

  std::atomic<std::int32_t> value_{0};

 public:
  spinlock_ttas_futex() = default;

  spinlock_ttas_futex(spinlock_ttas_futex const&) = delete;
  spinlock_ttas_futex& operator=(spinlock_ttas_futex const&) = delete;

  void lock() noexcept {
    static thread_local std::minstd_rand generator{std::random_device{}()};
    std::int32_t collisions = 0, retries = 0, expected = 0;
    // after max. spins or collisions suspend via futex
    while (retries++ < BOOST_FIBERS_RETRY_THRESHOLD) {
      if (0 != (expected = value_.load(std::memory_order_relaxed))) {
#if !defined(BOOST_FIBERS_SPIN_SINGLE_CORE)
        if (BOOST_FIBERS_SPIN_BEFORE_SLEEP0 > retries) {
          cpu_relax();
        } else if (BOOST_FIBERS_SPIN_BEFORE_YIELD > retries) {
          static constexpr std::chrono::microseconds us0{0};
          std::this_thread::sleep_for(us0);
        } else {
          std::this_thread::yield();
        }
#else
        std::this_thread::yield();
#endif
      } else if (!value_.compare_exchange_strong(expected, 1,
                                                 std::memory_order_acquire)) {
        std::uniform_int_distribution<std::int32_t> distribution{
            0,
            static_cast<std::int32_t>(1)
                << (std::min)(collisions,
                              static_cast<std::int32_t>(
                                  BOOST_FIBERS_CONTENTION_WINDOW_THRESHOLD))};
        const std::int32_t z = distribution(generator);
        ++collisions;
        for (std::int32_t i = 0; i < z; ++i) {
          cpu_relax();
        }
      } else {
        // success, lock acquired
        return;
      }
    }
    if (2 != expected) {
      expected = value_.exchange(2, std::memory_order_acquire);
    }
    while (0 != expected) {
      os_sync_wait_on_address(&value_, 2, sizeof(value_),
                              OS_SYNC_WAIT_ON_ADDRESS_NONE);
      expected = value_.exchange(2, std::memory_order_acquire);
    }
  }

  bool try_lock() noexcept {
    std::int32_t expected = 0;
    return value_.compare_exchange_strong(expected, 1,
                                          std::memory_order_acquire);
  }

  void unlock() noexcept {
    if (1 != value_.fetch_sub(1, std::memory_order_acquire)) {
      value_.store(0, std::memory_order_release);
      os_sync_wake_by_address_any(&value_, sizeof(value_),
                                  OS_SYNC_WAKE_BY_ADDRESS_NONE);
    }
  }
};

using spinlock_impl = spinlock_ttas_futex;
#else
using spinlock_impl = boost::fibers::detail::spinlock;
#endif

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED SpinLock {
 public:
  void Lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() { spinlock_.lock(); }

  void Unlock() noexcept ABSL_UNLOCK_FUNCTION() { spinlock_.unlock(); }

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() { Lock(); }

  void unlock() noexcept ABSL_UNLOCK_FUNCTION() { Unlock(); }

 private:
  spinlock_impl spinlock_;
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