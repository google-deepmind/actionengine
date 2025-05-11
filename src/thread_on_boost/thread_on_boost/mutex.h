#ifndef THREAD_ON_BOOST_MUTEX_H
#define THREAD_ON_BOOST_MUTEX_H

#include <atomic>

#include "thread_on_boost/absl_headers.h"
#include "thread_on_boost/futex.h"

namespace thread {

class ThreadOnlyMutex ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED {
 public:
  static constexpr int32_t kUnlocked = 0;
  static constexpr int32_t kLocked = 1;
  static constexpr int32_t kLockedWithWaiters = 2;

  ThreadOnlyMutex() = default;
  ~ThreadOnlyMutex() = default;

  void Lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() noexcept {
    if (ABSL_PREDICT_TRUE(TryLockFast())) {
      return;
    }
    LockWithWaiters();
  }
  bool TryLock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION() noexcept {
    return TryLockFast();
  }
  void Unlock() ABSL_UNLOCK_FUNCTION() {
    if (const int32_t state = state_.load(std::memory_order_relaxed);
        state == kLockedWithWaiters) {
      state_.store(kUnlocked, std::memory_order_release);
      futex_wake(&state_);
    } else if (state == kLocked) {
      state_.store(kUnlocked, std::memory_order_release);
    } else if (state == kUnlocked) {
      LOG(FATAL) << "ThreadOnlyMutex::Unlock() called when not locked.";
      ABSL_ASSUME(false);
    } else {
      LOG(FATAL) << "ThreadOnlyMutex corrupted, state_=" << state;
      ABSL_ASSUME(false);
    }
  }

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() noexcept { Lock(); }
  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION() noexcept {
    return TryLock();
  }
  void unlock() ABSL_UNLOCK_FUNCTION() noexcept { Unlock(); }

 private:
  bool TryLockFast() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION() noexcept {
    int expected = kUnlocked;
    return state_.compare_exchange_strong(expected, kLocked,
                                          std::memory_order_acquire);
  }
  void LockWithWaiters() ABSL_EXCLUSIVE_LOCK_FUNCTION() noexcept {
    while (true) {
      if (int32_t expected = kUnlocked; state_.compare_exchange_weak(
              expected, kLockedWithWaiters, std::memory_order_acquire)) {
        return;
      }

      futex_wait(&state_, kLockedWithWaiters);
    }
  }

  std::atomic<int32_t> state_{0};
};

}  // namespace thread

#endif  // THREAD_ON_BOOST_MUTEX_H