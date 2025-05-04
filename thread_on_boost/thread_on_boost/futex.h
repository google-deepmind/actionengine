#ifndef THREAD_ON_BOOST_FUTEX_H
#define THREAD_ON_BOOST_FUTEX_H

#include <atomic>

#include <boost/predef.h>
#include <boost/config.hpp>

#if BOOST_OS_MACOS
#include <Availability.h>  // IWYU pragma: keep
#define OS_SYNC_WAIT_ON_ADDRESS_AVAILABLE \
  __MAC_OS_X_VERSION_MIN_REQUIRED >= MAC_OS_VERSION_14_4
#endif

#if BOOST_OS_LINUX
extern "C" {
#include <linux/futex.h>
#include <sys/syscall.h>
}
#elif BOOST_OS_BSD_OPEN
extern "C" {
#include <sys/futex.h>
}
#elif BOOST_OS_WINDOWS
#include <windows.h>
#elif (BOOST_OS_MACOS && OS_SYNC_WAIT_ON_ADDRESS_AVAILABLE)
#include <os/os_sync_wait_on_address.h>
#endif

#ifndef SYS_futex
#define SYS_futex SYS_futex_time64
#endif

namespace thread {
#if BOOST_OS_LINUX || BOOST_OS_BSD_OPEN

inline int sys_futex(void* addr, std::int32_t op, std::int32_t x) {
#if BOOST_OS_BSD_OPEN
  return ::futex(static_cast<volatile uint32_t*>(addr), static_cast<int>(op), x,
                 nullptr, nullptr);
#else
  return ::syscall(SYS_futex, addr, op, x, nullptr, nullptr, 0);
#endif
}

inline int futex_wake(std::atomic<std::int32_t>* addr) {
  return 0 <= sys_futex(static_cast<void*>(addr), FUTEX_WAKE_PRIVATE, 1) ? 0
                                                                         : -1;
}

inline int futex_wait(std::atomic<std::int32_t>* addr, std::int32_t x) {
  return 0 <= sys_futex(static_cast<void*>(addr), FUTEX_WAIT_PRIVATE, x) ? 0
                                                                         : -1;
}
#elif BOOST_OS_WINDOWS

inline int futex_wake(std::atomic<std::int32_t>* addr) {
  ::WakeByAddressSingle(static_cast<void*>(addr));
  return 0;
}

inline int futex_wait(std::atomic<std::int32_t>* addr, std::int32_t x) {
  ::WaitOnAddress(static_cast<volatile void*>(addr), &x, sizeof(x), INFINITE);
  return 0;
}
#elif (BOOST_OS_MACOS && OS_SYNC_WAIT_ON_ADDRESS_AVAILABLE)

inline int futex_wake(std::atomic<std::int32_t>* addr) {
  return os_sync_wake_by_address_any(addr, sizeof(std::int32_t),
                                     OS_SYNC_WAKE_BY_ADDRESS_NONE);
}

inline int futex_wait(std::atomic<std::int32_t>* addr, std::int32_t x) {
  return os_sync_wait_on_address(addr, x, sizeof(std::int32_t),
                                 OS_SYNC_WAIT_ON_ADDRESS_NONE);
}
#else
#warn "no futex support on this platform"
#endif
}  // namespace thread

#endif  // THREAD_ON_BOOST_FUTEX_H
