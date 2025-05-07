#ifndef THREAD_ON_BOOST_CONCURRENCY_H_
#define THREAD_ON_BOOST_CONCURRENCY_H_

#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/channel.h"
#include "thread_on_boost/fiber.h"
#include "thread_on_boost/select.h"
#include "thread_on_boost/selectables.h"

namespace eglt::concurrency::impl {

using Mutex = thread::Mutex;
using MutexLock = thread::MutexLock;
using CondVar = thread::CondVar;

using PermanentEvent = thread::PermanentEvent;
using Case = thread::Case;
using CaseArray = thread::CaseArray;
using TreeOptions = thread::TreeOptions;
using Fiber = thread::Fiber;

inline void SleepFor(absl::Duration duration) {
  boost::fibers::context* active_ctx = boost::fibers::context::active();
  active_ctx->wait_until(std::chrono::steady_clock::now() +
                         absl::ToChronoNanoseconds(duration));
}

template <typename T>
using Channel = thread::Channel<T>;

template <typename T>
using ChannelReader = thread::Reader<T>;

template <typename T>
using ChannelWriter = thread::Writer<T>;

inline bool Cancelled() {
  return thread::Cancelled();
}

inline Case OnCancel() {
  return thread::OnCancel();
}

inline int Select(const CaseArray& cases) {
  return thread::Select(cases);
}

inline int SelectUntil(const absl::Time deadline, const CaseArray& cases) {
  return thread::SelectUntil(deadline, cases);
}

inline void Detach(std::unique_ptr<thread::Fiber> fiber) {
  thread::Detach(std::move(fiber));
}

inline void Detach(const TreeOptions& options,
                   absl::AnyInvocable<void()>&& fn) {
  thread::Detach(options, std::move(fn));
}

inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options,
                                      absl::AnyInvocable<void()>&& fn) {
  return thread::NewTree(options, std::move(fn));
}

inline Case NonSelectableCase() {
  return thread::NonSelectableCase();
}

inline Case AlwaysSelectableCase() {
  return thread::AlwaysSelectableCase();
}

}  // namespace eglt::concurrency::impl

#endif  // THREAD_ON_BOOST_CONCURRENCY_H_
