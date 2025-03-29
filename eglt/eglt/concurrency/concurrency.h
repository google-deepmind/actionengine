#ifndef EGLT_CONCURRENCY_CONCURRENCY_H_
#define EGLT_CONCURRENCY_CONCURRENCY_H_

#include <memory>
#include <utility>

#include <eglt/absl_headers.h>
#include <eglt/concurrency/base.h>  // IWYU pragma: export
#include <eglt/concurrency/implementation.h>  // IWYU pragma: export

namespace eglt::concurrency {

using Case = impl::Case;
using CaseArray = impl::CaseArray;
using TreeOptions = impl::TreeOptions;

template<typename T>
using ChannelReader = impl::ChannelReader<T>;

template<typename T>
using ChannelWriter = impl::ChannelWriter<T>;

template<typename T>
using Channel = impl::Channel<T>;

using Fiber = impl::Fiber;
using PermanentEvent = impl::PermanentEvent;

using Mutex = impl::Mutex;
using MutexLock = impl::MutexLock;
using ReaderMutexLock = impl::ReaderMutexLock;
using WriterMutexLock = impl::WriterMutexLock;

// In some implementations, like in google3, we can perform a more
// efficient join by first selecting on the fiber's joinable event.
// In other implementations, it might be more efficient to just call Join() to
// avoid a context switch. Google3's switches are fast, even for actual threads.
inline void JoinOptimally(Fiber* fiber) { impl::JoinOptimally(fiber); }

inline bool Cancelled() { return impl::Cancelled(); }

inline Case OnCancel() { return impl::OnCancel(); }

inline int Select(const CaseArray& cases) { return impl::Select(cases); }

inline int SelectUntil(absl::Time deadline, const CaseArray& cases) {
  return impl::SelectUntil(deadline, cases);
}

inline void Detach(const TreeOptions& options, Callable&& fn) {
  impl::Detach(options, std::move(fn));
}

inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options,
                                      Callable&& fn) {
  return impl::NewTree(options, std::move(fn));
}

} // namespace eglt::concurrency


#endif  // EGLT_CONCURRENCY_CONCURRENCY_H_
