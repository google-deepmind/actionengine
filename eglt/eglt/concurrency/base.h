#ifndef EGLT_CONCURRENCY_BASE_H_
#define EGLT_CONCURRENCY_BASE_H_

#include <cstddef>
#include <cstdio>
#include <memory>
#include <type_traits>

#include <eglt/absl_headers.h>

namespace eglt::concurrency {

using Callable = absl::AnyInvocable<void()>;

template<typename T, typename Case, typename ConcreteReader>
class BaseChannelReader {
  // Implementers of this class should ideally mimic google3's thread::Reader.
  // The required behaviours are described in method comments below.
 public:
  // Block until either
  // (a) the next item has been read into *item; returns true.
  // (b) Close() has been called on the corresponding writer and
  //     all values have been consumed; returns false.
  //
  // When supported, Read() will move into *item.
  bool Read(T* item);

  // Returns a Case (to pass to Select()) that will finish by either
  // (a) consuming the next item into *item and storing true in *ok, or
  // (b) storing false in *ok to indicate that the channel has been closed and
  //     all values have been consumed.
  //
  // The objects pointed to by both "item" and "ok" must outlive any call to
  // Select using this case.
  //
  // When supported, OnRead() will move into *item.
  Case OnRead(T* item, bool* ok);
};

template<typename T, typename ConcreteWriter>
class BaseChannelWriter {
  // Implementers of this class should ideally mimic google3's thread::Writer.
  // The required behaviours are described in method comments below.
 public:
  // Blocks until the channel is able to accept a value (for a description of
  // buffering see Channel()) and writes "item" to the channel.
  //
  // See thread::SelectUntil in select.h for how to write only if there's space.
  //
  // If "item" is a temporary or the result of calling std::move, it will be
  // move-assigned into the buffer or a waiting reader.
  //
  // REQUIRES: May not be called after this->Close().
  void Write(const T& item);
  void Write(T&& item);

  bool WriteUnlessCancelled(const T& item);
  bool WriteUnlessCancelled(T&& item);

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read().
  //
  // REQUIRES: May be called at most once.
  void Close();
};

template<typename T, typename Case, typename ConcreteChannel,
    typename ConcreteReader, typename ConcreteWriter>
class BaseChannel {
  // Implementers of this class should ideally mimic google3's thread::Channel.
  // The required behaviours are described in method comments below.
 public:
  static_assert(std::is_move_assignable<T>::value,
                "Channel<T> requires T to be MoveAssignable.");

  explicit BaseChannel(size_t buffer_size = 0) {}

  BaseChannelReader<T, Case, ConcreteReader>* GetReader();
  BaseChannelWriter<T, ConcreteWriter>* GetWriter();

  size_t Size() const;
};

template<typename Case, typename ConcreteFiber>
class BaseFiber {
 public:
  template<typename F,
      // Avoid binding Fiber(const Fiber&) or Fiber(Fiber):
      typename = std::invoke_result_t<F>>
  explicit BaseFiber(F&& f);

  void Cancel() {}
  Case OnCancel() { return Case(); }

  void Join() {}
  Case OnJoinable() { return Case(); }

 protected:
  BaseFiber() = default;
};

template<typename Case, typename ConcretePermanentEvent>
class BasePermanentEvent {
 public:
  void Notify();
  bool HasBeenNotified() const;

  Case OnEvent();
};

template<typename ConcreteMutex>
class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED BaseMutex {
 public:
  BaseMutex() = default;
  ~BaseMutex() = default;
};

template<typename ConcreteMutex, typename ConcreteMutexLock>
class ABSL_SCOPED_LOCKABLE BaseMutexLock {
 public:
  explicit BaseMutexLock(BaseMutex<ConcreteMutex>* mu)
  ABSL_EXCLUSIVE_LOCK_FUNCTION(mu) {}
  ~BaseMutexLock() ABSL_UNLOCK_FUNCTION() = default;
};

template<typename ConcreteMutex, typename ConcreteReaderMutexLock>
class ABSL_SCOPED_LOCKABLE BaseReaderMutexLock {
 public:
  explicit BaseReaderMutexLock(BaseMutex<ConcreteMutex>* mu) ABSL_SHARED_LOCK_FUNCTION(
      mu) {}
  ~BaseReaderMutexLock() ABSL_UNLOCK_FUNCTION() = default;
};

template<typename ConcreteMutex, typename ConcreteWriterMutexLock>
class ABSL_SCOPED_LOCKABLE BaseWriterMutexLock {
 public:
  explicit BaseWriterMutexLock(BaseMutex<ConcreteMutex>* mu) ABSL_EXCLUSIVE_LOCK_FUNCTION(
      mu) {}
  ~BaseWriterMutexLock() ABSL_UNLOCK_FUNCTION() = default;
};

#if !defined(__EGLT_CONCURRENCY_IMPLEMENTATION__)

namespace impl {

// dummy types that should not work without a concrete implementation.
class Case {};
typedef absl::InlinedVector<Case, 4> CaseArray;
class TreeOptions {};

template<typename T>
class ChannelReader : public BaseChannelReader<T, Case, ChannelReader<T>> {};

template<typename T>
class ChannelWriter : public BaseChannelWriter<T, ChannelWriter<T>> {};

template<typename T>
class Channel : public BaseChannel<T, Case, Channel<T>, ChannelReader<T>,
                                   ChannelWriter<T>> {
 public:
  explicit Channel(size_t buffer_size) {}
};

class Fiber : public BaseFiber<Case, Fiber> {
 public:
  template<typename F,
      // Avoid binding Fiber(const Fiber&) or Fiber(Fiber):
      typename = std::invoke_result_t<F>>
  explicit Fiber(F&& f) {}
};

class PermanentEvent : public BasePermanentEvent<Case, PermanentEvent> {};

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED Mutex : public BaseMutex<Mutex> {
};

class ABSL_SCOPED_LOCKABLE MutexLock : public BaseMutexLock<Mutex, MutexLock> {
};

class ABSL_SCOPED_LOCKABLE ReaderMutexLock
    : public BaseReaderMutexLock<Mutex, ReaderMutexLock> {
};

class ABSL_SCOPED_LOCKABLE WriterMutexLock
    : public BaseWriterMutexLock<Mutex, WriterMutexLock> {
};

// In some implementations, like in google3, we can perform a more
// efficient join by first selecting on the fiber's joinable event.
// In other implementations, it might be more efficient to just call Join() to
// avoid a context switch. Google3's switches are fast, even for actual threads.
// TODO(helenapankov): google3 thread join actually selects on the joinable
//   event, so the need in this function should be reconsidered.
inline void JoinOptimally(Fiber* fiber) { fiber->Join(); }

bool Cancelled();

Case OnCancel();

int Select(const CaseArray& cases);

int SelectUntil(absl::Time deadline, const CaseArray& cases);

void Detach(const TreeOptions& options, Callable fn);

// inlined only to avoid ODR violations.
inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options, Callable fn) {
  return nullptr;
}

}  // namespace impl

#endif  // !defined(__EGLT_CONCURRENCY_IMPLEMENTATION__)

} // namespace eglt::concurrency


#endif  // EGLT_CONCURRENCY_BASE_H_
