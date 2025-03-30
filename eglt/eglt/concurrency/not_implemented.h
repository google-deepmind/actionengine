#ifndef EGLT_CONCURRENCY_BASE_H_
#define EGLT_CONCURRENCY_BASE_H_

#if !defined(__EGLT_CONCURRENCY_IMPLEMENTATION__)

#define __EGLT_CONCURRENCY_IMPLEMENTATION__
#define __EGLT_CONCURRENCY_IMPLEMENTATION_NOT_IMPLEMENTED__

#include <cstddef>
#include <memory>
#include <type_traits>

#include "eglt/absl_headers.h"

namespace eglt::concurrency::impl {
class Case {};

typedef absl::InlinedVector<Case, 4> CaseArray;

class TreeOptions {};

template <typename T>
class ChannelReader {
  // Implementers of this class should ideally mimic google3's thread::Reader.
  // The required behaviours are described in method comments below.
public:
  // Block until either
  // (a) the next item has been read into *item; returns true.
  // (b) Close() has been called on the corresponding writer and
  //     all values have been consumed; returns false.
  //
  // When supported, Read() will move into *item.
  bool Read(T* item [[maybe_unused]]) { return false; }

  // Returns a Case (to pass to Select()) that will finish by either
  // (a) consuming the next item into *item and storing true in *ok, or
  // (b) storing false in *ok to indicate that the channel has been closed and
  //     all values have been consumed.
  //
  // The objects pointed to by both "item" and "ok" must outlive any call to
  // Select using this case.
  //
  // When supported, OnRead() will move into *item.
  Case OnRead(T* item [[maybe_unused]], bool* ok [[maybe_unused]]) {
    return {};
  }
};

template <typename T>
class ChannelWriter {
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
  void Write(const T& item [[maybe_unused]]) {}
  void Write(T&& item [[maybe_unused]]) {}

  bool WriteUnlessCancelled(const T& item [[maybe_unused]]) { return false; }
  bool WriteUnlessCancelled(T&& item [[maybe_unused]]) { return false; }

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read().
  //
  // REQUIRES: May be called at most once.
  void Close();
};

template <typename T>
void ChannelWriter<T>::Close() {
  // Implementation of Close() should notify all waiting readers.
  // This is a placeholder implementation.
  // Actual implementation would involve notifying the ChannelReader.
  // ...
}

template <typename T>
class Channel {
  // Implementers of this class should ideally mimic google3's thread::Channel.
  // The required behaviours are described in method comments below.
public:
  static_assert(std::is_move_assignable_v<T>,
                "Channel<T> requires T to be MoveAssignable.");

  explicit Channel(size_t buffer_size [[maybe_unused]] = 0) {}

  ChannelReader<T>* GetReader() { return nullptr; }
  ChannelWriter<T>* GetWriter() { return nullptr; }

  [[nodiscard]] size_t Size() const { return 0; }
};

class Fiber {
public:
  template <typename F,
            // Avoid binding Fiber(const Fiber&) or Fiber(Fiber):
            typename = std::invoke_result_t<F>>
  explicit Fiber(F f [[maybe_unused]]) {}

  void Cancel();
  Case OnCancel();

  void Join();
  Case OnJoinable();
};

class PermanentEvent {
public:
  void Notify();
  [[nodiscard]] bool HasBeenNotified() const;

  Case OnEvent();
};

using Mutex = absl::Mutex;
using MutexLock = absl::MutexLock;
using CondVar = absl::CondVar;

inline void JoinOptimally(Fiber* fiber) { fiber->Join(); }

bool Cancelled();

Case OnCancel();

int Select(const CaseArray& cases);

int SelectUntil(absl::Time deadline, const CaseArray& cases);

void Detach(const TreeOptions& options, absl::AnyInvocable<void()>&& fn);

// inlined only to avoid ODR violations.
inline std::unique_ptr<Fiber> NewTree(const TreeOptions&,
                                      absl::AnyInvocable<void()>&& fn [[
                                        maybe_unused]]) { return nullptr; }
} // namespace eglt::concurrency::impl

#endif  // !defined(__EGLT_CONCURRENCY_IMPLEMENTATION__)

#endif  // EGLT_CONCURRENCY_BASE_H_
