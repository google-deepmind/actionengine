// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef THREAD_FIBER_CHANNEL_H_
#define THREAD_FIBER_CHANNEL_H_

#include "g3fiber/boost_primitives.h"
#include "g3fiber/cases.h"
#include "g3fiber/channel/waiter_state.h"
#include "g3fiber/fiber.h"
#include "g3fiber/select.h"

namespace thread::internal {

enum class CopyOrMove {
  Copy,
  Move,
};

static constexpr auto kCopy = CopyOrMove::Copy;
static constexpr auto kMove = CopyOrMove::Move;

template <typename T>
T CopyOrMoveOut(T* absl_nonnull item, CopyOrMove strategy) {
  if (strategy == kCopy) {
    return *static_cast<const T*>(item);
  }

  if (strategy == kMove) {
    return std::move(*item);
  }

  LOG(FATAL) << "Invalid CopyOrMove strategy: " << static_cast<int>(strategy);
  ABSL_ASSUME(false);
}

}  // namespace thread::internal

namespace thread {
template <class T>
requires std::is_move_assignable_v<T> class Channel;
}

namespace thread::internal {
template <typename T>
struct ReadSelectable final : Selectable {
  explicit ReadSelectable(Channel<T>* absl_nonnull channel)
      : channel(channel) {}

  bool Handle(CaseInSelectClause* absl_nonnull reader, bool enqueue) override;
  void Unregister(CaseInSelectClause* absl_nonnull c) override;

  Channel<T>* absl_nonnull channel;
};

template <typename T>
struct WriteSelectable final : Selectable {
  explicit WriteSelectable(Channel<T>* absl_nonnull channel)
      : channel(channel) {}

  bool Handle(CaseInSelectClause* absl_nonnull writer, bool enqueue) override;
  void Unregister(CaseInSelectClause* absl_nonnull c) override;

  Channel<T>* absl_nonnull channel;
};
}  // namespace thread::internal

namespace thread {

/** @brief
 *    A Reader is used to read items from a Channel.
 *
 *  You can get a pointer to a Reader by calling Channel<T>::reader(), which is
 *  the object that you should pass to code that needs to read from the channel.
 */
template <class T>
class Reader {
 public:
  // This class is not copyable or movable.
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  /** @brief
   *    Reads an item from the channel, blocking until it can be read.
   *    Returns false if and only if the channel is closed.
   *
   *  On returning true, the item is moved into the provided pointer.
   *
   *  @return
   *    True if an item was read from the channel, false if the channel was
   *    closed before or while reading.
   */
  bool Read(T* absl_nonnull item);

  /** @brief
   *    Returns a Case that can be used to wait for and synchronise on reading
   *    an item from the channel.
   *
   *  @note
   *    The item is only read out of the channel if the returned Case is
   *    selected. For example:
   *    @code{.cc}
   *    // `reader` is a thread::Reader<T> pointer from a channel
   *    // with non-empty strings
   *
   *    std::string item;
   *    bool ok;
   *
   *    const int selected = thread::Select({
   *        thread::OnCancel(),
   *        reader->OnRead(&item, &ok),
   *    });
   *
   *    if (selected == 0) {
   *      assert(!ok);
   *      assert(item.empty());
   *    }
   *    @endcode
   *
   *  @return
   *    True if an item was read from the channel, false if the channel was
   *    closed before or while reading.
   */
  Case OnRead(T* absl_nonnull item, bool* absl_nonnull ok);

 private:
  friend class Channel<T>;
  explicit Reader(Channel<T>* absl_nonnull channel);

  Channel<T>* absl_nonnull channel_;
};

/** @brief
 *    A Writer is used to write items to a Channel.
 *
 *  You can get a pointer to a Writer by calling Channel<T>::writer(), which
 *  is the object that you should pass to code that needs to write to the
 *  channel. Writers must be closed by calling Writer::Close() when the
 *  communication is done, to notify any waiting readers that no more items
 *  will be written to the channel.
 */
template <class T>
class Writer {
 public:
  // This class is not copyable or movable.
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  /** @brief
   *    Writes the given item to the channel, blocking until it can be written.
   *    If the channel is closed, this will throw an exception.
   *
   *  @note
   *    The item is copied into the channel, so it must be copy-constructible.
   */
  void Write(const T& item);

  /** @brief
   *    Writes the given item to the channel, blocking until it can be written.
   *    If the channel is closed, this will throw an exception.
   *
   *  @note
   *    The item is moved into the channel, so it must be move-constructible.
   */
  void Write(T&& item);

  /** @brief
   *    Marks the channel as closed, notifying any waiting readers. See
   *    Reader::Read(). May be called at most once.
   */
  void Close();

  /** @brief
   *    Returns a Case that can be used to write the given item to the channel.
   *
   *  @note
   *    The lifetime of the item must be guaranteed to span the call to
   *    Select() with this case. Care must be taken when passing temporaries
   *    whose lifetime may only be guaranteed for the current statement.
   *    For example:
   *
   *     @code{.cc}
   *     // Legal: int(23)'s lifetime extends to end of statement.
   *     int index = thread::Select({ c_0, c_1, writer->OnWrite(23) });
   *
   *     // Illegal: Lifetime of int(19) not guaranteed beyond c's
   *     // declaration.
   *     thread::Case c = writer->OnWrite(19);
   *     int index = thread::Select({ c, ... });
   *     @endcode
   *
   * @param item
   *   The item to write to the channel. If the case is selected, ownership of
   *   the item is moved to the channel.
   */
  Case OnWrite(const T& item);

  /** @brief
   *    Returns a Case that can be used to write the given item to the channel.
   *    The item is not mutated (moved from) unless the returned Case is
   *    selected.
   *
   *  @note
   *    The \p item is not mutated (moved from) unless the returned Case is
   *    selected. For example:
   *
   *     @code{.cc}
   *     // This is safe, ownership is only moved from if the case is selected.
   *     auto unique_ptr = absl::make_unique<T>();
   *     int index = thread::Select({ c_0, c_1,
   *                                  writer->OnWrite(std::move(unique_ptr)) });
   *     if (index != 2) {
   *       unique_ptr->Foo();
   *     }
   *     @endcode
   *
   * @param item
   *   The item to write to the channel. If the case is selected, ownership of
   *   the item is moved to the channel.
   */
  Case OnWrite(T&& item);

  /** @brief
   *    Returns false iff the calling fiber is cancelled before the value can be
   *    written.
   *
   *  @param item
   *    The item to write to the channel.
   *
   *  @return
   *    True if the item was written to the channel, false if the calling fiber
   *    was cancelled before the item could be written.
   */
  bool WriteUnlessCancelled(const T& item);

  /** @brief
   *    Returns false iff the calling fiber is cancelled before the value can be
   *    written.
   *
   *  @note
   *    The item is only moved into the channel if the function returns true.
   *
   *  @param item
   *    The item to write to the channel. If the function returns true, the
   *    ownership of the item is moved to the channel.
   *
   *  @return
   *    True if the item was written to the channel, false if the calling fiber
   *    was cancelled before the item could be written.
   */
  bool WriteUnlessCancelled(T&& item);

 private:
  friend class Channel<T>;
  explicit Writer(Channel<T>* absl_nonnull channel_state);

  Channel<T>* absl_nonnull channel_;
};

/** @brief
 *    A Channel is a bounded FIFO queue that allows multiple readers and writers
 *    to communicate with each other.
 *
 *  The channel has a fixed capacity, and will block writers if the channel is
 *  full, and readers if the channel is empty. If the channel is closed, all
 *  waiting readers will be notified that no more items will be written to the
 *  channel.
 */
template <typename T>
requires std::is_move_assignable_v<T> class Channel {
 public:
  /** @brief
   *    Constructs a Channel with the given capacity.
   *
   *  @param capacity
   *    The maximum number of items that can be buffered in the channel. If the
   *    channel is full, writers will block until space is available.
   */
  explicit Channel(size_t capacity)
      : capacity_(capacity), closed_(false), rd_(this), wr_(this) {
    DCHECK(Invariants());
  }

  // This class is not copyable or movable.
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  ~Channel() {
    // Ensure exclusive access (to e.g. prevent concurrent Close()).
    act::concurrency::impl::MutexLock lock(&mu_);
    DCHECK(Invariants());
  }

  /**
   * Returns a Reader that can be used to read items from the channel.
   *
   * Multiple consumers can read from the channel concurrently, and every
   * item written to the channel will be read by exactly one reader.
   *
   * @return
   *   A Reader that can be used to read items from the channel.
   */
  Reader<T>* absl_nonnull reader() { return &reader_; }

  /**
   * Returns a Writer that can be used to write items to the channel.
   *
   * Multiple producers can write to the channel concurrently, and every
   * item written to the channel, if read at all, will be read by exactly one
   * reader. If the channel is at capacity, writers will block until space is
   * available in the channel.
   *
   * @return
   *   A Writer that can be used to write items to the channel.
   */
  Writer<T>* absl_nonnull writer() { return &writer_; }

  /**
   * Returns the instantaneous length of the channel.
   *
   * This method should never be used to determine whether the channel is full
   * or empty, as it may return a value that is not consistent with the state
   * of the channel at the time of the call.
   */
  [[nodiscard]] size_t length() const { return Length(); }

 private:
  friend struct internal::ReadSelectable<T>;
  friend struct internal::WriteSelectable<T>;
  friend class Reader<T>;
  friend class Writer<T>;

  void Close();

  bool Get(T* absl_nonnull dst);

  size_t Length() const {
    act::concurrency::impl::MutexLock lock(&mu_);
    return queue_.size();
  }

  Case OnRead(T* absl_nonnull dst, bool* absl_nonnull ok) {
    return {&rd_, dst, ok};
  }

  Case OnWrite(const T& item) requires(std::is_copy_constructible_v<T>) {
    return {&wr_, &item, &internal::kCopy};
  }

  Case OnWrite(T&& item) { return {&wr_, &item, &internal::kMove}; }

  const size_t capacity_;

  mutable act::concurrency::impl::Mutex mu_;
  std::deque<T> queue_ ABSL_GUARDED_BY(mu_);
  bool closed_ ABSL_GUARDED_BY(mu_);

  internal::ChannelWaiterState waiters_;

  Reader<T> reader_{this};
  internal::ReadSelectable<T> rd_;

  Writer<T> writer_{this};
  internal::WriteSelectable<T> wr_;

  bool Invariants() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    CHECK_LE(queue_.size(), capacity_) << "Channel queue size exceeds capacity";
    return true;
  }
};

template <typename T>
requires std::is_move_assignable_v<T> void Channel<T>::Close() {
  act::concurrency::impl::MutexLock lock(&mu_);
  DCHECK(Invariants());
  CHECK(!closed_) << "Calling Close() on closed channel";
  CHECK(waiters_.writers == nullptr)
      << "Calling Close() on a channel with blocked writers";
  closed_ = true;
  this->waiters_.CloseAndReleaseReaders();
  DCHECK(Invariants());
}

template <typename T>
requires std::is_move_assignable_v<T> bool Channel<T>::Get(
    T* absl_nonnull dst) {
  bool result;
  Select({OnRead(dst, &result)});
  return result;
}

template <typename T>
Reader<T>::Reader(Channel<T>* absl_nonnull channel) : channel_(channel) {}

template <typename T>
bool Reader<T>::Read(T* absl_nonnull item) {
  return channel_->Get(item);
}

template <typename T>
Case Reader<T>::OnRead(T* absl_nonnull item, bool* absl_nonnull ok) {
  return channel_->OnRead(item, ok);
}

template <typename T>
Writer<T>::Writer(Channel<T>* absl_nonnull channel_state)
    : channel_(channel_state) {}

template <typename T>
void Writer<T>::Write(const T& item) {
  Select({OnWrite(item)});
}

template <typename T>
void Writer<T>::Write(T&& item) {
  Select({OnWrite(std::move(item))});
}

template <typename T>
void Writer<T>::Close() {
  channel_->Close();
}

template <typename T>
Case Writer<T>::OnWrite(const T& item) {
  return channel_->OnWrite(item);
}

template <typename T>
Case Writer<T>::OnWrite(T&& item) {
  return channel_->OnWrite(std::move(item));
}

template <typename T>
bool Writer<T>::WriteUnlessCancelled(const T& item) {
  return !Cancelled() && Select({OnCancel(), OnWrite(item)}) == 1;
}

template <typename T>
bool Writer<T>::WriteUnlessCancelled(T&& item) {
  return !Cancelled() && Select({OnCancel(), OnWrite(std::move(item))}) == 1;
}

}  // namespace thread

namespace thread::internal {

template <typename T>
bool ReadSelectable<T>::Handle(CaseInSelectClause* absl_nonnull reader,
                               bool enqueue) {
  act::concurrency::impl::MutexLock lock(&channel->mu_);
  DCHECK(channel->Invariants());

  T* absl_nonnull dst_item = reader->GetCase()->GetArgPtr<T>(0);
  bool* absl_nonnull dst_ok = reader->GetCase()->GetArgPtr<bool>(1);

  // Is there a buffered item to read?
  if (!channel->queue_.empty()) {
    DVLOG(2) << "Get from buffer";
    reader->selector->mu.Lock();
    if (reader->selector->picked_case_index == Selector::kNonePicked) {
      // Move out of the buffer. Explicitly destruct behind for types that don't
      // have a move-assignment operator and where it may be harmful to leave
      // around a copy. (For example, a shared_ptr-like object with only a copy
      // assignment operator.)
      *dst_item = std::move(channel->queue_.front());
      channel->queue_.pop_front();
      *dst_ok = true;
      channel->waiters_.UnlockAndReleaseReader(reader);

      // Potentially admit a waiting writer.
      if (CaseInSelectClause * absl_nonnull unblocked_writer;
          channel->waiters_.GetWaitingWriter(&unblocked_writer)) {
        auto* absl_nonnull item = unblocked_writer->GetCase()->GetArgPtr<T>(0);
        auto copy_or_move =
            *unblocked_writer->GetCase()->GetArgPtr<CopyOrMove>(1);

        channel->queue_.push_back(CopyOrMoveOut(item, copy_or_move));
        channel->waiters_.UnlockAndReleaseWriter(unblocked_writer);
      }
    } else {
      // While we weren't technically able to proceed, there's no point in
      // Select() processing further cases, so we'll still return true below.
      reader->selector->mu.Unlock();
    }
    DCHECK(channel->Invariants());
    return true;
  }

  // Try to transfer directly from waiting writer to reader
  if (CaseInSelectClause * absl_nonnull writer;
      channel->waiters_.GetMatchingWriter(reader, &writer)) {
    auto* absl_nonnull item = writer->GetCase()->GetArgPtr<T>(0);
    auto copy_or_move = *writer->GetCase()->GetArgPtr<CopyOrMove>(1);

    *dst_item = CopyOrMoveOut(item, copy_or_move);
    *dst_ok = true;

    channel->waiters_.UnlockAndReleaseReader(reader);
    channel->waiters_.UnlockAndReleaseWriter(writer);
    DCHECK(channel->Invariants());
  }

  reader->selector->mu.Lock();
  // We must guarantee that this case is eligible to proceed before any
  // side effects can occur.
  if (reader->selector->picked_case_index != Selector::kNonePicked) {
    reader->selector->mu.Unlock();
    // Already handled item
    DVLOG(2) << "Read cancelled since another selector case done";
    DCHECK(channel->Invariants());
    return true;
  }

  if (channel->closed_) {
    DVLOG(2) << "Read failing because channel closed";
    *dst_ok = false;
    channel->waiters_.UnlockAndReleaseReader(reader);
    return true;
  }

  if (enqueue) {
    // Register with waiting readers
    DVLOG(2) << "Read waiting";
    internal::PushBack(&channel->waiters_.readers, reader);
  }

  reader->selector->mu.Unlock();
  DCHECK(channel->Invariants());
  return false;
}

template <typename T>
void ReadSelectable<T>::Unregister(CaseInSelectClause* absl_nonnull c) {
  act::concurrency::impl::MutexLock lock(&channel->mu_);
  internal::UnlinkFromList(&channel->waiters_.readers, c);
}

template <typename T>
bool WriteSelectable<T>::Handle(CaseInSelectClause* absl_nonnull writer,
                                bool enqueue) {
  act::concurrency::impl::MutexLock lock(&channel->mu_);
  DCHECK(channel->Invariants());
  CHECK(!channel->closed_) << "Calling Write() on closed channel";

  // First try to transfer directly from writer to a waiting reader
  if (CaseInSelectClause * absl_nonnull reader;
      channel->waiters_.GetMatchingReader(writer, &reader)) {
    auto* absl_nonnull writer_item = writer->GetCase()->GetArgPtr<T>(0);
    auto copy_or_move = *writer->GetCase()->GetArgPtr<CopyOrMove>(1);

    auto* absl_nonnull reader_item = reader->GetCase()->GetArgPtr<T>(0);
    bool* absl_nonnull reader_ok = reader->GetCase()->GetArgPtr<bool>(1);

    *reader_item = CopyOrMoveOut(writer_item, copy_or_move);
    *reader_ok = true;

    channel->waiters_.UnlockAndReleaseReader(reader);
    channel->waiters_.UnlockAndReleaseWriter(writer);

    DCHECK(channel->Invariants());
    return true;
  }

  writer->selector->mu.Lock();
  // We must guarantee that this case is eligible to proceed before any
  // side effects can occur.
  if (writer->selector->picked_case_index != Selector::kNonePicked) {
    writer->selector->mu.Unlock();
    // Already handled item
    DVLOG(2) << "Write cancelled since another selector case done";
    DCHECK(channel->Invariants());
    return true;
  }

  // Is there room to buffer item?
  if (channel->queue_.size() < channel->capacity_) {
    DVLOG(2) << "Add to buffer";

    T* absl_nonnull item = writer->GetCase()->GetArgPtr<T>(0);
    const CopyOrMove copy_or_move =
        *writer->GetCase()->GetArgPtr<CopyOrMove>(1);

    channel->queue_.push_back(CopyOrMoveOut(item, copy_or_move));
    channel->waiters_.UnlockAndReleaseWriter(writer);

    DCHECK(channel->Invariants());
    return true;
  }

  if (enqueue) {
    // Register with waiting writers
    DVLOG(2) << "Write waiting";
    internal::PushBack(&channel->waiters_.writers, writer);
  }

  writer->selector->mu.Unlock();
  DCHECK(channel->Invariants());
  return false;
}

template <typename T>
void WriteSelectable<T>::Unregister(CaseInSelectClause* absl_nonnull c) {
  act::concurrency::impl::MutexLock lock(&channel->mu_);
  internal::UnlinkFromList(&channel->waiters_.writers, c);
}
}  // namespace thread::internal

#endif  // THREAD_FIBER_CHANNEL_H_