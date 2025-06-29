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

#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/cases.h"
#include "thread_on_boost/channel/waiter_state.h"
#include "thread_on_boost/fiber.h"
#include "thread_on_boost/select.h"

namespace thread::internal {

enum class CopyOrMove {
  Copy,
  Move,
};

static constexpr auto kCopy = CopyOrMove::Copy;
static constexpr auto kMove = CopyOrMove::Move;

template <typename T>
T CopyOrMoveOut(T* item, CopyOrMove strategy) {
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

  bool Handle(PerSelectCaseState* reader, bool enqueue) override;
  void Unregister(PerSelectCaseState* c) override;

  Channel<T>* channel;
};

template <typename T>
struct WriteSelectable final : Selectable {
  explicit WriteSelectable(Channel<T>* absl_nonnull channel)
      : channel(channel) {}

  bool Handle(PerSelectCaseState* writer, bool enqueue) override;
  void Unregister(PerSelectCaseState* c) override;

  Channel<T>* channel;
};
}  // namespace thread::internal

namespace thread {

template <class T>
class Reader {
 public:
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  bool Read(T* absl_nonnull item);
  Case OnRead(T* absl_nonnull item, bool* absl_nonnull ok);

 private:
  friend class Channel<T>;
  explicit Reader(Channel<T>* absl_nonnull channel);

  Channel<T>* absl_nonnull channel_;
};

template <class T>
class Writer {
 public:
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  void Write(const T& item);
  void Write(T&& item);

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read(). May be called at most once.
  void Close();

  // CAUTION: The lifetime of "item" must be guaranteed to span any call to
  // Select() with this case. Care must be taken when passing temporaries, whose
  // lifetime may only be guaranteed for the current statement. For example:
  //
  //   // Legal: int(23)'s lifetime extends to end of statement.
  //   int index = thread::Select({ c_0, c_1, writer->OnWrite(23) });
  //   // Illegal: Lifetime of int(19) not guaranteed beyond c's declaration.
  //   thread::Case c = writer->OnWrite(19);
  //   int index = thread::Select({ c, ... });
  //
  Case OnWrite(const T& item);

  // NOTE: "item" is not mutated (moved from) unless the returned Case is
  // selected. E.g.:
  //   // This is safe, ownership is only moved from if the case is selected.
  //   auto unique_ptr = absl::make_unique<T>();
  //   int index = thread::Select({ c_0, c_1,
  //                                writer->OnWrite(std::move(unique_ptr)) });
  //   if (index != 2) {
  //     unique_ptr->Foo();
  //   }
  Case OnWrite(T&& item);

  // Returns false iff the calling fiber is cancelled before the value can be
  // written.
  bool WriteUnlessCancelled(const T& item);

  // NOTE: item is only moved from if this function returns true.
  bool WriteUnlessCancelled(T&& item);

 private:
  friend class Channel<T>;
  explicit Writer(Channel<T>* absl_nonnull channel_state);

  Channel<T>* absl_nonnull channel_;
};

template <typename T>
requires std::is_move_assignable_v<T> class Channel {
 public:
  explicit Channel(size_t capacity)
      : capacity_(capacity), closed_(false), rd_(this), wr_(this) {
    DCHECK(Invariants());
  }

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  ~Channel() {
    // Ensure exclusive access (to e.g. prevent concurrent Close()).
    eglt::concurrency::impl::MutexLock lock(&mu_);
    DCHECK(Invariants());
  }

  Reader<T>* reader() { return &reader_; }
  Writer<T>* writer() { return &writer_; }

  [[nodiscard]] size_t length() const { return Length(); }

 private:
  friend struct internal::ReadSelectable<T>;
  friend struct internal::WriteSelectable<T>;
  friend class Reader<T>;
  friend class Writer<T>;

  void Close();

  bool Get(T* absl_nonnull dst);

  size_t Length() const {
    eglt::concurrency::impl::MutexLock l(&mu_);
    return queue_.size();
  }

  Case OnRead(T* dst, bool* ok) { return {&rd_, dst, ok}; }

  Case OnWrite(const T& item) requires(std::is_copy_constructible_v<T>) {
    return {&wr_, &item, &internal::kCopy};
  }
  Case OnWrite(T&& item) { return {&wr_, &item, &internal::kMove}; }

  const size_t capacity_;

  mutable eglt::concurrency::impl::Mutex mu_;
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
  eglt::concurrency::impl::MutexLock lock(&mu_);
  DCHECK(Invariants());
  CHECK(!closed_) << "Calling Close() on closed channel";
  CHECK(waiters_.writers == nullptr)
      << "Calling Close() on a channel with blocked writers";
  closed_ = true;
  this->waiters_.CloseAndReleaseReaders();
  DCHECK(Invariants());
}

template <typename T>
requires std::is_move_assignable_v<T> bool Channel<T>::Get(T* dst) {
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
bool ReadSelectable<T>::Handle(PerSelectCaseState* reader, bool enqueue) {
  eglt::concurrency::impl::MutexLock lock(&channel->mu_);
  DCHECK(channel->Invariants());

  T* dst_item = reader->GetCase()->GetArgPtr<T>(0);
  bool* dst_ok = reader->GetCase()->GetArgPtr<bool>(1);

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
      if (PerSelectCaseState * unblocked_writer;
          channel->waiters_.GetWaitingWriter(&unblocked_writer)) {
        auto* item = unblocked_writer->GetCase()->GetArgPtr<T>(0);
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
  if (PerSelectCaseState * writer;
      channel->waiters_.GetMatchingWriter(reader, &writer)) {
    auto* item = writer->GetCase()->GetArgPtr<T>(0);
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
void ReadSelectable<T>::Unregister(PerSelectCaseState* c) {
  eglt::concurrency::impl::MutexLock l(&channel->mu_);
  internal::UnlinkFromList(&channel->waiters_.readers, c);
}

template <typename T>
bool WriteSelectable<T>::Handle(PerSelectCaseState* writer, bool enqueue) {
  eglt::concurrency::impl::MutexLock l(&channel->mu_);
  DCHECK(channel->Invariants());
  CHECK(!channel->closed_) << "Calling Write() on closed channel";

  // First try to transfer directly from writer to a waiting reader
  if (PerSelectCaseState * reader;
      channel->waiters_.GetMatchingReader(writer, &reader)) {
    auto* writer_item = writer->GetCase()->GetArgPtr<T>(0);
    auto copy_or_move = *writer->GetCase()->GetArgPtr<CopyOrMove>(1);

    auto* reader_item = reader->GetCase()->GetArgPtr<T>(0);
    bool* reader_ok = reader->GetCase()->GetArgPtr<bool>(1);

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

    T* item = writer->GetCase()->GetArgPtr<T>(0);
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
void WriteSelectable<T>::Unregister(PerSelectCaseState* c) {
  eglt::concurrency::impl::MutexLock l(&channel->mu_);
  internal::UnlinkFromList(&channel->waiters_.writers, c);
}
}  // namespace thread::internal

#endif  // THREAD_FIBER_CHANNEL_H_