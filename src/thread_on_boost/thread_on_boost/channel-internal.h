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

#ifndef THREAD_FIBER_CHANNEL_INTERNAL_H_
#define THREAD_FIBER_CHANNEL_INTERNAL_H_

#include <cstdint>
#include <deque>
#include <memory>
#include <type_traits>
#include <vector>

#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/select-internal.h"
#include "thread_on_boost/select.h"

namespace thread::internal {

// Type-independent channel implementation.
struct ChannelWaiterState {
  // NOTE: Caller is responsible for synchronizing all access to methods below
  // via ChannelState::mu_

  // Attempt to find an eligible "reader" to be paired with "writer".  Or,
  // a "writer" to be paired with the passed "reader", respectively.
  //
  // Returns true, and updates *reader (or *writer), if an eligible waiter
  // exists. The reader and writer both returned with selector mutexes held and
  // are guaranteed to be pickable.
  // Returns false with no side effects otherwise.
  // REQUIRES: reader != nullptr, writer != nullptr
  bool GetMatchingReader(CaseState* writer, CaseState** reader) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, (*reader)->sel->mu,
                                      writer->sel->mu);
  bool GetMatchingWriter(CaseState* reader, CaseState** writer) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, reader->sel->mu,
                                      (*writer)->sel->mu);

  // Attempt to find an eligible queued writer.  There is no matching reader in
  // this case, it is used for when space becomes available in the queue due to
  // a read completing, allowing a writer to complete without partner.
  //
  // Returns true, and updates *writer, if a suitable waiter exists.  *writer is
  // returned with selector mutex held and guaranteed pickable.
  // Returns false with no side effects otherwise.
  bool GetWaitingWriter(CaseState** writer) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, (*writer)->sel->mu);

  // Unlock (and mark selected) the passed reader/writer respectively.
  // REQUIRES: selector mutex is held, picked == kNonePicked
  void UnlockAndReleaseReader(CaseState* reader)
      ABSL_UNLOCK_FUNCTION(reader->sel->mu);
  void UnlockAndReleaseWriter(CaseState* writer)
      ABSL_UNLOCK_FUNCTION(writer->sel->mu);

  // Releases all waiting readers.  Unselected readers are picked and marked to
  // return that this channel was closed.
  void CloseAndReleaseReaders();

  internal::CaseState* waiting_readers_ = nullptr;
  internal::CaseState* waiting_writers_ = nullptr;
};

// Type-dependent channel implementation.
template <typename T>
class ChannelState final : public ChannelWaiterState {
 public:
  explicit ChannelState(size_t capacity)
      : capacity_(capacity), closed_(false), rd_(this), wr_(this) {
    DCHECK(Invariants());
  }

  // This type is neither copyable nor movable.
  ChannelState(const ChannelState&) = delete;
  ChannelState& operator=(const ChannelState&) = delete;

  ~ChannelState();

  void Close() {
    eglt::concurrency::impl::MutexLock l(&mu_);
    DCHECK(Invariants());
    CHECK(!closed_) << "Calling Close() on closed channel";
    CHECK(waiting_writers_ == nullptr)
        << "Calling Close() on a channel with blocked writers";
    closed_ = true;
    this->CloseAndReleaseReaders();
    DCHECK(Invariants());
  }

  bool Get(T* dst) {
    bool result;
    Select({OnRead(dst, &result)});
    return result;
  }

  size_t Length() const {
    eglt::concurrency::impl::MutexLock l(&mu_);
    return queue_.size();
  }

  Case OnRead(T* dst, bool* ok) {
    const Case c = {
        &rd_,
        reinterpret_cast<intptr_t>(dst),
        reinterpret_cast<intptr_t>(ok),
    };
    return c;
  }

  Case OnWrite(const T& item) {
    // Guard against user error at compile-time.
    static_assert(
        std::is_copy_constructible_v<T>,
        "Channel<T>::OnWrite called with const T& for a type T that is not "
        "copy constructible.");
    return {&wr_, reinterpret_cast<intptr_t>(&item),
            reinterpret_cast<intptr_t>(&CopyOut)};
  }

  Case OnWrite(T&& item) {
    return {&wr_, reinterpret_cast<intptr_t>(&item),
            reinterpret_cast<intptr_t>(&MoveOut)};
  }

 private:
  const size_t capacity_;  // User-supplied channel buffer size

  mutable eglt::concurrency::impl::Mutex mu_;
  // chunked_queue doesn't work with over aligned types, so we use std::deque
  // for those.
  std::deque<T> queue_ ABSL_GUARDED_BY(mu_);
  bool closed_ ABSL_GUARDED_BY(mu_);

  struct Rd final : public Selectable {
    ChannelState* state;
    explicit Rd(ChannelState* s) : state(s) {}
    bool Handle(CaseState* reader, bool enqueue) override;
    void Unregister(CaseState* c) override {
      eglt::concurrency::impl::MutexLock l(&state->mu_);
      internal::RemoveFromList(&state->waiting_readers_, c);
    }
  };
  Rd rd_;

  struct Wr final : public Selectable {
    ChannelState* state;
    explicit Wr(ChannelState* s) : state(s) {}
    bool Handle(CaseState* writer, bool enqueue) override;
    void Unregister(CaseState* c) override {
      eglt::concurrency::impl::MutexLock l(&state->mu_);
      internal::RemoveFromList(&state->waiting_writers_, c);
    }
  };
  Wr wr_;

  static T CopyOut(T* item) { return *static_cast<const T*>(item); }

  static T MoveOut(T* item) { return std::move(*item); }

  static T CopyOrMoveOut(T* item, intptr_t arg2) {
    return reinterpret_cast<T (*)(T*)>(arg2)(item);
  }

  T* Front() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) { return &queue_.front(); }

  bool Invariants() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
};

template <typename T>
ChannelState<T>::~ChannelState() {
  eglt::concurrency::impl::MutexLock l(
      &mu_);  // Must synchronize with remote operations (e.g. Close()).
  DCHECK(Invariants());
}

template <typename T>
bool ChannelState<T>::Rd::Handle(CaseState* reader, bool enqueue) {
  ChannelState* ch = state;
  eglt::concurrency::impl::MutexLock l(&ch->mu_);
  DCHECK(ch->Invariants());

  T* dst_item = reinterpret_cast<T*>(reader->params->arg1);
  bool* dst_ok = reinterpret_cast<bool*>(reader->params->arg2);

  // Is there a buffered item to read?
  if (!ch->queue_.empty()) {
    DVLOG(2) << "Get from buffer";
    reader->sel->mu.Lock();
    if (reader->sel->picked == Selector::kNonePicked) {
      // Move out of the buffer. Explicitly destruct behind for types that don't
      // have a move-assignment operator and where it may be harmful to leave
      // around a copy. (For example, a shared_ptr-like object with only a copy
      // assignment operator.)
      *dst_item = std::move(ch->queue_.front());
      ch->queue_.pop_front();
      *dst_ok = true;
      ch->UnlockAndReleaseReader(reader);

      // Potentially admit a waiting writer.
      CaseState* unblocked_writer;
      if (ch->GetWaitingWriter(&unblocked_writer)) {
        T* item = reinterpret_cast<T*>(unblocked_writer->params->arg1);
        ch->queue_.push_back(
            CopyOrMoveOut(item, unblocked_writer->params->arg2));
        ch->UnlockAndReleaseWriter(unblocked_writer);
      }
    } else {
      // While we weren't technically able to proceed, there's no point in
      // Select() processing further cases, so we'll still return true below.
      reader->sel->mu.Unlock();
    }
    DCHECK(ch->Invariants());
    return true;
  }

  // Try to transfer directly from waiting writer to reader
  CaseState* writer;
  if (ch->GetMatchingWriter(reader, &writer)) {
    T* item = reinterpret_cast<T*>(writer->params->arg1);
    *dst_item = CopyOrMoveOut(item, writer->params->arg2);
    *dst_ok = true;
    ch->UnlockAndReleaseReader(reader);
    ch->UnlockAndReleaseWriter(writer);
    DCHECK(ch->Invariants());
  }

  reader->sel->mu.Lock();
  // We must guarantee that this case is eligible to proceed before any
  // side effects can occur.
  if (reader->sel->picked != Selector::kNonePicked) {
    reader->sel->mu.Unlock();
    // Already handled item
    DVLOG(2) << "Read cancelled since another selector case done";
    DCHECK(ch->Invariants());
    return true;
  }

  if (ch->closed_) {
    DVLOG(2) << "Read failing because channel closed";
    *dst_ok = false;
    ch->UnlockAndReleaseReader(reader);
    return true;
  }

  if (enqueue) {
    // Register with waiting readers
    DVLOG(2) << "Read waiting";
    internal::PushBack(&ch->waiting_readers_, reader);
  }

  reader->sel->mu.Unlock();
  DCHECK(ch->Invariants());
  return false;
}

template <typename T>
bool ChannelState<T>::Wr::Handle(CaseState* writer, bool enqueue) {
  ChannelState* ch = state;
  eglt::concurrency::impl::MutexLock l(&ch->mu_);
  DCHECK(ch->Invariants());
  CHECK(!ch->closed_) << "Calling Write() on closed channel";

  // First try to transfer directly from writer to a waiting reader
  CaseState* reader;
  if (ch->GetMatchingReader(writer, &reader)) {
    T* writer_item = reinterpret_cast<T*>(writer->params->arg1);
    T* reader_item = reinterpret_cast<T*>(reader->params->arg1);
    *reader_item = CopyOrMoveOut(writer_item, writer->params->arg2);
    *reinterpret_cast<bool*>(reader->params->arg2) = true;
    ch->UnlockAndReleaseReader(reader);
    ch->UnlockAndReleaseWriter(writer);
    DCHECK(ch->Invariants());
    return true;
  }

  writer->sel->mu.Lock();
  // We must guarantee that this case is eligible to proceed before any
  // side effects can occur.
  if (writer->sel->picked != Selector::kNonePicked) {
    writer->sel->mu.Unlock();
    // Already handled item
    DVLOG(2) << "Write cancelled since another selector case done";
    DCHECK(ch->Invariants());
    return true;
  }

  // Is there room to buffer item?
  if (ch->queue_.size() < ch->capacity_) {
    DVLOG(2) << "Add to buffer";
    ch->queue_.push_back(CopyOrMoveOut(
        reinterpret_cast<T*>(writer->params->arg1), writer->params->arg2));
    ch->UnlockAndReleaseWriter(writer);
    DCHECK(ch->Invariants());
    return true;
  }

  if (enqueue) {
    // Register with waiting writers
    DVLOG(2) << "Write waiting";
    internal::PushBack(&ch->waiting_writers_, writer);
  }

  writer->sel->mu.Unlock();
  DCHECK(ch->Invariants());
  return false;
}

template <typename T>
bool ChannelState<T>::Invariants() const {
  // Use CHECK since if the caller wants no prod failures, they will
  // call DCHECK(Invariants()) and not get in here in prod mode.
  CHECK_LE(queue_.size(), capacity_);
  return true;
}

}  // namespace thread::internal

#endif  // THREAD_FIBER_CHANNEL_INTERNAL_H_
