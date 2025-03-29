#include "chunk_store_local.h"

#include <cstddef>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/nodes/chunk_store.h"

namespace eglt {

LocalChunkStore::LocalChunkStore() :
  ChunkStore() {}

LocalChunkStore::LocalChunkStore(const LocalChunkStore& other) {
  concurrency::MutexLock lock(&other.mutex_);
  // we only copy the data, not the events because it makes no sense to
  // replicate waiter state on a new object.
  arrival_order_to_seq_id_ = other.arrival_order_to_seq_id_;
  chunks_ = other.chunks_;
  final_seq_id_ = other.final_seq_id_;
  write_offset_ = other.write_offset_;
}

LocalChunkStore& LocalChunkStore::operator=(const LocalChunkStore& other) {
  if (this == &other) { return *this; }

  {
    // we're about to be replaced, so we should notify all waiters immediately.
    concurrency::MutexLock lock(&mutex_);
    NotifyAllWaiters();
  }

  concurrency::MutexLock lock(&other.mutex_);
  // we only copy the data, not the events because it makes no sense to
  // replicate waiter state on a new object.
  arrival_order_to_seq_id_ = other.arrival_order_to_seq_id_;
  chunks_ = other.chunks_;
  final_seq_id_ = other.final_seq_id_;
  write_offset_ = other.write_offset_;

  return *this;
}

LocalChunkStore::LocalChunkStore(LocalChunkStore&& other) noexcept {
  concurrency::MutexLock lock(&other.mutex_);
  concurrency::MutexLock event_lock(&other.event_mutex_);

  seq_id_readable_events_ = std::move(other.seq_id_readable_events_);
  arrival_offset_readable_events_ =
    std::move(other.arrival_offset_readable_events_);
  arrival_order_to_seq_id_ = std::move(other.arrival_order_to_seq_id_);
  chunks_ = std::move(other.chunks_);
  final_seq_id_ = other.final_seq_id_;
  write_offset_ = other.write_offset_;
}

LocalChunkStore& LocalChunkStore::operator=(LocalChunkStore&& other) noexcept {
  if (this == &other) { return *this; }

  {
    // we're about to be replaced, so we should notify all waiters immediately.
    concurrency::MutexLock lock(&mutex_);
    NotifyAllWaiters();
  }
  concurrency::MutexLock lock(&other.mutex_);
  concurrency::MutexLock event_lock(&other.event_mutex_);

  seq_id_readable_events_ = std::move(other.seq_id_readable_events_);
  arrival_offset_readable_events_ =
    std::move(other.arrival_offset_readable_events_);
  arrival_order_to_seq_id_ = std::move(other.arrival_order_to_seq_id_);
  chunks_ = std::move(other.chunks_);
  final_seq_id_ = other.final_seq_id_;
  write_offset_ = other.write_offset_;

  return *this;
}

absl::StatusOr<base::Chunk> LocalChunkStore::GetImmediately(int seq_id) {
  concurrency::MutexLock lock(&mutex_);
  if (!chunks_.contains(seq_id)) {
    return absl::NotFoundError(absl::StrCat("Chunk not found: ", seq_id));
  }
  return chunks_.at(seq_id);
}

absl::StatusOr<base::Chunk> LocalChunkStore::PopImmediately(int seq_id) {
  concurrency::MutexLock lock(&mutex_);
  if (!chunks_.contains(seq_id)) {
    return absl::NotFoundError(absl::StrCat("Chunk not found: ", seq_id));
  }
  auto chunk = chunks_.at(seq_id);
  chunks_.erase(seq_id);
  return chunk;
}

size_t LocalChunkStore::Size() {
  concurrency::MutexLock lock(&mutex_);
  return chunks_.size();
}

bool LocalChunkStore::Contains(int seq_id) {
  concurrency::MutexLock lock(&mutex_);
  return chunks_.contains(seq_id);
}

void LocalChunkStore::NotifyAllWaiters() {
  concurrency::MutexLock lock(&event_mutex_);
  for (const auto& [arrival_offset, event] : arrival_offset_readable_events_) {
    if (event->HasBeenNotified()) { continue; }
    event->Notify();
  }

  for (const auto& [seq_id, event] : seq_id_readable_events_) {
    if (event->HasBeenNotified()) { continue; }
    event->Notify();
  }
}

int LocalChunkStore::GetFinalSeqId() {
  concurrency::MutexLock lock(&mutex_);
  return final_seq_id_;
}

absl::Status LocalChunkStore::WaitForSeqId(int seq_id, float timeout) {
  concurrency::PermanentEvent* event;
  {
    concurrency::MutexLock data_lock(&mutex_);
    concurrency::MutexLock event_lock(&event_mutex_);

    if (chunks_.contains(seq_id)) { return absl::OkStatus(); }

    std::unique_ptr<concurrency::PermanentEvent>& event_ptr =
      seq_id_readable_events_[seq_id];
    if (!event_ptr) {
      event_ptr = std::make_unique<concurrency::PermanentEvent>();
    }
    event = event_ptr.get();

    // this may happen if another thread has just been selected after waiting,
    // but has not obtained the lock to erase the event yet. If we don't check
    // this, we may end up calling ->OnEvent() on a deleted object.
    if (event->HasBeenNotified()) { return absl::OkStatus(); }
  }

  absl::Time deadline = timeout < 0
    ? absl::InfiniteFuture()
    : absl::Now() + absl::Seconds(timeout);
  int selected = concurrency::SelectUntil(
    deadline, {event->OnEvent(), concurrency::OnCancel()});
  if (selected == -1) {
    return absl::DeadlineExceededError(absl::StrCat(
      "Timed out waiting for seq_id: ", seq_id, " timeout: ", timeout));
  }
  if (selected == 1) {
    return absl::CancelledError(absl::StrCat(
      "Cancelled waiting for seq_id: ", seq_id, " timeout: ", timeout));
  }

  concurrency::MutexLock event_lock(&event_mutex_);
  seq_id_readable_events_.erase(seq_id);

  return absl::OkStatus();
}

absl::StatusOr<int> LocalChunkStore::WriteToImmediateStore(
  int seq_id, base::Chunk chunk) {
  arrival_order_to_seq_id_[write_offset_] = seq_id;
  chunks_[seq_id] = std::move(chunk);

  int arrival_offset = write_offset_;
  ++write_offset_;
  return arrival_offset;
}

void LocalChunkStore::NotifyWaiters(int seq_id, int arrival_offset) {
  concurrency::MutexLock lock(&event_mutex_);
  if (seq_id_readable_events_.contains(seq_id)) {
    if (!seq_id_readable_events_.at(seq_id)->HasBeenNotified()) {
      seq_id_readable_events_.at(seq_id)->Notify();
    }
  }

  if (arrival_offset_readable_events_.contains(arrival_offset)) {
    if (!arrival_offset_readable_events_.at(arrival_offset)
                                        ->HasBeenNotified()) {
      arrival_offset_readable_events_.at(arrival_offset)->Notify();
    }
  }
}

absl::Status LocalChunkStore::WaitForArrivalOffset(int arrival_offset,
                                                   float timeout) {
  concurrency::PermanentEvent* event;
  {
    concurrency::MutexLock data_lock(&mutex_);
    concurrency::MutexLock event_lock(&event_mutex_);

    if (arrival_order_to_seq_id_.contains(arrival_offset)) {
      return absl::OkStatus();
    }

    std::unique_ptr<concurrency::PermanentEvent>& event_ptr =
      arrival_offset_readable_events_[arrival_offset];
    if (!event_ptr) {
      event_ptr = std::make_unique<concurrency::PermanentEvent>();
    }
    event = event_ptr.get();

    // this may happen if another thread has just been selected after waiting,
    // but has not obtained the lock to erase the event yet. If we don't check
    // this, we may end up calling ->OnEvent() on a deleted object.
    if (event->HasBeenNotified()) { return absl::OkStatus(); }
  }

  absl::Time deadline = timeout < 0
    ? absl::InfiniteFuture()
    : absl::Now() + absl::Seconds(timeout);
  int selected = concurrency::SelectUntil(
    deadline, {event->OnEvent(), concurrency::OnCancel()});
  if (selected == -1) {
    return absl::DeadlineExceededError(
      absl::StrCat("Timed out waiting for arrival offset: ", arrival_offset,
                   " timeout: ", timeout));
  }
  if (selected == 1) {
    return absl::CancelledError(
      absl::StrCat("Cancelled waiting for arrival offset: ", arrival_offset,
                   " timeout: ", timeout));
  }

  concurrency::MutexLock event_lock(&event_mutex_);
  arrival_offset_readable_events_.erase(arrival_offset);

  return absl::OkStatus();
}

int LocalChunkStore::GetSeqIdForArrivalOffset(int arrival_offset) {
  if (!arrival_order_to_seq_id_.contains(arrival_offset)) { return -1; }
  return arrival_order_to_seq_id_.at(arrival_offset);
}

void LocalChunkStore::SetFinalSeqId(int final_seq_id) {
  final_seq_id_ = final_seq_id;
}

} // namespace eglt
