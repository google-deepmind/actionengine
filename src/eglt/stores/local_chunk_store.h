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

#ifndef EGLT_STORES_LOCAL_CHUNK_STORE_H_
#define EGLT_STORES_LOCAL_CHUNK_STORE_H_

#include <cstddef>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/map_util.h"

namespace eglt {

/**
 * @brief
 *   A local chunk store for storing a node's chunks in memory.
 *
 * This class provides a thread-safe implementation of a chunk store that
 * stores chunks in memory. It allows for writing, reading, and waiting for
 * chunks to be available.
 *
 * @headerfile eglt/stores/local_chunk_store.h
 */
class LocalChunkStore final : public ChunkStore {
 public:
  LocalChunkStore() : ChunkStore() {}

  explicit LocalChunkStore(std::string_view id) : LocalChunkStore() {
    LocalChunkStore::SetIdOrDie(id);
  }

  // Neither copyable nor movable.
  LocalChunkStore(const LocalChunkStore& other);
  LocalChunkStore& operator=(const LocalChunkStore& other);

  ~LocalChunkStore() override { ClosePutsAndAwaitPendingOperations(); }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout) override {
    eglt::MutexLock lock(&mu_);
    concurrency::PreventExclusiveAccess pending(&finalization_guard_,
                                                /*retain_lock=*/true);
    if (chunks_.contains(seq)) {
      const Chunk& chunk = chunks_.at(seq);
      return chunk;
    }

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
    }

    absl::Status status;
    while (!chunks_.contains(seq) && !no_further_puts_ &&
           !thread::Cancelled()) {
      if (cv_.WaitWithTimeout(&mu_, timeout)) {
        status = absl::DeadlineExceededError(
            absl::StrCat("Timed out waiting for seq: ", seq));
        break;
      }
      if (thread::Cancelled()) {
        status = absl::CancelledError(
            absl::StrCat("Cancelled waiting for seq: ", seq));
        break;
      }
      if (no_further_puts_ && !chunks_.contains(seq)) {
        status = absl::FailedPreconditionError(
            "Cannot get chunks after the store has been closed.");
        break;
      }
    }

    if (!status.ok()) {
      return status;
    }
    return eglt::FindOrDie(chunks_, seq);
  }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRefByArrivalOrder(
      int64_t arrival_offset, absl::Duration timeout) override {
    eglt::MutexLock lock(&mu_);
    concurrency::PreventExclusiveAccess pending(&finalization_guard_,
                                                /*retain_lock=*/true);
    if (arrival_order_to_seq_.contains(arrival_offset)) {
      const int64_t seq = arrival_order_to_seq_.at(arrival_offset);
      const Chunk& chunk = eglt::FindOrDie(chunks_, seq);
      return chunk;
    }

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
    }

    absl::Status status;
    while (!arrival_order_to_seq_.contains(arrival_offset) &&
           !no_further_puts_ && !thread::Cancelled()) {

      if (cv_.WaitWithTimeout(&mu_, timeout)) {
        status = absl::DeadlineExceededError(absl::StrCat(
            "Timed out waiting for arrival offset: ", arrival_offset));
        break;
      }
      if (thread::Cancelled()) {
        status = absl::CancelledError(absl::StrCat(
            "Cancelled waiting for arrival offset: ", arrival_offset));
        break;
      }
      if (no_further_puts_ && !arrival_order_to_seq_.contains(arrival_offset)) {
        status = absl::FailedPreconditionError(
            "Cannot get chunks after the store has been closed.");
        break;
      }
    }

    if (!status.ok()) {
      return status;
    }
    return eglt::FindOrDie(
        chunks_, eglt::FindOrDie(arrival_order_to_seq_, arrival_offset));
  }

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override {
    eglt::MutexLock lock(&mu_);
    if (const auto map_node = chunks_.extract(seq); map_node) {
      const int64_t arrival_order = seq_to_arrival_order_[seq];
      seq_to_arrival_order_.erase(seq);
      arrival_order_to_seq_.erase(arrival_order);
      return std::move(map_node.mapped());
    }

    return std::nullopt;
  }

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override {
    eglt::MutexLock lock(&mu_);

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot put chunks after the store has been closed.");
    }

    max_seq_ = std::max(max_seq_, seq);
    final_seq_ = final ? seq : final_seq_;

    arrival_order_to_seq_[total_chunks_put_] = seq;
    seq_to_arrival_order_[seq] = total_chunks_put_;
    chunks_[seq] = std::move(chunk);
    ++total_chunks_put_;

    cv_.SignalAll();
    return absl::OkStatus();
  }

  absl::Status CloseWritesWithStatus(absl::Status) override {
    eglt::MutexLock lock(&mu_);

    no_further_puts_ = true;
    if (max_seq_ != -1) {
      final_seq_ = std::min(final_seq_, max_seq_);
    }
    // Notify all waiters because they will not be able to get any more chunks.
    cv_.SignalAll();
    return absl::OkStatus();
  }

  absl::StatusOr<size_t> Size() override {
    eglt::MutexLock lock(&mu_);
    return chunks_.size();
  }

  absl::StatusOr<bool> Contains(int64_t seq) override {
    eglt::MutexLock lock(&mu_);
    return chunks_.contains(seq);
  }

  absl::Status SetId(std::string_view id) override {
    id_ = id;
    return absl::OkStatus();
  }
  std::string_view GetId() const override { return id_; }

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override {
    eglt::MutexLock lock(&mu_);
    if (!arrival_order_to_seq_.contains(arrival_offset)) {
      return -1;
    }
    return arrival_order_to_seq_.at(arrival_offset);
  }

  absl::StatusOr<int64_t> GetFinalSeq() override {
    eglt::MutexLock lock(&mu_);
    return final_seq_;
  }

 private:
  void ClosePutsAndAwaitPendingOperations() {
    eglt::MutexLock lock(&mu_);

    no_further_puts_ = true;
    // Notify all waiters because they will not be able to get any more chunks.
    cv_.SignalAll();

    concurrency::EnsureExclusiveAccess waiter(&finalization_guard_);
  }

  mutable eglt::Mutex mu_;

  std::string id_;

  absl::flat_hash_map<int64_t, int64_t> seq_to_arrival_order_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int64_t, int64_t> arrival_order_to_seq_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int64_t, Chunk> chunks_ ABSL_GUARDED_BY(mu_);

  int64_t final_seq_ = -1;
  int64_t max_seq_ = -1;
  int64_t total_chunks_put_ ABSL_GUARDED_BY(mu_) = 0;

  bool no_further_puts_ ABSL_GUARDED_BY(mu_) = false;
  mutable eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);
  mutable concurrency::ExclusiveAccessGuard finalization_guard_{&mu_, &cv_};
};

}  // namespace eglt

#endif  // EGLT_STORES_LOCAL_CHUNK_STORE_H_
