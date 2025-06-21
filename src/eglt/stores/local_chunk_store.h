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
#include <memory>

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
    SetId(id);
  }

  ~LocalChunkStore() override { ClosePutsAndAwaitPendingOperations(); }

  LocalChunkStore(const LocalChunkStore& other);
  LocalChunkStore& operator=(const LocalChunkStore& other);

  LocalChunkStore(LocalChunkStore&& other) noexcept;
  LocalChunkStore& operator=(LocalChunkStore&& other) noexcept;

  absl::StatusOr<std::reference_wrapper<const Chunk>> Get(
      int seq_id, absl::Duration timeout) const override {
    concurrency::MutexLock lock(&mu_);
    concurrency::PreventExclusiveAccess pending(&finalization_guard_,
                                                /*retain_lock=*/true);
    if (chunks_.contains(seq_id)) {
      const auto& chunk = chunks_.at(seq_id);
      return chunk;
    }

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
    }

    absl::Status status;
    while (!chunks_.contains(seq_id) && !no_further_puts_ &&
           !concurrency::Cancelled()) {
      if (cv_.WaitWithTimeout(&mu_, timeout)) {
        status = absl::DeadlineExceededError(
            absl::StrCat("Timed out waiting for seq_id: ", seq_id));
        break;
      }
      if (concurrency::Cancelled()) {
        status = absl::CancelledError(
            absl::StrCat("Cancelled waiting for seq_id: ", seq_id));
        break;
      }
      if (no_further_puts_ && !chunks_.contains(seq_id)) {
        status = absl::FailedPreconditionError(
            "Cannot get chunks after the store has been closed.");
        break;
      }
    }

    if (!status.ok()) {
      return status;
    }
    return eglt::FindOrDie(chunks_, seq_id);
  }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetByArrivalOrder(
      int arrival_offset, absl::Duration timeout) const override {
    concurrency::MutexLock lock(&mu_);
    concurrency::PreventExclusiveAccess pending(&finalization_guard_,
                                                /*retain_lock=*/true);
    if (arrival_order_to_seq_id_.contains(arrival_offset)) {
      const int seq_id = arrival_order_to_seq_id_.at(arrival_offset);
      const Chunk& chunk = eglt::FindOrDie(chunks_, seq_id);
      return chunk;
    }

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
    }

    absl::Status status;
    while (!arrival_order_to_seq_id_.contains(arrival_offset) &&
           !no_further_puts_ && !concurrency::Cancelled()) {

      if (cv_.WaitWithTimeout(&mu_, timeout)) {
        status = absl::DeadlineExceededError(absl::StrCat(
            "Timed out waiting for arrival offset: ", arrival_offset));
        break;
      }
      if (concurrency::Cancelled()) {
        status = absl::CancelledError(absl::StrCat(
            "Cancelled waiting for arrival offset: ", arrival_offset));
        break;
      }
      if (no_further_puts_ &&
          !arrival_order_to_seq_id_.contains(arrival_offset)) {
        status = absl::FailedPreconditionError(
            "Cannot get chunks after the store has been closed.");
        break;
      }
    }

    if (!status.ok()) {
      return status;
    }
    return eglt::FindOrDie(
        chunks_, eglt::FindOrDie(arrival_order_to_seq_id_, arrival_offset));
  }

  std::optional<Chunk> Pop(int seq_id) override {
    concurrency::MutexLock lock(&mu_);
    if (const auto map_node = chunks_.extract(seq_id); map_node) {
      const int arrival_order = seq_id_to_arrival_order_[seq_id];
      seq_id_to_arrival_order_.erase(seq_id);
      arrival_order_to_seq_id_.erase(arrival_order);
      return std::move(map_node.mapped());
    }

    return std::nullopt;
  }

  absl::Status Put(int seq_id, Chunk chunk, bool final) override {
    concurrency::MutexLock lock(&mu_);

    if (no_further_puts_) {
      return absl::FailedPreconditionError(
          "Cannot put chunks after the store has been closed.");
    }

    max_seq_id_ = std::max(max_seq_id_, seq_id);
    final_seq_id_ = final ? seq_id : final_seq_id_;

    arrival_order_to_seq_id_[total_chunks_put_] = seq_id;
    seq_id_to_arrival_order_[seq_id] = total_chunks_put_;
    chunks_[seq_id] = std::move(chunk);
    ++total_chunks_put_;

    cv_.SignalAll();
    return absl::OkStatus();
  }

  void NoFurtherPuts() override {
    concurrency::MutexLock lock(&mu_);

    no_further_puts_ = true;
    if (max_seq_id_ != -1) {
      final_seq_id_ = std::min(final_seq_id_, max_seq_id_);
    }
    // Notify all waiters because they will not be able to get any more chunks.
    cv_.SignalAll();
  }

  size_t Size() override ABSL_LOCKS_EXCLUDED(mu_) {
    concurrency::MutexLock lock(&mu_);
    return chunks_.size();
  }

  bool Contains(int seq_id) override ABSL_LOCKS_EXCLUDED(mu_) {
    concurrency::MutexLock lock(&mu_);
    return chunks_.contains(seq_id);
  }

  void SetId(std::string_view id) override { id_ = id; }
  std::string_view GetId() const override { return id_; }

  int GetSeqIdForArrivalOffset(int arrival_offset) override {
    concurrency::MutexLock lock(&mu_);
    if (!arrival_order_to_seq_id_.contains(arrival_offset)) {
      return -1;
    }
    return arrival_order_to_seq_id_.at(arrival_offset);
  }

  int GetFinalSeqId() override {
    concurrency::MutexLock lock(&mu_);
    return final_seq_id_;
  }

 private:
  void ClosePutsAndAwaitPendingOperations() {
    concurrency::MutexLock lock(&mu_);

    no_further_puts_ = true;
    // Notify all waiters because they will not be able to get any more chunks.
    cv_.SignalAll();

    concurrency::EnsureExclusiveAccess waiter(&finalization_guard_);
  }

  mutable concurrency::Mutex mu_;

  std::string id_;

  absl::flat_hash_map<int, int> seq_id_to_arrival_order_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, int> arrival_order_to_seq_id_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, Chunk> chunks_ ABSL_GUARDED_BY(mu_);

  int final_seq_id_ = -1;
  int max_seq_id_ = -1;
  int total_chunks_put_ ABSL_GUARDED_BY(mu_) = 0;

  bool no_further_puts_ ABSL_GUARDED_BY(mu_) = false;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mu_);
  mutable concurrency::ExclusiveAccessGuard finalization_guard_{&mu_, &cv_};
};

}  // namespace eglt

#endif  // EGLT_STORES_LOCAL_CHUNK_STORE_H_
