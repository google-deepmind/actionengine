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

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/map_util.h"

namespace eglt {

/**
 * A local chunk store for storing a given node's chunks in memory.
 *
 * This class provides a thread-safe implementation of a chunk store that
 * stores chunks in memory. It allows for writing, reading, and waiting for
 * chunks to be available.
 *
 * @headerfile eglt/stores/local_chunk_store.h
 */
class LocalChunkStore final : public ChunkStore {
  // For detailed documentation, see the base class, ChunkStore.
 public:
  LocalChunkStore() : ChunkStore() {}

  explicit LocalChunkStore(std::string_view id);

  // Neither copyable nor movable.
  LocalChunkStore(const LocalChunkStore& other) = delete;
  LocalChunkStore& operator=(const LocalChunkStore& other) = delete;

  ~LocalChunkStore() override;

  void Notify() override;

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout) override;

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRefByArrivalOrder(
      int64_t arrival_offset, absl::Duration timeout) override;

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override;

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override;

  absl::Status CloseWritesWithStatus(absl::Status) override;

  absl::StatusOr<size_t> Size() override;

  absl::StatusOr<bool> Contains(int64_t seq) override;

  absl::Status SetId(std::string_view id) override;

  std::string_view GetId() const override;

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override;

  absl::StatusOr<int64_t> GetFinalSeq() override;

 private:
  void ClosePutsAndAwaitPendingOperations() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

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
