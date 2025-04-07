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

#ifndef EGLT_NODES_CHUNK_STORE_LOCAL_H_
#define EGLT_NODES_CHUNK_STORE_LOCAL_H_

#include <cstddef>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/nodes/chunk_store.h"

namespace eglt {

/**
 * @brief
 *   A local chunk store for storing a node's chunks in memory.
 *
 * This class provides a thread-safe implementation of a chunk store that
 * stores chunks in memory. It allows for writing, reading, and waiting for
 * chunks to be available.
 *
 * @headerfile eglt/nodes/chunk_store_local.h
 */
class LocalChunkStore final : public ChunkStore {
public:
  LocalChunkStore();

  LocalChunkStore(const LocalChunkStore& other);
  LocalChunkStore& operator=(const LocalChunkStore& other);

  LocalChunkStore(LocalChunkStore&& other) noexcept;
  LocalChunkStore& operator=(LocalChunkStore&& other) noexcept;

  ABSL_LOCKS_EXCLUDED(mutex_)
  auto GetImmediately(int seq_id) -> absl::StatusOr<base::Chunk> override;
  ABSL_LOCKS_EXCLUDED(mutex_)
  auto PopImmediately(int seq_id) -> absl::StatusOr<base::Chunk> override;

  ABSL_LOCKS_EXCLUDED(mutex_)
  auto Size() -> size_t override;
  ABSL_LOCKS_EXCLUDED(mutex_)
  bool Contains(int seq_id) override;

  void NotifyAllWaiters() ABSL_LOCKS_EXCLUDED(event_mutex_) override;

  auto GetSeqIdForArrivalOffset(int arrival_offset) -> int override;
  auto GetFinalSeqId() -> int override;

  ABSL_LOCKS_EXCLUDED(mutex_, event_mutex_)
  auto WaitForSeqId(int seq_id, float timeout) -> absl::Status override;
  ABSL_LOCKS_EXCLUDED(mutex_, event_mutex_)
  auto WaitForArrivalOffset(int arrival_offset, float timeout)
    -> absl::Status override;

protected:
  ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_)
  auto WriteToImmediateStore(int seq_id, base::Chunk chunk)
    -> absl::StatusOr<int> override;

  void NotifyWaiters(int seq_id, int arrival_offset)
  ABSL_LOCKS_EXCLUDED(event_mutex_) override;

  void SetFinalSeqId(int final_seq_id) override;

private:
  absl::flat_hash_map<int, std::unique_ptr<concurrency::PermanentEvent>>
  seq_id_readable_events_ ABSL_GUARDED_BY(event_mutex_);
  absl::flat_hash_map<int, std::unique_ptr<concurrency::PermanentEvent>>
  arrival_offset_readable_events_ ABSL_GUARDED_BY(event_mutex_);

  absl::flat_hash_map<int, int> arrival_order_to_seq_id_
  ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<int, base::Chunk> chunks_ ABSL_GUARDED_BY(mutex_);

  // TODO(helenapankov): this field has to be protected, but that might require
  //   a reconsideration of the interface/implementation split.
  int final_seq_id_ = -1;
  int write_offset_ ABSL_GUARDED_BY(mutex_) = 0;
};

} // namespace eglt

#endif  // EGLT_NODES_CHUNK_STORE_LOCAL_H_
