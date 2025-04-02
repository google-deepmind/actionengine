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

#ifndef EGLT_NODES_CHUNK_STORE_H_
#define EGLT_NODES_CHUNK_STORE_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"

namespace eglt {

class ChunkStore {
 public:
  ChunkStore() = default;
  virtual ~ChunkStore() = default;

  ChunkStore(const ChunkStore&) = delete;
  ChunkStore(ChunkStore&& other) = delete;

  ChunkStore& operator=(const ChunkStore& other) = delete;
  ChunkStore& operator=(ChunkStore&& other) = delete;

  virtual auto Get(int seq_id, float timeout) -> absl::StatusOr<base::Chunk>;
  virtual auto Pop(int seq_id, float timeout) -> absl::StatusOr<base::Chunk>;
  virtual auto Put(int seq_id, base::Chunk chunk, bool final) -> absl::Status;

  virtual auto GetImmediately(int seq_id) -> absl::StatusOr<base::Chunk> = 0;
  virtual auto PopImmediately(int seq_id) -> absl::StatusOr<base::Chunk> = 0;

  virtual auto Size() -> size_t = 0;
  virtual bool Contains(int seq_id) = 0;
  virtual void NotifyAllWaiters() = 0;

  void SetNodeId(std::string_view id) { node_id_ = id; }
  std::string GetNodeId() const { return node_id_; }

  virtual auto GetSeqIdForArrivalOffset(int arrival_offset) -> int = 0;
  virtual auto GetFinalSeqId() -> int = 0;
  // TODO(helenapankov): use absl::Duration instead of float
  virtual auto WaitForSeqId(int seq_id, float timeout) -> absl::Status = 0;
  virtual auto WaitForArrivalOffset(int arrival_offset, float timeout)
      -> absl::Status = 0;
  // TODO (helenapankov): add a method to wait for finalisation

 protected:
  virtual auto WriteToImmediateStore(int seq_id, base::Chunk chunk)
      -> absl::StatusOr<int> = 0;

  virtual void NotifyWaiters(int seq_id, int arrival_offset) = 0;

  virtual void SetFinalSeqId(int final_seq_id) = 0;

  mutable concurrency::Mutex mutex_ ABSL_ACQUIRED_BEFORE(event_mutex_);
  mutable concurrency::Mutex event_mutex_ ABSL_ACQUIRED_AFTER(mutex_);

  std::string node_id_;
};

using ChunkStoreFactory = std::function<std::unique_ptr<ChunkStore>()>;

template <typename T>
std::unique_ptr<T> MakeChunkStore() {
  return std::make_unique<T>();
}

}  // namespace eglt

#endif  // EGLT_NODES_CHUNK_STORE_H_
