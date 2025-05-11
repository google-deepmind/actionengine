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

/**
 * @file
 * @brief
 *   An abstract interface for raw data storage and retrieval for Evergreen
 *   nodes.
 */

#ifndef EGLT_STORES_CHUNK_STORE_H_
#define EGLT_STORES_CHUNK_STORE_H_

#include <functional>
#include <memory>
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

  [[nodiscard]] virtual auto Get(int seq_id, absl::Duration timeout) const
      -> absl::StatusOr<std::reference_wrapper<const Chunk>> = 0;
  [[nodiscard]] virtual auto GetByArrivalOrder(int arrival_offset,
                                               absl::Duration timeout) const
      -> absl::StatusOr<std::reference_wrapper<const Chunk>> = 0;
  virtual auto Pop(int seq_id) -> std::optional<Chunk> = 0;
  virtual auto Put(int seq_id, Chunk chunk, bool final) -> absl::Status = 0;

  virtual void NoFurtherPuts() = 0;

  virtual auto Size() -> size_t = 0;
  virtual bool Contains(int seq_id) = 0;

  virtual void SetId(std::string_view id) = 0;
  [[nodiscard]] virtual auto GetId() const -> std::string_view = 0;

  virtual auto GetSeqIdForArrivalOffset(int arrival_offset) -> int = 0;
  virtual auto GetFinalSeqId() -> int = 0;
};

using ChunkStoreFactory = std::function<std::unique_ptr<ChunkStore>()>;

template <typename T, typename... Args>
std::unique_ptr<T> MakeChunkStore(Args&&... args) {
  return std::make_unique<T>(std::forward<Args>(args)...);
}

}  // namespace eglt

#endif  // EGLT_STORES_CHUNK_STORE_H_
