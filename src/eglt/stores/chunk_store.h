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
 *   An abstract interface for raw data storage and retrieval for ActionEngine
 *   nodes.
 */

#ifndef EGLT_STORES_CHUNK_STORE_H_
#define EGLT_STORES_CHUNK_STORE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>

#include "eglt/data/eg_structs.h"

namespace eglt {

class ChunkStore {
 public:
  ChunkStore() = default;

  // Neither copyable nor movable.
  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore& other) = delete;

  virtual ~ChunkStore() = default;

  virtual void Notify() {}

  virtual absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout);
  virtual absl::StatusOr<Chunk> GetByArrivalOrder(int64_t seq,
                                                  absl::Duration timeout);

  virtual absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout);
  virtual absl::StatusOr<std::reference_wrapper<const Chunk>>
  GetRefByArrivalOrder(int64_t seq, absl::Duration timeout);

  virtual absl::Status Put(int64_t seq, Chunk chunk, bool final) = 0;
  virtual absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) = 0;

  virtual absl::Status CloseWritesWithStatus(absl::Status status) = 0;

  virtual absl::StatusOr<size_t> Size() = 0;
  virtual absl::StatusOr<bool> Contains(int64_t seq) = 0;

  virtual absl::Status SetId(std::string_view id) = 0;
  [[nodiscard]] virtual auto GetId() const -> std::string_view = 0;

  virtual absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) = 0;
  virtual absl::StatusOr<int64_t> GetFinalSeq() = 0;

  // You should not override these methods. They are provided for convenience
  // and will call the StatusOr methods above, checking for errors and
  // terminating if any occur.
  virtual std::optional<Chunk> PopOrDie(int64_t seq) noexcept;
  virtual void CloseWritesWithStatusOrDie(absl::Status status) noexcept;
  [[nodiscard]] virtual size_t SizeOrDie() noexcept;
  [[nodiscard]] virtual bool ContainsOrDie(int64_t seq) noexcept;
  virtual void SetIdOrDie(std::string_view id) noexcept;
  [[nodiscard]] virtual int64_t GetSeqForArrivalOffsetOrDie(
      int64_t arrival_offset) noexcept;
  [[nodiscard]] virtual int64_t GetFinalSeqOrDie() noexcept;
};

using ChunkStoreFactory =
    std::function<std::unique_ptr<ChunkStore>(std::string_view)>;

template <typename T, typename... Args>
std::unique_ptr<T> MakeChunkStore(Args&&... args) {
  return std::make_unique<T>(std::forward<Args>(args)...);
}

}  // namespace eglt

#endif  // EGLT_STORES_CHUNK_STORE_H_
