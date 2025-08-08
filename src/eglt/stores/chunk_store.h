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

/**
 * @brief
 *   An abstract interface for raw data storage and retrieval for ActionEngine
 *   nodes.
 *
 * This class provides a generic interface for storing and retrieving chunks of
 * data, identified by a sequence number. It supports both ordered and unordered
 * access to the chunks, as well as various operations such as putting, getting,
 * and popping chunks.
 *
 * @note
 *   A key feature of this interface is its flexibility, allowing it to be used
 *   in a wide range of scenarios, from simple in-memory storage to complex
 *   distributed systems.
 * @note
 *   However, this places a limitation: note that
 *   ChunkStore itself does not care about streaming semantics or, for example,
 *   range-based retrieval or writes. Streaming semantics are tackled by
 *   ChunkStoreReader and ChunkStoreWriter, which are built on top of this
 *   interface and are used together by AsyncNode to provide channel-like
 *   functionality.
 *
 * Importantly, the Get() and GetByArrivalOrder() methods are designed to
 * block until the requested chunk is available, allowing for synchronous
 * channel-like behavior. This is particularly useful in asynchronous workflows
 * by providing a way to wait for data to become available without having to
 * manage the underlying storage details or streaming semantics directly.
 *
 * ChunkStore can be implemented in various ways,
 * such as in-memory or file-based storage, in relational or key-value
 * databases, or even in distributed storage systems. The specific
 * implementation will determine how the chunks are stored and retrieved, but
 * the interface remains consistent across implementations.
 *
 * @note
 *   Efficiency and performance optimizations for specific
 *   storage backends, such as range-based queries, are expected to be
 *   implemented in concrete ChunkStore subclasses. This may change in the
 *   future as this interface evolves. For now, it is based, essentially,
 *   around keys and values.
 *
 * @headerfile eglt/stores/chunk_store.h
 */
class ChunkStore {
 public:
  ChunkStore() = default;

  // Neither copyable nor movable.
  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore& other) = delete;

  virtual ~ChunkStore() = default;

  virtual void Notify() {}

  /** @brief
   *    Get a chunk by its sequence number from the represented store.
   *
   *  This method blocks until the chunk with the specified sequence number
   *  is available in the store, or until the specified timeout expires. As
   *  storage may be non-local, any errors encountered while retrieving the
   *  chunk will be returned as an absl::Status.
   *
   *  @param seq
   *    The sequence number of the chunk to retrieve.
   *  @param timeout
   *    The maximum duration to wait for the chunk to become available.
   *  @return
   *    A Chunk object containing the data associated with the specified
   *    sequence number, or an `absl::Status` indicating an error if the chunk
   *    could not be retrieved due to timeout or other issues.
   *
   *  @note
   *    This method may react to cancellation of the calling fiber, but it
   *    is not guaranteed to do so: that depends on the specific implementation.
   *    However, in cases when it becomes evident that the chunk can never be
   *    retrieved (e.g. writes were closed or final sequence number is less than
   *    the requested sequence number), correct implementations should return
   *    errors and not block indefinitely, even if the state only becomes
   *    evident after the method is called.
   */
  virtual absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout);
  /** @brief
   *    Same as Get(), but retrieves the chunk by its arrival order (rank by
   *    arrival time) instead of sequence number.
   *
   *  @param arrival_order
   *    The parameter `n` to retrieve the `n`-th chunk that has arrived in the
   *    store. The first chunk that arrives has an arrival order of 0, the
   *    second has an arrival order of 1, and so on.
   *  @param timeout
   *    The maximum duration to wait for the chunk to become available.
   *  @return
   *    A Chunk object containing the data associated with the specified
   *    sequence number, or an `absl::Status` indicating an error if the chunk
   *    could not be retrieved due to timeout or other issues.
   */
  virtual absl::StatusOr<Chunk> GetByArrivalOrder(int64_t arrival_order,
                                                  absl::Duration timeout);

  /** @brief
   *    Same as Get(), but returns a reference to the chunk instead of
   *    copying it.
   *
   *  This allows for more efficient access to the chunk data in cases where
   *  data lives in-process, such as in-memory storage.
   *
   *  @param seq
   *    The sequence number of the chunk to retrieve.
   *  @param timeout
   *    The maximum duration to wait for the chunk to become available.
   *  @return
   *    A reference to a Chunk object containing the data associated with the
   *    specified sequence number, or an `absl::Status` indicating an error if
   *    the chunk could not be retrieved due to timeout or other issues.
   */
  virtual absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout);
  /** @brief
   *    Same as GetByArrivalOrder(), but returns a reference to the chunk
   *    instead of copying it.
   *
   *  This allows for more efficient access to the chunk data in cases where
   *  data lives in-process, such as in-memory storage.
   *
   *  @param seq
   *    The sequence number of the chunk to retrieve.
   *  @param timeout
   *    The maximum duration to wait for the chunk to become available.
   *  @return
   *    A reference to a Chunk object containing the data associated with the
   *    specified sequence number, or an `absl::Status` indicating an error if
   *    the chunk could not be retrieved due to timeout or other issues.
   */
  virtual absl::StatusOr<std::reference_wrapper<const Chunk>>
  GetRefByArrivalOrder(int64_t seq, absl::Duration timeout);

  /** @brief
   *    Put a chunk into the store with the specified sequence number.
   *
   * This method allows you to store a chunk of data in the store, associating
   * it with a specific sequence number. If the `final` parameter is set to
   * `true`, it indicates that this is the last chunk in a sequence, and no
   * further chunks with a higher sequence number will be added.
   *
   * @param seq
   *   The sequence number to associate with the chunk. Putting the same \p seq
   *   twice is an error.
   * @param chunk
   *   The Chunk of data to put in the store.
   * @param final
   *   A boolean indicating whether this is the final chunk in a sequence.
   *   If `true`, it indicates that no further chunks with a higher sequence
   *   number will be added to the store.
   *
   * @note
   *   Complete and correct implementations of this method may become quite
   *   complex, as they need to handle various edge cases, such as:
   *   * ensuring that the sequence number is unique,
   *   * managing the finality of chunks and communicating that to
   *     readers,
   *   * handling concurrent writes and reads without data corruption or loss.
   * @note
   *   LocalChunkStore is a good example of such an implementation.
   *   redis::ChunkStore is another example, which implements this interface
   *   on top of Redis, leveraging its atomicity within Lua scripts to ensure
   *   that writes are safe and consistent.
   *
   * @ref ChunkStore::Put
   */
  virtual absl::Status Put(int64_t seq, Chunk chunk, bool final) = 0;
  /** @brief
   *    Pop a chunk from the store by its sequence number.
   *
   * This method removes the chunk associated with the specified sequence number
   * from the store and returns it. If no chunk with that sequence number exists,
   * it returns `std::nullopt`.
   *
   * @param seq
   *   The sequence number of the chunk to pop.
   * @return
   *   An `absl::StatusOr<std::optional<Chunk>>` containing the popped chunk, or
   *   `std::nullopt` if no chunk with that sequence number exists. If an error
   *   occurs during the operation, it returns an `absl::Status` indicating the
   *   error.
   */
  virtual absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) = 0;

  /** @brief
   *    Closes the store for writes, allowing for finalization of the store.
   *
   * This method is used to indicate that no further writes will be made to the
   * store. It can be used to finalize the store, ensuring that no new writes
   * are allowed. Any pending read operations that are waiting for new writes
   * will be notified that no further writes will occur.
   *
   * @param status
   *   The status to set for the store, indicating whether it was closed
   *   successfully or with an error.
   * @return
   *   An `absl::Status` indicating the success or failure of the operation.
   */
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
