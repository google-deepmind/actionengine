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

#ifndef ACTIONENGINE_STORES_CHUNK_STORE_READER_H_
#define ACTIONENGINE_STORES_CHUNK_STORE_READER_H_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/serialization.h"
#include "actionengine/data/types.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/util/status_macros.h"

namespace act {

/// Options for the ChunkStoreReader.
struct ChunkStoreReaderOptions {
  /// Whether to read chunks in the explicit seq order.
  /// If `false`, chunks will be read in the order they arrive in the store.
  /// This is useful for streaming data where the order of chunks is not
  /// important, such as parallel independent processing of chunks.
  bool ordered = true;

  /// Whether to remove chunks from the store after reading them.
  /// If `false`, chunks will remain in the store after reading. This is useful
  /// if you want to read the same chunks multiple times or if you want to
  /// keep the chunks for later processing.
  bool remove_chunks = true;

  /// The number of chunks to buffer in memory before the background fiber
  /// blocks on reading more chunks.
  size_t n_chunks_to_buffer = 32;

  /// The timeout for reading chunks from the store, which applies to
  /// the Next() method.
  absl::Duration timeout = absl::InfiniteDuration();
};

/** @brief
 *    A utility for reading chunks from a ChunkStore.
 *
 * This class provides an interface for reading chunks from a ChunkStore.
 * It supports reading chunks in order (explicitly specified) or unordered
 * (as ingested to the store), and can buffer chunks in memory to improve
 * performance.
 *
 * The reader can be cancelled, which stops the background prefetch fiber
 * and prevents further reading from the store. The remaining prefetched
 * chunks can still be read until the buffer is empty.
 */
class ChunkStoreReader {
 public:
  /** @brief
   *    Constructs a ChunkStoreReader for the given ChunkStore, setting
   *    @p options if provided.
   *
   * @param chunk_store
   *   The ChunkStore to read from. Must not be null.
   * @param options
   *   Options for the reader, such as whether to read in order, buffer size,
   *   and timeout.
   */
  explicit ChunkStoreReader(
      ChunkStore* absl_nonnull chunk_store,
      ChunkStoreReaderOptions options = ChunkStoreReaderOptions());

  // This class is not copyable or movable.
  ChunkStoreReader(const ChunkStoreReader&) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader&) = delete;

  ~ChunkStoreReader();

  /** @brief
   *    Cancels the background prefetch fiber and stops reading from the store.
   *
   *  Does not prevent further reading of chunks that have already been
   *  prefetched into the buffer. The buffer will be closed, and no further
   *  chunks will be read from the store. If the reader is already cancelled,
   *  this method does nothing.
   */
  void Cancel() const;

  /** @brief
   *    Sets the options for the ChunkStoreReader.
   *
   *  This method can only be called before the reader is started (i.e., before
   *  the first call to Next()). If called after the reader has been started,
   *  the program will be terminated with a CHECK failure.
   *
   * @param options
   *   The options to set for the reader.
   *
   * @note
   *   You can use designated initializers to set only the options you want to
   *   be different from the defaults. For example:
   *   @code{.cc}
   *   reader.SetOptions({.ordered = false});
   *   @endcode
   */
  void SetOptions(const ChunkStoreReaderOptions& options);

  /** @brief
   *    Returns the current options of the ChunkStoreReader.
   *
   * @return
   *   The current options of the reader.
   */
  [[nodiscard]]
  const ChunkStoreReaderOptions& GetOptions() const {
    return options_;
  }

  /** @brief
   *    Reads the next chunk from the ChunkStore.
   *
   *  If the reader is ordered, it will read the next chunk in the order of
   *  their seq numbers. If the reader is unordered, it will read the next
   *  chunk that has arrived in the store. This method respects cancellation
   *  and will return an error if the calling fiber has been cancelled.
   *
   * @param timeout
   *   An optional timeout for reading the next chunk. If not provided, the
   *   default timeout from `options_` will be used.
   *
   * @return
   *   The next chunk, or `std::nullopt` if there are no more chunks to read.
   *   If unsuccessful, returns an error status, which may indicate
   *   cancellation or timeout.
   *
   * @note
   *   Issues encountered while reading from the store are NOT returned here,
   *   but rather in the GetStatus() method.
   */
  absl::StatusOr<std::optional<Chunk>> Next(
      std::optional<absl::Duration> timeout = std::nullopt);

  /** @brief
   *    Same as Next(), but casts the chunk to the specified type `T`.
   *
   *  The cast is done using the `FromChunkAs<T>()` function, which must be
   *  defined for the type `T`. For custom types, this is ensured by providing
   *  the overload `EgltAssignInto(Chunk, T*)` in the global namespace or that
   *  of the type `T`.
   */
  template <typename T>
  absl::StatusOr<std::optional<T>> Next(
      std::optional<absl::Duration> timeout = std::nullopt) {
    ASSIGN_OR_RETURN(std::optional<Chunk> chunk, Next(timeout));
    if (!chunk) {
      return std::nullopt;
    }
    ASSIGN_OR_RETURN(T result, FromChunkAs<T>(*std::move(chunk)));
    return result;
  }

  // Definitions follow in the header for some well-known types. If the next
  // chunk cannot be read, this operator will crash. Prefer to use the Next()
  // method instead, which returns an absl::StatusOr<std::optional<T>>,
  // unless you are sure that the next chunk will always be available.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

  /** @brief
   *    Returns the status of the background prefetch loop.
   *
   * This status will be `absl::OkStatus()` unless and until the prefetch
   * loop has encountered an error, and then it will contain the error status.
   */
  absl::Status GetStatus() const;

 private:
  friend class Action;

  void EnsurePrefetchIsRunningOrHasCompleted()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<Chunk>> GetNextChunkFromBuffer(
      absl::Duration timeout) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::StatusOr<std::optional<std::pair<int, Chunk>>>
  GetNextSeqAndChunkFromBuffer(absl::Duration timeout)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<std::pair<int, Chunk>>>
  GetNextUnorderedSeqAndChunkFromStore() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status RunPrefetchLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  ChunkStore* const absl_nonnull chunk_store_;
  ChunkStoreReaderOptions options_;

  std::unique_ptr<thread::Fiber> fiber_;
  std::unique_ptr<thread::Channel<std::optional<std::pair<int, Chunk>>>>
      buffer_;
  int total_chunks_read_ = 0;

  absl::Status status_;
  mutable act::Mutex mu_;
};

// These specializations simply take the mutex and call the internal methods
// GetNextSeqAndChunkFromBuffer and GetNextChunkFromBuffer.
template <>
absl::StatusOr<std::optional<std::pair<int, Chunk>>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout);
template <>
absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout);

template <typename T>
ChunkStoreReader& operator>>(ChunkStoreReader& reader,
                             std::optional<T>& value) {
  absl::StatusOr<std::optional<T>> next_value = reader.Next<T>();
  CHECK_OK(next_value.status())
      << "Failed to read next value: " << next_value.status();
  value = *std::move(next_value);
  return reader;
}

template <typename T>
ChunkStoreReader& operator>>(ChunkStoreReader& reader, std::vector<T>& value) {
  while (true) {
    absl::StatusOr<std::optional<T>> status_or_element = reader.Next<T>();
    CHECK_OK(status_or_element.status())
        << "Failed to read next element: " << status_or_element.status();

    std::optional<T>& element = *status_or_element;
    if (!element) {
      break;
    }

    value.push_back(*std::move(element));
  }
  return reader;
}

template <typename T>
ChunkStoreReader* operator>>(ChunkStoreReader* absl_nonnull reader, T& value) {
  *reader >> value;
  return reader;
}

template <typename T>
std::unique_ptr<ChunkStoreReader>& operator>>(
    std::unique_ptr<ChunkStoreReader>& reader, T& value) {
  *reader >> value;
  return reader;
}

template <typename T>
std::shared_ptr<ChunkStoreReader>& operator>>(
    std::shared_ptr<ChunkStoreReader>& reader, T& value) {
  *reader >> value;
  return reader;
}

}  // namespace act

#endif  // ACTIONENGINE_STORES_CHUNK_STORE_READER_H_