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

#ifndef EGLT_STORES_CHUNK_STORE_READER_H_
#define EGLT_STORES_CHUNK_STORE_READER_H_

#include <algorithm>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/data/serialization.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/status_macros.h"

namespace eglt {

struct ChunkStoreReaderOptions {
  /// Whether to read chunks in the explicit seq order.
  /// If false, chunks will be read in the order they arrive in the store.
  /// This is useful for streaming data where the order of chunks is not
  /// important, such as parallel independent processing of chunks.
  bool ordered = true;

  /// Whether to remove chunks from the store after reading them.
  /// If false, chunks will remain in the store after reading. This is useful
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

class ChunkStoreReader {
 public:
  explicit ChunkStoreReader(
      ChunkStore* absl_nonnull chunk_store,
      ChunkStoreReaderOptions options = ChunkStoreReaderOptions());

  // This class is not copyable or movable.
  ChunkStoreReader(const ChunkStoreReader&) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader&) = delete;

  ~ChunkStoreReader();

  void Cancel() const;

  void SetOptions(ChunkStoreReaderOptions options);

  const ChunkStoreReaderOptions& GetOptions() const { return options_; }

  absl::StatusOr<std::optional<Chunk>> Next(
      std::optional<absl::Duration> timeout = std::nullopt);

  template <typename T>
  absl::StatusOr<std::optional<T>> Next(
      std::optional<absl::Duration> timeout = std::nullopt) {
    ASSIGN_OR_RETURN(std::optional<Chunk> chunk, Next(timeout));
    ASSIGN_OR_RETURN(T result, FromChunkAs<T>(*std::move(chunk)));
    return result;
  }

  // Definitions follow in the header for some well-known types. If the next
  // chunk cannot be read, this operator will crash.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

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
  bool buffer_closed_ = false;
  int total_chunks_read_ = 0;

  absl::Status status_;
  mutable eglt::Mutex mu_;
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

}  // namespace eglt

#endif  // EGLT_STORES_CHUNK_STORE_READER_H_