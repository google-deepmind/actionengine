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

class ChunkStoreReader {
 public:
  constexpr static absl::Duration kDefaultWaitTimeout =
      absl::InfiniteDuration();
  constexpr static absl::Duration kNoTimeout = absl::InfiniteDuration();

  explicit ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                            bool ordered = true, bool remove_chunks = true,
                            int n_chunks_to_buffer = -1,
                            absl::Duration timeout = kDefaultWaitTimeout);

  // This class is not copyable or movable.
  ChunkStoreReader(const ChunkStoreReader&) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader&) = delete;

  ~ChunkStoreReader();

  void Cancel() const {
    eglt::MutexLock lock(&mu_);
    if (fiber_ != nullptr) {
      fiber_->Cancel();
    }
  }

  template <typename T>
  absl::StatusOr<std::optional<T>> Next() {
    std::optional<Chunk> chunk;
    {
      eglt::MutexLock lock(&mu_);
      ASSIGN_OR_RETURN(chunk, GetNextChunkFromBuffer());
      if (!chunk || chunk->IsNull()) {
        return std::nullopt;
      }
    }

    ASSIGN_OR_RETURN(T result, FromChunkAs<T>(*std::move(chunk)));
    return result;
  }

  // Definitions follow in the header for some well-known types. If the next
  // chunk cannot be read, this operator will crash.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

  absl::Status GetStatus() const;

 private:
  void EnsurePrefetchIsRunningOrHasCompleted()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<Chunk>> GetNextChunkFromBuffer()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::StatusOr<std::optional<std::pair<int, Chunk>>>
  GetNextSeqAndChunkFromBuffer() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<std::pair<int, Chunk>>>
  GetNextUnorderedSeqAndChunkFromStore() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status RunPrefetchLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  ChunkStore* const absl_nonnull chunk_store_;
  const bool ordered_;
  const bool remove_chunks_;
  const int n_chunks_to_buffer_;
  const absl::Duration timeout_;

  std::unique_ptr<thread::Fiber> fiber_;
  thread::Channel<std::optional<std::pair<int, Chunk>>> buffer_;
  bool buffer_closed_ = false;
  int total_chunks_read_ = 0;

  absl::Status status_;
  mutable eglt::Mutex mu_;
};

// These specializations simply take the mutex and call the internal methods
// GetNextSeqAndChunkFromBuffer and GetNextChunkFromBuffer.
template <>
absl::StatusOr<std::optional<std::pair<int, Chunk>>> ChunkStoreReader::Next();
template <>
absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::Next();

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