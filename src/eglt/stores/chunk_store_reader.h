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

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/stores/chunk_store.h"

namespace eglt {

class ChunkStoreReader {
  // This class is thread-compatible. Calls to Run() and Reset() must be
  // externally synchronized and coordinated with calls to Next() / GetStatus().
  // In particular, status' reads/updates are protected, but users should not
  // assume that a call to GetStatus() will return the same status as the
  // previous call to Run(). Internally, status is updated in a background
  // thread, so it is possible for the status to change arbitrarily between
  // various calls.
 public:
  constexpr static absl::Duration kDefaultWaitTimeout =
      absl::InfiniteDuration();
  constexpr static absl::Duration kNoTimeout = absl::InfiniteDuration();

  explicit ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                            bool ordered = true, bool remove_chunks = true,
                            int n_chunks_to_buffer = -1,
                            absl::Duration timeout = kDefaultWaitTimeout)
      : chunk_store_(chunk_store),
        ordered_(ordered),
        remove_chunks_(remove_chunks),
        n_chunks_to_buffer_(n_chunks_to_buffer),
        timeout_(timeout),
        buffer_(std::make_unique<
                concurrency::Channel<std::optional<std::pair<int, Chunk>>>>(
            n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {}

  ~ChunkStoreReader() {
    std::unique_ptr<concurrency::Fiber> fiber;
    {
      concurrency::MutexLock lock(&mutex_);
      if (fiber_ == nullptr) {
        return;
      }
      fiber = std::move(fiber_);
      fiber_ = nullptr;
    }
    fiber->Cancel();
    fiber->Join();
  }

  // This class is not copyable or movable.
  ChunkStoreReader(const ChunkStoreReader& other) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader& other) = delete;

  absl::Status Run() {
    concurrency::MutexLock lock(&mutex_);

    if (fiber_ != nullptr) {
      status_ =
          absl::FailedPreconditionError("ChunkStoreReader is already running.");
    } else {
      status_ = absl::OkStatus();
      fiber_ = concurrency::NewTree({}, [this] { RunPrefetchLoop(); });
    }

    return status_;
  }

  template <typename T>
  std::optional<T> Next() {
    auto chunk = Next<Chunk>();

    if (!chunk || chunk->IsNull()) {
      return std::nullopt;
    }
    return DeserializeAs<T>(*std::move(chunk));
  }

  // definitions follow in the header for some well-known types.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return status_;
  }

 private:
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> NextInternal() const {
    const int next_read_offset = total_chunks_read_;

    // if (const int final_seq_id = chunk_store_->GetFinalSeqId();
    //     final_seq_id != -1 && next_read_offset > final_seq_id) {
    //   return std::nullopt;
    // }

    auto chunk_or_status =
        chunk_store_->GetByArrivalOrder(next_read_offset, kNoTimeout);
    if (!chunk_or_status.ok()) {
      return chunk_or_status.status();
    }

    auto chunk = *chunk_or_status;
    const int seq_id = chunk_store_->GetSeqIdForArrivalOffset(next_read_offset);
    if (chunk.get().IsNull()) {
      chunk_store_->Pop(seq_id);
      return std::nullopt;
    }

    return std::pair(seq_id, chunk.get());
  }

  void RunPrefetchLoop() {
    while (!concurrency::Cancelled()) {
      const auto final_seq_id = chunk_store_->GetFinalSeqId();
      if (final_seq_id >= 0 && total_chunks_read_ > final_seq_id) {
        UpdateStatus(absl::OkStatus());
        break;
      }

      std::optional<Chunk> next_chunk;
      int next_seq_id = -1;
      if (ordered_) {
        auto chunk = chunk_store_->Get(total_chunks_read_, timeout_);
        if (!chunk.ok()) {
          UpdateStatus(chunk.status());
          return;
        }
        next_chunk = chunk.value();
        next_seq_id = total_chunks_read_;
      } else {
        auto next_chunk_or_status = NextInternal();
        if (!next_chunk_or_status.ok()) {
          UpdateStatus(next_chunk_or_status.status());
          return;
        }
        auto next_seq_and_chunk = next_chunk_or_status.value();
        if (next_seq_and_chunk.has_value()) {
          std::tie(next_seq_id, next_chunk) = std::move(*next_seq_and_chunk);
          if (next_seq_id == -1) {
            next_seq_id = 0;
          }
        }
      }

      if (remove_chunks_ && next_seq_id >= 0) {
        chunk_store_->Pop(next_seq_id);
      }

      total_chunks_read_++;

      // on an std::nullopt, we are done and can close the buffer.
      if (!next_chunk.has_value()) {
        UpdateStatus(absl::OkStatus());
        break;
      }

      buffer_->writer()->Write(
          std::make_pair(next_seq_id, std::move(*next_chunk)));
    }
    // concurrency::MutexLock lock(&mutex_);
    buffer_->writer()->Close();
  }

  // This is primarily used by the prefetch loop to update the status of the
  // reader to avoid scoped locks in multiple places.
  void UpdateStatus(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    status_ = status;
  }

  ChunkStore* const absl_nonnull chunk_store_;
  const bool ordered_;
  const bool remove_chunks_;
  const int n_chunks_to_buffer_;
  const absl::Duration timeout_;

  std::unique_ptr<concurrency::Fiber> fiber_;
  std::unique_ptr<concurrency::Channel<std::optional<std::pair<int, Chunk>>>>
      buffer_;
  bool buffer_closed_ = false;
  int total_chunks_read_ = 0;

  absl::Status status_;
  mutable concurrency::Mutex mutex_;
};

template <>
inline std::optional<std::pair<int, Chunk>> ChunkStoreReader::Next() {
  if (fiber_ == nullptr) {
    Run().IgnoreError();
  }
  std::optional<std::pair<int, Chunk>> seq_and_chunk;
  bool ok;
  const int selected = concurrency::SelectUntil(
      absl::Now() + timeout_, {buffer_->reader()->OnRead(&seq_and_chunk, &ok),
                               concurrency::OnCancel()});
  if (selected == -1) {
    UpdateStatus(absl::DeadlineExceededError("Timed out waiting for chunk."));
    DLOG(INFO) << "Timed out waiting for chunk.";
    return std::nullopt;
  }
  if (selected == 1) {
    UpdateStatus(absl::CancelledError("Cancelled waiting for chunk."));
    DLOG(INFO) << "Cancelled waiting for chunk.";
    return std::nullopt;
  }
  UpdateStatus(absl::OkStatus());
  if (!ok) {
    return std::nullopt;
  }
  if (seq_and_chunk->second.IsNull()) {
    return std::nullopt;
  }
  return seq_and_chunk;
}

template <>
inline std::optional<Chunk> ChunkStoreReader::Next() {
  auto seq_and_chunk = Next<std::pair<int, Chunk>>();

  if (!seq_and_chunk.has_value()) {
    return std::nullopt;
  }
  return std::move(seq_and_chunk)->second;
}

template <typename T>
ChunkStoreReader& operator>>(ChunkStoreReader& reader,
                             std::optional<T>& value) {
  value = reader.Next<T>();
  return reader;
}

template <typename T>
ChunkStoreReader& operator>>(ChunkStoreReader& reader, std::vector<T>& value) {
  while (true) {
    auto chunk = reader.Next<T>();
    if (!chunk.has_value()) {
      break;
    }
    if (!reader.GetStatus().ok()) {
      LOG(ERROR) << "Error: " << reader.GetStatus() << "\n";
      break;
    }
    value.push_back(std::move(*chunk));
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