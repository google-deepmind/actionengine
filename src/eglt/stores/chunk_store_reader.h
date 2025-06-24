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
        buffer_(thread::Channel<std::optional<std::pair<int, Chunk>>>(
            n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {}

  // This class is not copyable or movable.
  ChunkStoreReader(const ChunkStoreReader&) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader&) = delete;

  ~ChunkStoreReader() {
    eglt::MutexLock lock(&mu_);
    if (fiber_ == nullptr) {
      return;
    }

    const std::unique_ptr<thread::Fiber> fiber = std::move(fiber_);
    fiber_ = nullptr;

    fiber->Cancel();

    mu_.Unlock();
    fiber->Join();
    mu_.Lock();
  }

  template <typename T>
  std::optional<T> Next() {
    auto chunk = Next<Chunk>();

    if (!chunk || chunk->IsNull()) {
      return std::nullopt;
    }
    auto result = FromChunkAs<T>(*std::move(chunk));
    if (!result.ok()) {
      UpdateStatus(result.status());
      LOG(ERROR) << "Failed to convert chunk: " << result.status();
      return std::nullopt;
    }
    return std::move(*result);
  }

  // definitions follow in the header for some well-known types.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(mu_) {
    eglt::MutexLock lock(&mu_);
    return status_;
  }

 private:
  absl::Status Run() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (fiber_ != nullptr) {
      status_ =
          absl::FailedPreconditionError("ChunkStoreReader is already running.");
    } else {
      status_ = absl::OkStatus();
      fiber_ = thread::NewTree({}, [this] {
        eglt::MutexLock lock(&mu_);
        RunPrefetchLoop();
      });
    }

    return status_;
  }

  absl::StatusOr<std::optional<std::pair<int, Chunk>>> NextInternal() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    const int next_read_offset = total_chunks_read_;

    // if (const int final_seq_id = chunk_store_->GetFinalSeqId();
    //     final_seq_id != -1 && next_read_offset > final_seq_id) {
    //   return std::nullopt;
    // }

    mu_.Unlock();
    auto chunk_or_status =
        chunk_store_->GetByArrivalOrder(next_read_offset, kNoTimeout);
    mu_.Lock();

    if (!chunk_or_status.ok()) {
      return chunk_or_status.status();
    }

    const auto chunk = *chunk_or_status;
    const int seq_id = chunk_store_->GetSeqIdForArrivalOffset(next_read_offset);
    if (chunk.get().IsNull()) {
      mu_.Unlock();
      chunk_store_->Pop(seq_id);
      mu_.Lock();
      return std::nullopt;
    }

    return std::pair(seq_id, chunk.get());
  }

  void RunPrefetchLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    while (!thread::Cancelled()) {
      if (const auto final_seq_id = chunk_store_->GetFinalSeqId();
          final_seq_id >= 0 && total_chunks_read_ > final_seq_id) {
        status_ = absl::OkStatus();
        break;
      }

      std::optional<Chunk> next_chunk;
      int next_seq_id = -1;
      if (ordered_) {
        mu_.Unlock();
        auto chunk = chunk_store_->Get(total_chunks_read_, timeout_);
        mu_.Lock();

        if (!chunk.ok()) {
          status_ = chunk.status();
          return;
        }
        next_chunk = chunk.value();
        next_seq_id = total_chunks_read_;
      } else {
        auto next_chunk_or_status = NextInternal();
        if (!next_chunk_or_status.ok()) {
          status_ = next_chunk_or_status.status();
          return;
        }
        if (auto next_seq_and_chunk = next_chunk_or_status.value();
            next_seq_and_chunk.has_value()) {
          std::tie(next_seq_id, next_chunk) = std::move(*next_seq_and_chunk);
          if (next_seq_id == -1) {
            next_seq_id = 0;
          }
        }
      }

      if (remove_chunks_ && next_seq_id >= 0) {
        mu_.Unlock();
        chunk_store_->Pop(next_seq_id);
        mu_.Lock();
      }

      total_chunks_read_++;

      // on an std::nullopt, we are done and can close the buffer.
      if (!next_chunk.has_value()) {
        status_ = absl::OkStatus();
        break;
      }

      buffer_.writer()->Write(
          std::make_pair(next_seq_id, std::move(*next_chunk)));
    }
    buffer_.writer()->Close();
  }

  // This is primarily used by the prefetch loop to update the status of the
  // reader to avoid scoped locks in multiple places.
  void UpdateStatus(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mu_) {
    eglt::MutexLock lock(&mu_);
    status_ = status;
  }

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

template <>
inline std::optional<std::pair<int, Chunk>> ChunkStoreReader::Next()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (fiber_ == nullptr) {
    Run().IgnoreError();
  }
  std::optional<std::pair<int, Chunk>> seq_and_chunk;
  bool ok;
  mu_.Unlock();
  const int selected = thread::SelectUntil(
      absl::Now() + timeout_,
      {buffer_.reader()->OnRead(&seq_and_chunk, &ok), thread::OnCancel()});
  mu_.Lock();

  if (selected == -1) {
    status_ = absl::DeadlineExceededError("Timed out waiting for chunk.");
    DLOG(INFO) << "Timed out waiting for chunk.";
    return std::nullopt;
  }
  if (selected == 1) {
    status_ = absl::CancelledError("Cancelled waiting for chunk.");
    DLOG(INFO) << "Cancelled waiting for chunk.";
    return std::nullopt;
  }
  status_ = absl::OkStatus();
  if (!ok) {
    return std::nullopt;
  }
  if (seq_and_chunk->second.IsNull()) {
    return std::nullopt;
  }
  return seq_and_chunk;
}

template <>
inline std::optional<Chunk> ChunkStoreReader::Next() ABSL_LOCKS_EXCLUDED(mu_) {
  eglt::MutexLock lock(&mu_);
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