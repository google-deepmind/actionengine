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

#include "eglt/stores/chunk_store_reader.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/status_macros.h"

namespace eglt {

ChunkStoreReader::ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                                   bool ordered, bool remove_chunks,
                                   int n_chunks_to_buffer,
                                   absl::Duration timeout)
    : chunk_store_(chunk_store),
      ordered_(ordered),
      remove_chunks_(remove_chunks),
      n_chunks_to_buffer_(n_chunks_to_buffer),
      timeout_(timeout),
      buffer_(thread::Channel<std::optional<std::pair<int, Chunk>>>(
          n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {}

ChunkStoreReader::~ChunkStoreReader() {
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

template <>
absl::StatusOr<std::optional<std::pair<int, Chunk>>> ChunkStoreReader::Next() {
  eglt::MutexLock lock(&mu_);
  return GetNextSeqAndChunkFromBuffer();
}

template <>
absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::Next() {
  eglt::MutexLock lock(&mu_);
  return GetNextChunkFromBuffer();
}

absl::StatusOr<std::optional<std::pair<int, Chunk>>>
ChunkStoreReader::GetNextSeqAndChunkFromBuffer()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  EnsurePrefetchIsRunningOrHasCompleted();

  std::optional<std::pair<int, Chunk>> seq_and_chunk;
  bool ok;
  mu_.Unlock();
  const int selected = thread::SelectUntil(
      absl::Now() + timeout_,
      {buffer_.reader()->OnRead(&seq_and_chunk, &ok), thread::OnCancel()});
  mu_.Lock();

  if (selected == -1) {
    return absl::DeadlineExceededError("Timed out waiting for chunk.");
  }
  if (selected == 1) {
    return absl::CancelledError("Cancelled waiting for chunk.");
  }
  if (!ok) {
    return std::nullopt;
  }
  return seq_and_chunk;
}

absl::StatusOr<std::optional<std::pair<int, Chunk>>>
ChunkStoreReader::GetNextUnorderedSeqAndChunkFromStore() const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {

  const int next_read_offset = total_chunks_read_;

  // if (const int final_seq = chunk_store_->GetFinalSeq();
  //     final_seq != -1 && next_read_offset > final_seq) {
  //   return std::nullopt;
  // }

  mu_.Unlock();
  auto chunk_or_status =
      chunk_store_->GetByArrivalOrder(next_read_offset, kNoTimeout);
  mu_.Lock();

  if (!chunk_or_status.ok()) {
    return chunk_or_status.status();
  }

  const Chunk& chunk = *chunk_or_status;
  ASSIGN_OR_RETURN(const int seq,
                   chunk_store_->GetSeqForArrivalOffset(next_read_offset));
  if (chunk.IsNull()) {
    mu_.Unlock();
    absl::Status pop_status = chunk_store_->Pop(seq).status();
    mu_.Lock();
    if (!pop_status.ok()) {
      DLOG(ERROR) << "Failed to pop chunk at seq " << seq << ": " << pop_status;
      return pop_status;
    }
    return std::nullopt;
  }

  return std::pair(seq, chunk);
}

absl::Status ChunkStoreReader::RunPrefetchLoop()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  absl::Status status;

  while (!thread::Cancelled()) {
    if (const auto final_seq = chunk_store_->GetFinalSeqOrDie();
        final_seq >= 0 && total_chunks_read_ > final_seq) {
      status = absl::OkStatus();
      break;
    }

    // Either branch of the following code will read the next chunk and seq
    // into these variables.
    Chunk next_chunk;
    int next_seq = -1;

    if (ordered_) {
      mu_.Unlock();
      auto chunk = chunk_store_->Get(total_chunks_read_, timeout_);
      mu_.Lock();
      if (!chunk.ok()) {
        status = chunk.status();
        break;
      }
      next_chunk = *chunk;
      next_seq = total_chunks_read_;
    } else {
      auto next_unordered_seq_and_chunk =
          GetNextUnorderedSeqAndChunkFromStore();
      if (!next_unordered_seq_and_chunk.ok()) {
        status = next_unordered_seq_and_chunk.status();
        break;
      }
      if (!next_unordered_seq_and_chunk->has_value()) {
        // No more chunks to read.
        status = absl::OkStatus();
        break;
      }
      if (auto next_seq_and_chunk = next_unordered_seq_and_chunk.value();
          next_seq_and_chunk.has_value()) {
        std::tie(next_seq, next_chunk) = *std::move(next_seq_and_chunk);
        if (next_seq == -1) {
          next_seq = 0;
        }
      }
    }

    if (remove_chunks_ && next_seq >= 0) {
      mu_.Unlock();
      absl::Status pop_status = chunk_store_->Pop(next_seq).status();
      mu_.Lock();
      if (!pop_status.ok()) {
        status = pop_status;
        DLOG(ERROR) << "Failed to pop chunk: " << pop_status;
        break;
      }
    }

    ++total_chunks_read_;

    buffer_.writer()->Write(std::make_pair(next_seq, std::move(next_chunk)));
  }
  buffer_.writer()->Close();
  return status;
}

absl::Status ChunkStoreReader::GetStatus() const {
  eglt::MutexLock lock(&mu_);
  return status_;
}

void ChunkStoreReader::EnsurePrefetchIsRunningOrHasCompleted()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (fiber_ != nullptr) {
    return;
  }
  status_ = absl::OkStatus();
  fiber_ = thread::NewTree({}, [this] {
    eglt::MutexLock lock(&mu_);
    status_ = RunPrefetchLoop();
  });
}

absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::GetNextChunkFromBuffer()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> seq_and_chunk =
      GetNextSeqAndChunkFromBuffer();
  RETURN_IF_ERROR(seq_and_chunk.status());

  if (!seq_and_chunk->has_value()) {
    return std::nullopt;
  }
  if (seq_and_chunk->value().second.IsNull()) {
    // If the chunk is null, it means that the stream has ended.
    // TODO: this logic is not ideal, as it does not allow to distinguish
    //   between an empty chunk and the end of the stream. We should rethink it.
    return std::nullopt;
  }
  return (*std::move(seq_and_chunk))->second;
}

}  // namespace eglt