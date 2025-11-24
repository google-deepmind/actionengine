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

#include "actionengine/stores/chunk_store_reader.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>

#include <absl/log/log.h>
#include <absl/time/clock.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/util/status_macros.h"

namespace act {

// struct StoreReaderPoolTask {
//   int read_offset;
//   ChunkStore* absl_nonnull chunk_store;
//   bool pop = false;
//   bool cancelled = false;
//   absl::AnyInvocable<void(
//       absl::StatusOr<std::optional<std::pair<int, Chunk>>> /*result*/,
//       bool /*cancelled*/) &&>
//       on_done;
// };
//
// class StoreReaderFiberPool {
//  public:
//  private:
//   thread::Channel<std::shared_ptr<StoreReaderPoolTask>> tasks_;
// };

ChunkStoreReader::ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                                   ChunkStoreReaderOptions options)
    : chunk_store_(chunk_store), options_(std::move(options)) {}

ChunkStoreReader::~ChunkStoreReader() {
  act::MutexLock lock(&mu_);
  if (fiber_ == nullptr) {
    return;
  }

  const std::unique_ptr<thread::Fiber> fiber = std::move(fiber_);
  fiber_ = nullptr;

  fiber->Cancel();

  mu_.unlock();
  fiber->Join();
  mu_.lock();

  while (pending_ops_ > 0) {
    cv_.Wait(&mu_);
  }
}

void ChunkStoreReader::Cancel() const {
  act::MutexLock lock(&mu_);
  if (fiber_ != nullptr) {
    fiber_->Cancel();
    chunk_store_->Notify();
  }
}

void ChunkStoreReader::SetOptions(const ChunkStoreReaderOptions& options) {
  act::MutexLock lock(&mu_);
  CHECK(fiber_ == nullptr)
      << "Cannot set options after the reader has been started.";
  options_ = options;
}

absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(std::optional<Chunk> chunk,
                   GetNextChunkFromBuffer(timeout.value_or(options_.timeout)));
  if (!chunk || chunk->IsNull()) {
    return std::nullopt;
  }
  if (chunk->metadata && chunk->metadata->mimetype == "__status__") {
    absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(*chunk);
    if (!status.ok()) {
      return status.status();
    }
  }
  return chunk;
}

absl::StatusOr<std::optional<NodeFragment>> ChunkStoreReader::NextFragment(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> seq_and_chunk =
      GetNextSeqAndChunkFromBuffer(timeout.value_or(options_.timeout));
  RETURN_IF_ERROR(seq_and_chunk.status());
  ASSIGN_OR_RETURN(const int64_t final_seq, chunk_store_->GetFinalSeq());
  if (!seq_and_chunk->has_value()) {
    return std::nullopt;
  }
  if (seq_and_chunk->value().second.IsNull()) {
    // If the chunk is null, it means that the stream has ended.
    // TODO: this logic is not ideal, as it does not allow to distinguish
    //   between an empty chunk and the end of the stream. We should rethink it.
    return std::nullopt;
  }
  auto& chunk = seq_and_chunk->value().second;
  const int seq = seq_and_chunk->value().first;
  return NodeFragment{std::string(chunk_store_->GetId()), std::move(chunk), seq,
                      final_seq == -1 || seq != final_seq};
}

template <>
absl::StatusOr<std::optional<std::pair<int, Chunk>>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  return GetNextSeqAndChunkFromBuffer(timeout.value_or(options_.timeout));
}

absl::StatusOr<std::optional<std::pair<int, Chunk>>>
ChunkStoreReader::GetNextSeqAndChunkFromBuffer(absl::Duration timeout)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  EnsurePrefetchIsRunningOrHasCompleted();

  std::optional<std::pair<int, Chunk>> seq_and_chunk;
  bool ok;
  ++pending_ops_;
  mu_.unlock();
  const int selected = thread::SelectUntil(
      absl::Now() + timeout,
      {buffer_->reader()->OnRead(&seq_and_chunk, &ok), thread::OnCancel()});
  mu_.lock();
  --pending_ops_;
  cv_.SignalAll();

  if (selected == -1) {
    return absl::DeadlineExceededError("Timed out waiting for chunk.");
  }
  if (thread::Cancelled()) {
    return absl::CancelledError("Cancelled waiting for chunk.");
  }

  if (!ok) {
    // If the prefetcher finished with an error, return the error.
    RETURN_IF_ERROR(status_);
    // Otherwise it simply finished reading.
    return std::nullopt;
  }
  if (seq_and_chunk) {
    if (const Chunk& chunk = seq_and_chunk->second;
        chunk.metadata && chunk.metadata->mimetype == "__status__") {
      if (absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(chunk);
          !status.ok()) {
        return status.status();
      }
    }
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

  mu_.unlock();
  auto chunk_or_status = chunk_store_->GetByArrivalOrder(
      next_read_offset, absl::InfiniteDuration());
  mu_.lock();

  if (!chunk_or_status.ok()) {
    return chunk_or_status.status();
  }

  const Chunk& chunk = *chunk_or_status;
  ASSIGN_OR_RETURN(const int seq,
                   chunk_store_->GetSeqForArrivalOffset(next_read_offset));
  if (chunk.IsNull()) {
    mu_.unlock();
    absl::Status pop_status = chunk_store_->Pop(seq).status();
    mu_.lock();
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
  total_chunks_read_ = options_.start_seq_or_offset;

  while (!thread::Cancelled()) {
    if (const absl::StatusOr<int64_t> final_seq = chunk_store_->GetFinalSeq();
        !final_seq.ok()) {
      status = final_seq.status();
      break;
    } else if (*final_seq >= 0 && total_chunks_read_ > *final_seq) {
      // If we have read all chunks, we can stop.
      status = absl::OkStatus();
      break;
    }

    // Either branch of the following code will read the next chunk and seq
    // into these variables.
    Chunk next_chunk;
    int next_seq = -1;

    if (options_.ordered) {
      mu_.unlock();
      auto chunk =
          chunk_store_->Get(total_chunks_read_, absl::InfiniteDuration());
      mu_.lock();
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

    if (options_.remove_chunks && next_seq >= 0) {
      mu_.unlock();
      absl::Status pop_status = chunk_store_->Pop(next_seq).status();
      mu_.lock();
      if (!pop_status.ok()) {
        status = pop_status;
        DLOG(ERROR) << "Failed to pop chunk: " << pop_status;
        break;
      }
    }

    ++total_chunks_read_;

    buffer_->writer()->Write(std::make_pair(next_seq, std::move(next_chunk)));
  }

  buffer_->writer()->Close();

  if (thread::Cancelled()) {
    status.Update(absl::CancelledError("Prefetcher fiber was cancelled."));
  }
  return status;
}

template <>
absl::StatusOr<std::optional<absl::Status>>
ChunkStoreReader::Next<absl::Status>(std::optional<absl::Duration> timeout) {
  ASSIGN_OR_RETURN(std::optional<Chunk> chunk, Next(timeout));
  if (!chunk) {
    return std::nullopt;
  }
  if (chunk->metadata && chunk->metadata->mimetype == "__status__") {
    absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(*chunk);
    if (!status.ok()) {
      return status.status();
    }
    if (!status->ok()) {
      return *status;
    }

    absl::StatusOr<absl::Status> result;
    result.emplace() = absl::OkStatus();
    return result;
  }
  return absl::InvalidArgumentError(
      "Expected a status chunk, but got a regular chunk.");
}

absl::Status ChunkStoreReader::GetStatus() const {
  act::MutexLock lock(&mu_);
  return status_;
}

void ChunkStoreReader::EnsurePrefetchIsRunningOrHasCompleted()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (fiber_ != nullptr || buffer_ != nullptr) {
    return;
  }

  buffer_ =
      std::make_unique<thread::Channel<std::optional<std::pair<int, Chunk>>>>(
          options_.n_chunks_to_buffer);

  status_ = absl::OkStatus();
  fiber_ = thread::NewTree({}, [this] {
    act::MutexLock lock(&mu_);
    status_ = RunPrefetchLoop();
  });
}

absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::GetNextChunkFromBuffer(
    absl::Duration timeout) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> seq_and_chunk =
      GetNextSeqAndChunkFromBuffer(timeout);
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
  return std::move((*seq_and_chunk)->second);
}

}  // namespace act