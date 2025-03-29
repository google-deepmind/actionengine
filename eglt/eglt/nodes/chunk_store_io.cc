#include "chunk_store_io.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>

#include <eglt/absl_headers.h>
#include <eglt/concurrency/concurrency.h>
#include <eglt/data/eg_structs.h>
#include <eglt/nodes/chunk_store.h>

namespace eglt {

ChunkStoreReader::ChunkStoreReader(ChunkStore* chunk_store, bool ordered,
                                   bool remove_chunks, int n_chunks_to_buffer,
                                   float timeout)
    : chunk_store_(chunk_store),
      ordered_(ordered),
      remove_chunks_(remove_chunks),
      n_chunks_to_buffer_(n_chunks_to_buffer),
      timeout_(timeout),
      buffer_(std::make_unique<
          concurrency::Channel<std::optional<std::pair<int, base::Chunk>>>>(
          n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {}

ChunkStoreReader::~ChunkStoreReader() {
  if (fiber_ == nullptr) {
    return;
  }
  fiber_->Cancel();
  concurrency::JoinOptimally(fiber_.get());
  fiber_ = nullptr;
}

absl::Status ChunkStoreReader::Run() {
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

absl::StatusOr<std::optional<std::pair<int, base::Chunk>>>
ChunkStoreReader::NextInternal() {
  if (chunk_store_ == nullptr) {
    return absl::FailedPreconditionError(
        "Chunk store is not initialized or has been destroyed.");
  }

  const int next_read_offset = total_chunks_read_;
  const auto final_seq_id = chunk_store_->GetFinalSeqId();

  if (final_seq_id != -1 && next_read_offset > final_seq_id) {
    return std::nullopt;
  }

  auto status =
      chunk_store_->WaitForArrivalOffset(next_read_offset, kNoTimeout);
  if (!status.ok()) {
    return status;
  }

  const auto seq_id = chunk_store_->GetSeqIdForArrivalOffset(next_read_offset);
  const auto fragment = chunk_store_->GetImmediately(seq_id);
  if (!fragment.ok()) {
    return fragment.status();
  }

  if (base::IsNullChunk(*fragment)) {
    chunk_store_->PopImmediately(seq_id).IgnoreError();
    return std::nullopt;
  }

  return std::pair(seq_id, *fragment);
}

void ChunkStoreReader::RunPrefetchLoop() {
  while (!concurrency::Cancelled()) {
    if (chunk_store_ == nullptr) {
      UpdateStatus(absl::FailedPreconditionError(
          "Chunk store is not initialized or has been destroyed."));
      break;
    }

    const auto final_seq_id = chunk_store_->GetFinalSeqId();
    if (final_seq_id >= 0 && total_chunks_read_ > final_seq_id) {
      UpdateStatus(absl::OkStatus());
      break;
    }

    std::optional<base::Chunk> next_chunk;
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
      chunk_store_->Pop(next_seq_id, kNoTimeout).IgnoreError();
    }

    // on an std::nullopt, we are done and can close the buffer.
    if (!next_chunk.has_value()) {
      UpdateStatus(absl::OkStatus());
      break;
    }

    buffer_->GetWriter()->Write(
        std::make_pair(next_seq_id, std::move(*next_chunk)));
    total_chunks_read_++;
  }
  buffer_->GetWriter()->Close();
}

void ChunkStoreReader::Join(bool cancel) {
  if (fiber_ == nullptr) {
    return;
  }

  if (cancel) {
    fiber_->Cancel();
  }
  concurrency::JoinOptimally(fiber_.get());
  fiber_ = nullptr;
}

ChunkStoreWriter::ChunkStoreWriter(ChunkStore* chunk_store,
                                   int n_chunks_to_buffer)
    : chunk_store_(chunk_store),
      n_chunks_to_buffer_(n_chunks_to_buffer),
      buffer_(std::make_unique<
          concurrency::Channel<std::optional<base::NodeFragment>>>(
          n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {}

ChunkStoreWriter::~ChunkStoreWriter() { TerminateSafely(); }

void ChunkStoreWriter::FinishWrites() {
  concurrency::MutexLock lock(&mutex_);
  accepts_puts_ = false;
  if (status_.ok()) {
    if (!buffer_->GetWriter()->WriteUnlessCancelled(std::nullopt)) {
      status_ = absl::CancelledError("Cancelled.");
    }
  }
  SafelyCloseBuffer();
}

void ChunkStoreWriter::EnsureWriteLoop() {
  if (fiber_ == nullptr && accepts_puts_) {
    fiber_ = concurrency::NewTree({}, [this] { RunWriteLoop(); });
  }
}

void ChunkStoreWriter::SafelyCloseBuffer() {
  accepts_puts_ = false;
  if (!buffer_writer_closed_ && buffer_ != nullptr) {
    buffer_->GetWriter()->Close();
    buffer_writer_closed_ = true;
  }
}

void ChunkStoreWriter::RunWriteLoop() {
  while (!concurrency::Cancelled()) {
    if (chunk_store_ == nullptr) {
      UpdateStatus(absl::FailedPreconditionError(
          "Chunk store is not initialized or has been destroyed."));
      break;
    }

    std::optional<base::NodeFragment> next_fragment;
    bool ok;

    if (concurrency::Select({buffer_->GetReader()->OnRead(&next_fragment, &ok),
                             concurrency::OnCancel()}) == 1) {
      {
        concurrency::MutexLock lock(&mutex_);
        status_ = absl::CancelledError("Cancelled.");
      }
      break;
    }

    // we only enter this case if the buffer is closed and empty,
    // so we're done.
    if (!ok) {
      {
        concurrency::MutexLock lock(&mutex_);
        accepts_puts_ = false;
        status_ = absl::OkStatus();
      }
      break;
    }

    // if we receive a nullopt, then we are done and can communicate this to
    // the fragment store and close writes to the buffer.
    if (!next_fragment.has_value()) {
      {
        concurrency::MutexLock lock(&mutex_);

        accepts_puts_ = false;
        SafelyCloseBuffer();
        chunk_store_->NotifyAllWaiters();
        status_ = absl::OkStatus();
      }
      break;
    }

    if (!GetStatus().ok()) {
      break;
    }

    auto status = chunk_store_->Put(/*seq_id=*/next_fragment->seq,
        /*chunk=*/std::move(*next_fragment->chunk),
        /*final=*/!next_fragment->continued);
    if (!status.ok()) {
      UpdateStatus(status);
      break;
    }

    // TODO: currently the semantics of the null chunk is not clearly
    // established: we need it both to indicate "normally" ended stream and
    // to indicate "closed in an error state"
    // bool is_null = base::IsNullChunk(*fragment.chunk);
    auto final_seq_id = chunk_store_->GetFinalSeqId();
    ++total_chunks_written_;
    if (final_seq_id >= 0 &&
        total_chunks_written_ > chunk_store_->GetFinalSeqId()) {
      buffer_->GetWriter()->Write(std::nullopt);
    }

    if (!GetStatus().ok()) {
      break;
    }
  }
}

void ChunkStoreWriter::TerminateSafely() {
  std::unique_ptr<concurrency::Fiber> fiber;
  {
    concurrency::MutexLock lock(&mutex_);
    SafelyCloseBuffer();
    fiber = std::move(fiber_);
    fiber_ = nullptr;
  }
  if (fiber != nullptr) {
    fiber->Cancel();
    concurrency::JoinOptimally(fiber.get());
  }
}

}  // namespace eglt
