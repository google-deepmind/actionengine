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

#include "eglt/stores/chunk_store_writer.h"

namespace eglt {

ChunkStoreWriter::ChunkStoreWriter(ChunkStore* chunk_store,
                                   int n_chunks_to_buffer)
    : chunk_store_(chunk_store),
      n_chunks_to_buffer_(n_chunks_to_buffer),
      buffer_(thread::Channel<std::optional<NodeFragment>>(
          n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {
  accepts_puts_ = true;
}

ChunkStoreWriter::~ChunkStoreWriter() {
  eglt::MutexLock lock(&mu_);

  accepts_puts_ = false;
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

absl::Status ChunkStoreWriter::GetStatus() const {
  eglt::MutexLock lock(&mu_);
  return status_;
}

void ChunkStoreWriter::EnsureWriteLoop() {
  if (fiber_ == nullptr && accepts_puts_) {
    fiber_ = thread::NewTree({}, [this] {
      eglt::MutexLock lock(&mu_);
      status_ = RunWriteLoop();
    });
  }
}

void ChunkStoreWriter::SafelyCloseBuffer() {
  accepts_puts_ = false;
  if (!buffer_writer_closed_) {
    buffer_.writer()->Close();
    buffer_writer_closed_ = true;
  }
}

absl::Status ChunkStoreWriter::RunWriteLoop() {
  absl::Status status = absl::OkStatus();

  while (!thread::Cancelled()) {
    std::optional<NodeFragment> next_fragment;
    bool buffer_open;

    mu_.Unlock();
    thread::Select({buffer_.reader()->OnRead(&next_fragment, &buffer_open),
                    thread::OnCancel()});
    mu_.Lock();

    if (thread::Cancelled()) {
      SafelyCloseBuffer();
    }

    // we only enter this case if the buffer is closed and empty,
    // so we're done.
    if (!buffer_open) {
      status = absl::OkStatus();
      break;
    }

    // if we receive a nullopt, then we are done and can communicate this to
    // the fragment store and close writes to the buffer.
    if (!next_fragment.has_value()) {
      SafelyCloseBuffer();
      status = absl::OkStatus();
      break;
    }

    mu_.Unlock();
    status = chunk_store_->Put(/*seq=*/next_fragment->seq,
                               /*chunk=*/
                               std::move(*next_fragment->chunk),
                               /*final=*/
                               !next_fragment->continued);
    mu_.Lock();

    if (!status.ok()) {
      break;
    }

    ++total_chunks_written_;
    if (final_seq_ >= 0 && total_chunks_written_ > final_seq_) {
      if (!buffer_writer_closed_) {
        buffer_.writer()->WriteUnlessCancelled(std::nullopt);
      }
    }
  }
  accepts_puts_ = false;
  status.Update(chunk_store_->CloseWritesWithStatus(status));
  return status;
}

absl::StatusOr<int> ChunkStoreWriter::Put(Chunk value, int seq, bool final)
    ABSL_LOCKS_EXCLUDED(mu_) {
  eglt::MutexLock lock(&mu_);
  if (!accepts_puts_) {
    return absl::FailedPreconditionError(
        "Put was called on a writer that does not accept more puts.");
  }

  if (seq != -1 && final_seq_ != -1 && seq > final_seq_) {
    return absl::FailedPreconditionError(
        "Cannot put chunks with seq > final_seq.");
  }

  if (value.IsNull() && !final) {
    return absl::FailedPreconditionError(
        "Cannot put a null chunk without also finalizing.");
  }

  int written_seq = seq;
  if (seq == -1) {
    written_seq = total_chunks_put_;
  }
  total_chunks_put_++;

  if (final) {
    final_seq_ = written_seq;
  }

  EnsureWriteLoop();
  if (!buffer_.writer()->WriteUnlessCancelled(NodeFragment{
          .chunk = std::move(value),
          .seq = written_seq,
          .continued = !final,
      })) {
    accepts_puts_ = false;
    return absl::CancelledError("Cancelled.");
  }

  return written_seq;
}

template <>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, Chunk value) {
  // value is moved on the next line, so we must grab IsNull before that.
  const bool final = value.IsNull();
  CHECK_OK(writer.Put(std::move(value), /*seq=*/-1, /*final=*/final).status());
  return writer;
}

template <>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                             std::pair<Chunk, int> value) {
  bool final = value.first.IsNull();
  auto [data_value, seq] = std::move(value);
  CHECK_OK(writer.Put(std::move(data_value), seq, /*final=*/final).status());
  return writer;
}

}  // namespace eglt