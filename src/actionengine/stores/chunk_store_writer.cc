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

#include "actionengine/stores/chunk_store_writer.h"

#include <cstdint>
#include <string>
#include <string_view>

#include <absl/log/check.h>

namespace act {

ChunkStoreWriter::ChunkStoreWriter(ChunkStore* chunk_store,
                                   int n_chunks_to_buffer)
    : chunk_store_(chunk_store),
      n_chunks_to_buffer_(n_chunks_to_buffer),
      buffer_(thread::Channel<std::optional<NodeFragment>>(
          n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {
  accepts_puts_ = true;
}

ChunkStoreWriter::~ChunkStoreWriter() {
  act::MutexLock lock(&mu_);

  CancelInternal();
  JoinInternal();
}

absl::Status ChunkStoreWriter::GetStatus() const {
  act::MutexLock lock(&mu_);
  return status_;
}

void ChunkStoreWriter::BindPeers(
    absl::flat_hash_map<std::string, WireStream*> peers) {
  act::MutexLock lock(&mu_);
  if (peers.empty() && !peers_.empty()) {
    // if we are unbinding all peers, we should ensure that the write loop
    // has finished, so that we finish all writes made to the previous peers.
    JoinInternal();
  }

  peers_ = std::move(peers);
}

void ChunkStoreWriter::EnsureWriteLoop() {
  if (fiber_ == nullptr && accepts_puts_) {
    fiber_ = thread::NewTree({}, [this] {
      act::MutexLock lock(&mu_);
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

    mu_.unlock();
    thread::Select({buffer_.reader()->OnRead(&next_fragment, &buffer_open),
                    thread::OnCancel()});
    mu_.lock();

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

    auto peers = peers_;  // copy for use outside the lock

    mu_.unlock();

    if (next_fragment) {
      next_fragment->id = chunk_store_->GetId();
      WireMessage message_for_peers;
      message_for_peers.node_fragments.push_back(*next_fragment);
      for (const auto& [peer_id, peer] : peers) {
        status.Update(peer->Send(message_for_peers));
      }
      if (!status.ok()) {
        mu_.lock();
        break;
      }
    }

    status = chunk_store_->Put(/*seq=*/next_fragment->seq.value_or(-1),
                               /*chunk=*/
                               std::move(next_fragment->GetChunkOrDie()),
                               /*final=*/
                               !next_fragment->continued);
    mu_.lock();

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
  act::MutexLock lock(&mu_);
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
          .data = std::move(value),
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

}  // namespace act