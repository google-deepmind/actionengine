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

#ifndef EGLT_STORES_CHUNK_STORE_WRITER_H_
#define EGLT_STORES_CHUNK_STORE_WRITER_H_

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

class ChunkStoreWriter {
  // This class is thread-safe. Public methods can be called concurrently from
  // different threads. Chunk store and buffer are only accessed by the writer
  // thread, and are only set on construction. Other fields are only accessed by
  // the internal writer fiber, so access is always synchronous.
 public:
  explicit ChunkStoreWriter(ChunkStore* absl_nonnull chunk_store,
                            int n_chunks_to_buffer = -1)
      : chunk_store_(chunk_store),
        n_chunks_to_buffer_(n_chunks_to_buffer),
        buffer_(
            std::make_unique<concurrency::Channel<std::optional<NodeFragment>>>(
                n_chunks_to_buffer == -1 ? SIZE_MAX : n_chunks_to_buffer)) {
    accepts_puts_ = true;
  }
  ~ChunkStoreWriter() {
    std::unique_ptr<concurrency::Fiber> fiber;
    {
      concurrency::MutexLock lock(&mutex_);
      accepts_puts_ = false;
      if (fiber_ == nullptr) {
        return;
      }
      fiber = std::move(fiber_);
      fiber_ = nullptr;
    }
    fiber->Cancel();
    fiber->Join();
  }

  ChunkStoreWriter(const ChunkStoreWriter& other) = delete;
  ChunkStoreWriter& operator=(const ChunkStoreWriter& other) = delete;

  template <typename T>
  absl::StatusOr<int> Put(T value, int seq = -1, bool final = false)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    return Put(Serialize(std::move(value)), seq, final);
  }

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return status_;
  }

  template <typename T>
  friend ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, T value) {
    Chunk chunk = Serialize(std::move(value));
    const bool final = chunk.IsNull();
    writer.Put(std::move(chunk), -1, final).IgnoreError();
    return writer;
  }

 private:
  void EnsureWriteLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    if (fiber_ == nullptr && accepts_puts_) {
      fiber_ = concurrency::NewTree({}, [this] { RunWriteLoop(); });
    }
  }

  void SafelyCloseBuffer() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    accepts_puts_ = false;
    if (!buffer_writer_closed_ && buffer_ != nullptr) {
      buffer_->writer()->Close();
      buffer_writer_closed_ = true;
    }
  }

  void RunWriteLoop() {
    while (!concurrency::Cancelled()) {
      std::optional<NodeFragment> next_fragment;
      bool ok;

      if (concurrency::Select({buffer_->reader()->OnRead(&next_fragment, &ok),
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
          status_ = absl::OkStatus();
        }
        break;
      }

      // if we receive a nullopt, then we are done and can communicate this to
      // the fragment store and close writes to the buffer.
      if (!next_fragment.has_value()) {
        {
          concurrency::MutexLock lock(&mutex_);
          SafelyCloseBuffer();
          status_ = absl::OkStatus();
        }
        break;
      }

      if (!GetStatus().ok()) {
        break;
      }

      auto status = chunk_store_->Put(/*seq_id=*/next_fragment->seq,
                                      /*chunk=*/
                                      std::move(*next_fragment->chunk),
                                      /*final=*/
                                      !next_fragment->continued);
      if (!status.ok()) {
        {
          concurrency::MutexLock lock(&mutex_);
          status_ = status;
        }
        break;
      }

      ++total_chunks_written_;
      int final_seq_id;
      {
        concurrency::MutexLock lock(&mutex_);
        final_seq_id = final_seq_id_;
      }
      if (final_seq_id >= 0 && total_chunks_written_ > final_seq_id) {
        {
          concurrency::MutexLock lock(&mutex_);
          if (!buffer_writer_closed_) {
            buffer_->writer()->WriteUnlessCancelled(std::nullopt);
          }
        }
      }

      if (!GetStatus().ok()) {
        break;
      }
    }
    concurrency::MutexLock lock(&mutex_);
    accepts_puts_ = false;
    chunk_store_->NoFurtherPuts();
  }

  void UpdateStatus(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    status_ = status;
  }

  ChunkStore* absl_nonnull const chunk_store_ = nullptr;
  const int n_chunks_to_buffer_;

  int final_seq_id_ ABSL_GUARDED_BY(mutex_) = -1;
  int total_chunks_put_ ABSL_GUARDED_BY(mutex_) = 0;

  bool accepts_puts_ ABSL_GUARDED_BY(mutex_) = true;
  bool buffer_writer_closed_ ABSL_GUARDED_BY(mutex_) = false;

  int total_chunks_written_ = 0;

  std::unique_ptr<concurrency::Fiber> fiber_ ABSL_GUARDED_BY(mutex_);
  std::unique_ptr<concurrency::Channel<std::optional<NodeFragment>>> buffer_;
  absl::Status status_ ABSL_GUARDED_BY(mutex_);

  mutable concurrency::Mutex mutex_;
};

template <>
inline absl::StatusOr<int> ChunkStoreWriter::Put(Chunk value, int seq,
                                                 bool final)
    ABSL_LOCKS_EXCLUDED(mutex_) {
  concurrency::MutexLock lock(&mutex_);
  if (!accepts_puts_) {
    DLOG(ERROR)
        << "Put was called on a writer that is not accepting more puts.";
    return absl::FailedPreconditionError(
        "Put was called on a writer that is not accepting more puts.");
  }

  if (seq != -1 && final_seq_id_ != -1 && seq > final_seq_id_) {
    DLOG(ERROR) << "Cannot put chunks with seq_id > final_seq_id: " << seq
                << " > " << final_seq_id_;
    return absl::FailedPreconditionError(
        "Cannot put chunks with seq_id > final_seq_id.");
  }

  if (value.IsNull() && !final) {
    DLOG(ERROR) << "Cannot put a null chunk without also finalizing.";
    return absl::FailedPreconditionError(
        "Cannot put a null chunk without also finalizing.");
  }

  int written_seq = seq;
  if (seq == -1) {
    written_seq = total_chunks_put_;
  }
  total_chunks_put_++;

  if (final) {
    final_seq_id_ = written_seq;
  }

  EnsureWriteLoop();
  if (!buffer_->writer()->WriteUnlessCancelled(NodeFragment{
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
inline ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, Chunk value) {
  bool final = value.IsNull();
  writer.Put(std::move(value), /*seq=*/-1, /*final=*/final).IgnoreError();
  return writer;
}

/// @private
template <typename T>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, std::vector<T> value) {
  for (auto& element : std::move(value)) {
    writer << std::move(element);
    if (!writer.GetStatus().ok()) {
      LOG(ERROR) << "Failed to put element: " << writer.GetStatus();
      break;
    }
  }
  return writer;
}

template <typename T>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                             std::pair<T, int> value) {
  auto [data_value, seq] = std::move(value);
  writer.Put(std::move(data_value), seq, /*final=*/false).IgnoreError();
  return writer;
}

template <>
inline ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                                    std::pair<Chunk, int> value) {
  bool final = value.first.IsNull();
  auto [data_value, seq] = std::move(value);
  writer.Put(std::move(data_value), seq, /*final=*/final).IgnoreError();
  return writer;
}

template <typename T>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                             std::pair<T, bool> value) {
  auto [data_value, is_final] = std::move(value);
  writer.Put(std::move(data_value), -1, /*final=*/is_final).IgnoreError();
  return writer;
}

/// @private
template <typename T>
ChunkStoreWriter* operator<<(ChunkStoreWriter* absl_nonnull writer, T value) {
  *writer << std::move(value);
  return writer;
}

/// @private
template <typename T>
std::unique_ptr<ChunkStoreWriter>& operator<<(
    std::unique_ptr<ChunkStoreWriter>& writer, T value) {
  *writer << std::move(value);
  return writer;
}

/// @private
template <typename T>
std::shared_ptr<ChunkStoreWriter>& operator<<(
    std::shared_ptr<ChunkStoreWriter>& writer, T value) {
  *writer << std::move(value);
  return writer;
}

}  // namespace eglt

#endif  // EGLT_STORES_CHUNK_STORE_WRITER_H_