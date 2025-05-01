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

#ifndef EGLT_NODES_CHUNK_STORE_IO_H_
#define EGLT_NODES_CHUNK_STORE_IO_H_

#include <algorithm>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"

namespace eglt {

class ChunkStore;

class ChunkStoreReader {
  // This class is thread-compatible. Calls to Run() and Reset() must be
  // externally synchronized and coordinated with calls to Next() / GetStatus().
  // In particular, status' reads/updates are protected, but users should not
  // assume that a call to GetStatus() will return the same status as the
  // previous call to Run(). Internally, status is updated in a background
  // thread, so it is possible for the status to change arbitrarily between
  // various calls.
  // TODO(hpnkv): clarify the thread-compatibility contract and the
  //   behavior of GetStatus().
 public:
  constexpr static absl::Duration kDefaultWaitTimeout =
      absl::InfiniteDuration();
  constexpr static absl::Duration kNoTimeout = absl::InfiniteDuration();

  explicit ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                            bool ordered = false, bool remove_chunks = false,
                            int n_chunks_to_buffer = -1,
                            absl::Duration timeout = kDefaultWaitTimeout);
  ~ChunkStoreReader();

  ChunkStoreReader(const ChunkStoreReader& other) = delete;
  ChunkStoreReader& operator=(const ChunkStoreReader& other) = delete;

  absl::Status Run();

  template <typename T>
  std::optional<T> Next() {
    auto chunk = Next<Chunk>();
    if (!chunk.has_value() || chunk->IsNull()) {
      return std::nullopt;
    }
    return ConvertTo<T>(*std::move(chunk));
  }

  // definitions follow in the header for some well-known types.
  template <typename T>
  friend ChunkStoreReader& operator>>(ChunkStoreReader& reader, T& value);

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return status_;
  }

 private:
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> NextInternal() const;

  void RunPrefetchLoop();

  // This is primarily used by the prefetch loop to update the status of the
  // reader to avoid scoped locks in multiple places.
  void UpdateStatus(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    status_ = status;
  }

  void Join(bool cancel = false);

  ChunkStore* chunk_store_;
  const bool ordered_;
  const bool remove_chunks_;
  const int n_chunks_to_buffer_;
  const absl::Duration timeout_;

  std::unique_ptr<concurrency::Fiber> fiber_;
  std::unique_ptr<concurrency::Channel<std::optional<std::pair<int, Chunk>>>>
      buffer_;
  int total_chunks_read_ = 0;

  absl::Status status_ ABSL_GUARDED_BY(mutex_);
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
      absl::Now() + timeout_,
      {buffer_->GetReader()->OnRead(&seq_and_chunk, &ok)});
  if (selected == -1) {
    UpdateStatus(absl::DeadlineExceededError("Timed out waiting for chunk."));
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
ChunkStoreReader* operator>>(ChunkStoreReader* reader, T& value) {
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

class ChunkStoreWriter {
  // This class is thread-safe. Public methods can be called concurrently from
  // different threads. Chunk store and buffer are only accessed by the writer
  // thread, and are only set on construction. Other fields are only accessed by
  // the internal writer fiber, so access is always synchronous.
 public:
  explicit ChunkStoreWriter(ChunkStore* chunk_store,
                            int n_chunks_to_buffer = -1);
  ~ChunkStoreWriter() ABSL_LOCKS_EXCLUDED(mutex_);

  ChunkStoreWriter(const ChunkStoreWriter& other) = delete;
  ChunkStoreWriter& operator=(const ChunkStoreWriter& other) = delete;

  template <typename T>
  absl::StatusOr<int> Put(T value, int seq = -1, bool final = false)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    return Put(ConvertTo<Chunk>(std::move(value)), seq, final);
  }

  // putting a chunk is considered a base case, therefore the definition is
  // inside the class body.
  template <>
  absl::StatusOr<int> Put(Chunk value, int seq, bool final)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    if (!accepts_puts_) {
      return absl::FailedPreconditionError(
          "Put was called on a writer that is not accepting more puts.");
    }

    int written_seq = seq;
    if (seq == -1) {
      written_seq = total_chunks_put_;
    }
    total_chunks_put_++;

    EnsureWriteLoop();
    max_seq_put_ = std::max(max_seq_put_, written_seq);
    if (!buffer_->GetWriter()->WriteUnlessCancelled(NodeFragment{
            .chunk = std::move(value),
            .seq = written_seq,
            .continued = !(final || value.IsNull()),
        })) {
      return absl::CancelledError("Cancelled.");
    }

    return written_seq;
  }

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return status_;
  }

  template <typename T>
  friend ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, T value) {
    writer.Put(ConvertTo<Chunk>(std::move(value))).IgnoreError();
    return writer;
  }

 private:
  void EnsureWriteLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void SafelyCloseBuffer() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RunWriteLoop();

  void TerminateSafely() ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateStatus(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    status_ = status;
  }

  ChunkStore* chunk_store_ = nullptr;
  const int n_chunks_to_buffer_;

  int max_seq_put_ ABSL_GUARDED_BY(mutex_) = -1;
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
inline ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, Chunk value) {
  bool continued = true;
  if (value.IsNull()) {
    continued = false;
  }

  writer.Put(std::move(value), /*seq=*/-1, /*final=*/!continued).IgnoreError();
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

template <typename T>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                             std::pair<T, bool> value) {
  auto [data_value, is_final] = std::move(value);
  writer.Put(std::move(data_value), -1, /*final=*/is_final).IgnoreError();
  return writer;
}

/// @private
template <typename T>
ChunkStoreWriter* operator<<(ChunkStoreWriter* writer, T value) {
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

#endif  // EGLT_NODES_CHUNK_STORE_IO_H_
