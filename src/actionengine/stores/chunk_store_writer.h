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

#ifndef ACTIONENGINE_STORES_CHUNK_STORE_WRITER_H_
#define ACTIONENGINE_STORES_CHUNK_STORE_WRITER_H_

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/serialization.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

/** * @brief
 *   A writer for the ChunkStore that allows writing chunks to the store in a
 *   buffered manner.
 *
 * This class provides methods to write chunks to the store through
 * a non-blocking interface. It buffers chunks in memory and writes them
 * to the store in a separate fiber, allowing for efficient writing without
 * blocking the calling thread.
 *
 */
class ChunkStoreWriter {
 public:
  /** @brief
   *    Constructs a ChunkStoreWriter for the given ChunkStore, setting
   *    @p n_chunks_to_buffer if provided.
   *
   * @param chunk_store
   *   The ChunkStore to write to. Must not be null.
   * @param n_chunks_to_buffer
   *   The number of chunks to buffer in memory before writing them to the store.
   *   If -1, the buffer will be unbounded.
   */
  explicit ChunkStoreWriter(ChunkStore* absl_nonnull chunk_store,
                            int n_chunks_to_buffer = -1);

  // This class is not copyable or movable.
  ChunkStoreWriter(const ChunkStoreWriter&) = delete;
  ChunkStoreWriter& operator=(const ChunkStoreWriter&) = delete;

  ~ChunkStoreWriter();

  absl::StatusOr<int> Put(Chunk value, int seq, bool final);

  template <typename T>
  absl::StatusOr<int> Put(T value, int seq = -1, bool final = false) {
    auto chunk = ToChunk(std::move(value));
    if (!chunk.ok()) {
      return chunk.status();
    }
    return Put(*std::move(chunk), seq, final);
  }

  template <typename T>
  friend ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, T value) {
    absl::StatusOr<Chunk> chunk = ToChunk(std::move(value));
    const bool final = chunk->IsNull();
    writer.Put(*std::move(chunk), -1, final).IgnoreError();
    return writer;
  }

  absl::Status GetStatus() const;

  void BindPeers(absl::flat_hash_map<std::string, WireStream*> peers);

  void Cancel() {
    act::MutexLock lock(&mu_);
    CancelInternal();
  }

  void WaitForBufferToDrain() {
    act::MutexLock lock(&mu_);
    WaitForBufferToDrainInternal();
  }

  void Join() {
    act::MutexLock lock(&mu_);
    JoinInternal();
  }

 private:
  void EnsureWriteLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void SafelyCloseBuffer() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status RunWriteLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void WaitForBufferToDrainInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    SafelyCloseBuffer();
    JoinInternal();
  }

  void CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    SafelyCloseBuffer();
    if (fiber_ != nullptr) {
      fiber_->Cancel();
    }
  }

  void JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (fiber_ != nullptr) {
      thread::Fiber* fiber = fiber_.get();
      mu_.unlock();
      fiber->Join();
      mu_.lock();
      fiber_ = nullptr;
    }
  }

  ChunkStore* absl_nonnull const chunk_store_ = nullptr;
  const int n_chunks_to_buffer_;

  int final_seq_ ABSL_GUARDED_BY(mu_) = -1;
  int total_chunks_put_ ABSL_GUARDED_BY(mu_) = 0;

  bool accepts_puts_ ABSL_GUARDED_BY(mu_) = true;
  bool buffer_writer_closed_ ABSL_GUARDED_BY(mu_) = false;

  int total_chunks_written_ = 0;

  std::unique_ptr<thread::Fiber> fiber_ ABSL_GUARDED_BY(mu_);
  thread::Channel<std::optional<NodeFragment>> buffer_;
  absl::Status status_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, WireStream*> peers_ ABSL_GUARDED_BY(mu_);

  mutable act::Mutex mu_;
};

template <>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, Chunk value);

template <>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer,
                             std::pair<Chunk, int> value);

template <typename T>
ChunkStoreWriter& operator<<(ChunkStoreWriter& writer, std::vector<T> value) {
  for (auto& element : std::move(value)) {
    writer << std::move(element);
  }
  return writer;
}

template <typename T>
ChunkStoreWriter* absl_nonnull operator<<(ChunkStoreWriter* absl_nonnull writer,
                                          T value) {
  *writer << std::move(value);
  return writer;
}

template <typename T>
std::unique_ptr<ChunkStoreWriter>& operator<<(
    std::unique_ptr<ChunkStoreWriter>& writer, T value) {
  *writer << std::move(value);
  return writer;
}

template <typename T>
std::shared_ptr<ChunkStoreWriter>& operator<<(
    std::shared_ptr<ChunkStoreWriter>& writer, T value) {
  *writer << std::move(value);
  return writer;
}

}  // namespace act

#endif  // ACTIONENGINE_STORES_CHUNK_STORE_WRITER_H_