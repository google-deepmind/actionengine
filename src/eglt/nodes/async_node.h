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

#ifndef EGLT_NODES_ASYNC_NODE_H_
#define EGLT_NODES_ASYNC_NODE_H_

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/optimization.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/serialization.h"
#include "eglt/data/types.h"
#include "eglt/net/stream.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/chunk_store_reader.h"
#include "eglt/stores/chunk_store_writer.h"
#include "eglt/util/status_macros.h"

namespace eglt {
class NodeMap;
}

/**
 * @file
 * @brief Provides the AsyncNode class for handling asynchronous data streams.
 *
 * The `AsyncNode` class is designed to manage asynchronous data streams in the
 * ActionEngine framework. It allows for the reading and writing of data chunks,
 * supports peer binding to enable mirroring of written chunks to a party over
 * the network, and provides methods for consuming data as specific types.
 */
namespace eglt {

/**
 * An asynchronous node in the ActionEngine framework.
 *
 * This class represents a node that can read and write data asynchronously.
 * It supports binding to peers for mirroring data, and provides methods for
 * consuming data as specific types. The node is backed by a `ChunkStore` for
 * storing data chunks.
 *
 * @headerfile eglt/nodes/async_node.h
 */
class AsyncNode {
 public:
  /**
   * Constructs an AsyncNode with the given ID, node map, and chunk store.
   *
   * @param id
   *   The unique identifier for the node.
   * @param node_map
   *   An optional pointer to a NodeMap for managing multiple nodes.
   * @param chunk_store
   *   An optional unique pointer to a ChunkStore for storing data chunks. If
   *   not provided, a default chunk store will be created.
   */
  explicit AsyncNode(std::string_view id = "",
                     NodeMap* absl_nullable node_map = nullptr,
                     std::unique_ptr<ChunkStore> chunk_store = nullptr);

  // This class cannot be copied as each AsyncNode contains non-trivial state
  // that cannot be shared between copies, such as reader and writer fibers.
  AsyncNode(AsyncNode& other) = delete;
  AsyncNode& operator=(AsyncNode& other) = delete;

  // AsyncNode objects can be moved by value.
  AsyncNode(AsyncNode&& other) noexcept;
  AsyncNode& operator=(AsyncNode&& other) noexcept;

  ~AsyncNode();

  void BindPeers(absl::flat_hash_map<std::string, WireStream*> peers);

  /**
   * Enqueues a chunk to be written to underlying chunk store.
   *
   * @param value
   *   The chunk to write to the store.
   * @param seq
   *  The sequence number of the chunk. If -1, the chunk will be written
   *  without a specific sequence number.
   * @param final
   *   A flag indicating whether this is the final chunk in a sequence.
   *   If true, the chunk will be marked as final, and no more chunks are
   *   expected after this one.
   * @return
   *   An absl::Status indicating the success or failure of the operation.
   *   Writers are asynchronous, so an OK status does not guarantee that
   *   the chunk has been written to the store immediately. It only indicates
   *   that the chunk has been successfully enqueued for writing.
   */
  auto Put(Chunk value, int seq = -1, bool final = false) -> absl::Status;
  /**
   * Enqueues a NodeFragment to be written to the underlying chunk store.
   *
   * See Put(Chunk, int, bool) for details on the parameters, which in this
   * case are part of the NodeFragment structure.
   *
   * @param value
   *   The NodeFragment to write to the store.
   * @return
   *   An absl::Status indicating the success or failure of the operation.
   */
  auto Put(NodeFragment value) -> absl::Status;

  /**
   * Enqueues a value to be written to the underlying chunk store.
   *
   * This method converts the value to a Chunk using `ToChunk` and then calls
   * Put(Chunk, int, bool) with the converted chunk.
   *
   * @param value
   *   The value to write to the store.
   * @param seq
   *  The sequence number of the chunk. If -1, the chunk will be written
   *  without a specific sequence number.
   * @param final
   *   A flag indicating whether this is the final chunk in a sequence.
   * @return
   *   An absl::Status indicating the success or failure of the operation. In
   *   addition to the failure cases of Put(Chunk, int, bool), this method
   *   can also fail if the conversion from T to Chunk fails.
   */
  template <typename T>
  auto Put(T value, int seq = -1, bool final = false) -> absl::Status {
    auto chunk = ToChunk(std::move(value));
    if (!chunk.ok()) {
      return chunk.status();
    }
    return Put(*std::move(chunk), seq, final);
  }

  /**
   * Returns the writer for this AsyncNode.
   *
   * This method ensures that a writer is created if it does not already exist,
   * so never fails.
   *
   * @return
   *   A reference to the ChunkStoreWriter associated with this AsyncNode.
   */
  [[nodiscard]] ChunkStoreWriter& GetWriter();
  /**
    * Returns the status of the writer for this AsyncNode.
    *
    * This method checks if the writer is in a valid state and returns an
    * `absl::Status` indicating the status of the writer.
    *
    * @return
    *   An `absl::Status` indicating the status of the writer. If the writer is
    *   in a valid state, it returns `absl::OkStatus()`. If there is an error,
    *   it returns an appropriate error status.
    */
  [[nodiscard]]
  auto GetWriterStatus() const -> absl::Status;

  [[nodiscard]] auto GetId() const -> std::string;

  /**
   * Reads the next chunk from the underlying chunk store.
   *
   * This method reads the next chunk from the store, blocking until a chunk is
   * available or the timeout expires. If no chunk is available within the
   * timeout, it returns an empty optional.
   *
   * The default associated reader is used, but multiple readers in principle
   * can be created for the same AsyncNode, each with its own options.
   *
   * @param timeout
   *   An optional duration to wait for a chunk to become available. If not
   *   provided, it defaults to the reader's timeout.
   * @return
   *   An `absl::StatusOr` containing an optional Chunk. If a chunk is available,
   *   it will be returned; otherwise, an empty optional is returned.
   */
  absl::StatusOr<std::optional<Chunk>> Next(
      std::optional<absl::Duration> timeout = std::nullopt);

  template <typename T>
  auto Next(std::optional<absl::Duration> timeout = std::nullopt)
      -> absl::StatusOr<std::optional<T>> {
    ChunkStoreReader& reader = GetReader();
    return reader.Next<T>(timeout);
  }

  std::optional<Chunk> NextOrDie(
      std::optional<absl::Duration> timeout = std::nullopt);

  template <typename T>
  auto NextOrDie(std::optional<absl::Duration> timeout = std::nullopt)
      -> std::optional<T> {
    auto next = Next<T>(timeout);
    CHECK_OK(next.status());
    return *std::move(next);
  }

  template <typename T>
  absl::StatusOr<T> ConsumeAs(
      std::optional<absl::Duration> timeout = std::nullopt) {
    timeout = timeout.value_or(GetReader().GetOptions().timeout);
    const absl::Time started_at = absl::Now();

    // The node being consumed must contain an element.
    ASSIGN_OR_RETURN(std::optional<T> item, Next<T>(*timeout));
    if (!item) {
      return absl::FailedPreconditionError(
          "AsyncNode is empty at current offset, "
          "cannot consume item as type T.");
    }

    const absl::Duration elapsed = absl::Now() - started_at;
    if (elapsed > *timeout) {
      return absl::DeadlineExceededError(
          absl::StrCat("Timed out after ", absl::FormatDuration(elapsed),
                       " while consuming item as type T."));
    }

    // The node must be empty after consuming the item.
    ASSIGN_OR_RETURN(const std::optional<Chunk> must_be_nullopt,
                     Next<Chunk>(*timeout - elapsed));
    if (must_be_nullopt.has_value()) {
      return absl::FailedPreconditionError(
          "AsyncNode must be empty after consuming the item.");
    }

    return *std::move(item);
  }

  ChunkStoreReader& GetReader() ABSL_LOCKS_EXCLUDED(mu_);
  auto GetReaderStatus() const -> absl::Status;
  [[nodiscard]] auto MakeReader(ChunkStoreReaderOptions options) const
      -> std::unique_ptr<ChunkStoreReader>;
  auto SetReaderOptions(const ChunkStoreReaderOptions& options) -> AsyncNode&;
  auto ResetReader() -> AsyncNode&;

  template <typename T>
  friend AsyncNode& operator>>(AsyncNode& node, std::optional<T>& value);

  template <typename T>
  friend AsyncNode& operator<<(AsyncNode& node, T value);

 private:
  ChunkStoreReader* absl_nonnull EnsureReader()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  ChunkStoreWriter* absl_nonnull EnsureWriter(int n_chunks_to_buffer = -1)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status PutInternal(NodeFragment fragment)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status PutInternal(Chunk chunk, int seq = -1, bool final = false)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  NodeMap* absl_nullable node_map_ = nullptr;
  std::unique_ptr<ChunkStore> chunk_store_;

  mutable eglt::Mutex mu_;
  mutable eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<ChunkStoreReader> default_reader_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<ChunkStoreWriter> default_writer_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, WireStream*> peers_ ABSL_GUARDED_BY(mu_);
};

// -----------------------------------------------------------------------------
// IO operators for AsyncNode. These templates have concrete instantiations for
// Chunk and NodeFragment, and a default overload for all other types, which is
// implemented in terms of ConstructFrom<Chunk>(T) and MoveAs<T>(Chunk) and
// therefore specified for types for which these functions are defined.
// -----------------------------------------------------------------------------
template <typename T>
AsyncNode& operator>>(AsyncNode& node, std::optional<T>& value) {
  std::optional<Chunk> chunk;
  node >> chunk;

  if (!chunk.has_value()) {
    value = std::nullopt;
    return node;
  }

  auto status_or_value = FromChunkAs<T>(*std::move(chunk));
  if (!status_or_value.ok()) {
    LOG(FATAL) << "Failed to convert chunk to value: "
               << status_or_value.status();
    ABSL_ASSUME(false);
  }
  value = std::move(status_or_value.value());
  return node;
}

template <typename T>
AsyncNode& operator<<(AsyncNode& node, T value) {
  node.EnsureWriter();
  return node << *ToChunk(std::move(value));
}

// -----------------------------------------------------------------------------

// Concrete instantiation for the operator>> for Chunk.
template <>
AsyncNode& operator>>(AsyncNode& node, std::optional<Chunk>& value);

// -----------------------------------------------------------------------------

// Helpers for the operator>> on pointers to AsyncNode.
template <typename T>
AsyncNode*& operator>>(AsyncNode*& node, T& value) {
  *node >> value;
  return node;
}

template <typename T>
std::unique_ptr<AsyncNode>& operator>>(std::unique_ptr<AsyncNode>& node,
                                       T& value) {
  *node >> value;
  return node;
}

template <typename T>
std::shared_ptr<AsyncNode>& operator>>(std::shared_ptr<AsyncNode>& node,
                                       T& value) {
  *node >> value;
  return node;
}

// -----------------------------------------------------------------------------
// "Concrete" instantiations for the operator<< for Chunk and NodeFragment.
// -----------------------------------------------------------------------------
template <>
AsyncNode& operator<<(AsyncNode& node, NodeFragment value);

template <>
AsyncNode& operator<<(AsyncNode& node, Chunk value);

// -----------------------------------------------------------------------------

template <typename T>
AsyncNode& operator<<(AsyncNode& node, std::vector<T> value) {
  for (auto& element : std::move(value)) {
    auto status = node.Put(std::move(element));
    if (!status.ok()) {
      LOG(ERROR) << "Failed to put element: " << status;
      break;
    }
  }
  return node;
}

// Convenience operators to write to an AsyncNode pointers (such as in the case
// of action->GetOutput("text"))
template <typename T>
AsyncNode* absl_nonnull operator<<(AsyncNode* absl_nonnull node, T value) {
  *node << std::move(value);
  return node;
}

template <typename T>
std::unique_ptr<AsyncNode>& operator<<(std::unique_ptr<AsyncNode>& node,
                                       T value) {
  *node << std::move(value);
  return node;
}

template <typename T>
std::shared_ptr<AsyncNode>& operator<<(std::shared_ptr<AsyncNode>& node,
                                       T value) {
  *node << std::move(value);
  return node;
}

}  // namespace eglt

#endif  // EGLT_NODES_ASYNC_NODE_H_
