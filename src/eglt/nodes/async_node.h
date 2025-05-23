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
#include <tuple>
#include <utility>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/peers.h"
#include "eglt/net/stream.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/chunk_store_io.h"

namespace eglt {

class NodeMap;

absl::Status SendToStreamIfNotNullAndOpen(EvergreenWireStream* stream,
                                          NodeFragment&& fragment);

class AsyncNode {
 public:
  explicit AsyncNode(std::string_view id = "",
                     NodeMap* absl_nullable node_map = nullptr,
                     std::unique_ptr<ChunkStore> chunk_store = nullptr);

  AsyncNode(AsyncNode& other) = delete;
  AsyncNode(AsyncNode&& other) noexcept;

  AsyncNode& operator=(AsyncNode& other) = delete;
  AsyncNode& operator=(AsyncNode&& other) noexcept;

  void BindPeers(absl::flat_hash_map<std::string_view,
                                     std::shared_ptr<EvergreenWireStream>>
                     peers) {
    concurrency::MutexLock lock(&mutex_);
    peers_ = std::move(peers);
  }

  template <typename T>
  auto Put(T value, int seq_id = -1, bool final = false) -> absl::Status {
    return Put(Serialize(std::move(value)), seq_id, final);
  }

  template <typename T>
  auto PutAndClose(T value, int seq_id = -1) -> absl::Status {
    return Put(Serialize(std::move(value)), seq_id, /*final=*/true);
  }

  ChunkStoreWriter& GetWriter() ABSL_LOCKS_EXCLUDED(mutex_);
  auto GetWriterStatus() const -> absl::Status;

  [[nodiscard]] auto GetId() const -> std::string {
    return std::string(chunk_store_->GetId());
  }

  template <typename T>
  auto StatusOrNext() -> absl::StatusOr<std::optional<T>> {
    ChunkStoreReader& reader = GetReader();
    auto next = reader.Next<T>();
    if (absl::Status status = reader.GetStatus(); !status.ok()) {
      return status;
    }
    if (!next.has_value()) {
      return std::nullopt;
    }
    return next;
  }

  template <typename T>
  auto Next() -> std::optional<T> {
    auto status_or_next = StatusOrNext<T>();
    if (!status_or_next.ok()) {
      LOG(FATAL) << "Failed to get next chunk: " << status_or_next.status();
      ABSL_ASSUME(false);
    }
    return status_or_next.value();
  }

  template <typename T>
  T ConsumeAs() {
    auto status_or_item = StatusOrNext<T>();
    if (!status_or_item.ok()) {
      LOG(FATAL) << "Failed to get chunk: " << status_or_item.status();
      ABSL_ASSUME(false);
    }

    auto must_be_nullopt = StatusOrNext<T>();
    if (!must_be_nullopt.ok()) {
      LOG(FATAL) << "Error probing reader: " << must_be_nullopt.status();
      ABSL_ASSUME(false);
    }
    if (*must_be_nullopt) {
      LOG(FATAL) << "Reader must be empty after consuming the node.";
      ABSL_ASSUME(false);
    }

    return *std::move(status_or_item);
  }

  ChunkStoreReader& GetReader() ABSL_LOCKS_EXCLUDED(mutex_);
  auto GetReaderStatus() const -> absl::Status;
  [[nodiscard]] auto MakeReader(bool ordered = false,
                                bool remove_chunks = false,
                                int n_chunks_to_buffer = -1) const
      -> std::unique_ptr<ChunkStoreReader>;
  auto SetReaderOptions(bool ordered = false, bool remove_chunks = false,
                        int n_chunks_to_buffer = -1) -> AsyncNode&;
  auto ResetReader() -> AsyncNode&;

  template <typename T>
  friend AsyncNode& operator>>(AsyncNode& node, std::optional<T>& value);

  template <typename T>
  friend AsyncNode& operator<<(AsyncNode& node, T value);

 private:
  auto EnsureReader(bool ordered = false, bool remove_chunks = false,
                    int n_chunks_to_buffer = -1) -> ChunkStoreReader*;

  auto EnsureWriter(int n_chunks_to_buffer = -1) -> ChunkStoreWriter*;

  auto PutFragment(NodeFragment fragment, int seq_id = -1) -> absl::Status;
  auto PutChunk(Chunk chunk, int seq_id = -1, bool final = false)
      -> absl::Status;

  NodeMap* absl_nullable node_map_ = nullptr;
  std::unique_ptr<ChunkStore> chunk_store_;

  mutable concurrency::Mutex mutex_;
  std::unique_ptr<ChunkStoreReader> default_reader_ ABSL_GUARDED_BY(mutex_);
  std::unique_ptr<ChunkStoreWriter> default_writer_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string_view, std::shared_ptr<EvergreenWireStream>>
      peers_ ABSL_GUARDED_BY(mutex_);
};

template <>
inline auto AsyncNode::Put<Chunk>(Chunk value, int seq_id, bool final)
    -> absl::Status {
  bool continued = !final && !value.IsNull();
  return PutFragment(NodeFragment{
      .id = std::string(chunk_store_->GetId()),
      .chunk = std::move(value),
      .continued = continued,
  });
}

template <>
inline auto AsyncNode::Put(NodeFragment value, int seq_id, bool final)
    -> absl::Status {
  return PutFragment(std::move(value), seq_id);
}

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
  value = DeserializeAs<T>(*std::move(chunk));
  return node;
}

/// @private
template <typename T>
AsyncNode& operator<<(AsyncNode& node, T value) {
  node.EnsureWriter();
  return node << Serialize(std::move(value));
}

// -----------------------------------------------------------------------------

// Concrete instantiation for the operator>> for Chunk.
template <>
inline AsyncNode& operator>>(AsyncNode& node, std::optional<Chunk>& value) {
  auto next_chunk_or_status = node.StatusOrNext<Chunk>();
  if (!next_chunk_or_status.ok()) {
    LOG(ERROR) << "Failed to get next chunk: " << next_chunk_or_status.status();
    return node;
  }
  value = next_chunk_or_status.value();
  return node;
}

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
/// @private
template <>
inline AsyncNode& operator<<(AsyncNode& node, NodeFragment value) {
  node.Put(std::move(value)).IgnoreError();
  return node;
}

/// @private
template <>
inline AsyncNode& operator<<(AsyncNode& node, Chunk value) {
  const bool final = value.IsNull();
  node.Put(std::move(value), /*seq_id=*/-1, /*final=*/final).IgnoreError();
  return node;
}

// -----------------------------------------------------------------------------

/// @private
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

/// @private
template <typename T>
AsyncNode& operator<<(AsyncNode& node, std::pair<T, int> value) {
  auto [data_value, seq] = std::move(value);
  node.Put(std::move(data_value), seq, /*final=*/false).IgnoreError();
  return node;
}

/// @private
template <>
inline AsyncNode& operator<<(AsyncNode& node, std::pair<Chunk, int> value) {
  auto [data_value, seq] = std::move(value);
  bool final = data_value.IsNull();
  node.Put(std::move(data_value), seq, /*final=*/final).IgnoreError();
  return node;
}

// Convenience operators to write to an AsyncNode pointers (such as in the case
// of action->GetOutput("text"))
/// @private
template <typename T>
AsyncNode* operator<<(AsyncNode* absl_nonnull node, T value) {
  *node << std::move(value);
  return node;
}

/// @private
template <typename T>
std::unique_ptr<AsyncNode>& operator<<(std::unique_ptr<AsyncNode>& node,
                                       T value) {
  *node << std::move(value);
  return node;
}

/// @private
template <typename T>
std::shared_ptr<AsyncNode>& operator<<(std::shared_ptr<AsyncNode>& node,
                                       T value) {
  *node << std::move(value);
  return node;
}

}  // namespace eglt

#endif  // EGLT_NODES_ASYNC_NODE_H_
