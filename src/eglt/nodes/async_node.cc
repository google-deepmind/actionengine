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

#include "async_node.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/strings/str_cat.h>

#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/chunk_store_reader.h"
#include "eglt/stores/chunk_store_writer.h"
#include "eglt/stores/local_chunk_store.h"

namespace eglt {

AsyncNode::AsyncNode(std::string_view id, NodeMap* absl_nullable node_map,
                     std::unique_ptr<ChunkStore> chunk_store)
    : node_map_(node_map), chunk_store_(std::move(chunk_store)) {
  if (chunk_store_ == nullptr) {
    chunk_store_ = std::make_unique<LocalChunkStore>();
  }
  chunk_store_->SetIdOrDie(id);
}

AsyncNode::AsyncNode(AsyncNode&& other) noexcept {
  eglt::MutexLock lock(&other.mu_);
  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
  peers_ = std::move(other.peers_);
  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  other.default_reader_ = nullptr;
  other.default_writer_ = nullptr;
}

AsyncNode& AsyncNode::operator=(AsyncNode&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  concurrency::TwoMutexLock lock(&mu_, &other.mu_);

  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
  peers_ = std::move(other.peers_);

  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  other.default_reader_ = nullptr;
  other.default_writer_ = nullptr;

  return *this;
}

AsyncNode::~AsyncNode() {
  eglt::MutexLock lock(&mu_);
  if (default_reader_ != nullptr) {
    default_reader_->Cancel();
    default_reader_.reset();
  }
}

void AsyncNode::BindPeers(
    absl::flat_hash_map<std::string, std::shared_ptr<WireStream>> peers) {
  eglt::MutexLock lock(&mu_);
  peers_ = std::move(peers);
}

absl::Status AsyncNode::PutInternal(NodeFragment fragment)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  const std::string node_id(chunk_store_->GetId());
  if (!fragment.id.empty()) {
    if (fragment.id != node_id) {
      return absl::FailedPreconditionError(
          absl::StrCat("Fragment id: ", fragment.id,
                       " does not match the node id: ", chunk_store_->GetId()));
    }
    if (node_id.empty()) {
      chunk_store_->SetIdOrDie(fragment.id);
    }
  }

  if (!fragment.chunk) {
    return absl::OkStatus();
  }

  return PutInternal(*std::move(fragment.chunk), fragment.seq,
                     !fragment.continued);
}

absl::Status AsyncNode::PutInternal(Chunk chunk, int seq, bool final)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  ChunkStoreWriter* writer = EnsureWriter();

  SessionMessage message_for_peers;
  if (!peers_.empty()) {
    message_for_peers.node_fragments.push_back(
        NodeFragment{.id = std::string(chunk_store_->GetId()),
                     .chunk = chunk,
                     .seq = seq,
                     .continued = !final});
  }

  auto status_or_seq = writer->Put(std::move(chunk), seq, final);
  if (!status_or_seq.ok()) {
    LOG(ERROR) << "Failed to put chunk: " << status_or_seq.status();
    return status_or_seq.status();
  }

  std::vector<std::string_view> dead_peers;
  for (const auto& [peer_id, peer] : peers_) {
    if (auto status = peer->Send(message_for_peers); !status.ok()) {
      LOG(ERROR) << "Failed to send to stream: " << status;
      dead_peers.push_back(peer_id);
    }
  }
  for (const auto& peer_id : dead_peers) {
    peers_.erase(peer_id);
  }

  return absl::OkStatus();
}

ChunkStoreWriter& AsyncNode::GetWriter() ABSL_LOCKS_EXCLUDED(mu_) {
  eglt::MutexLock lock(&mu_);
  return *EnsureWriter();
}

absl::Status AsyncNode::GetWriterStatus() const {
  eglt::MutexLock lock(&mu_);
  if (default_writer_ == nullptr) {
    return absl::OkStatus();
  }
  return default_writer_->GetStatus();
}

ChunkStoreReader& AsyncNode::GetReader() ABSL_LOCKS_EXCLUDED(mu_) {
  eglt::MutexLock lock(&mu_);
  return *EnsureReader();
}

absl::Status AsyncNode::GetReaderStatus() const {
  eglt::MutexLock lock(&mu_);
  if (default_reader_ == nullptr) {
    return absl::FailedPreconditionError("Reader is not initialized.");
  }
  return default_reader_->GetStatus();
}

std::unique_ptr<ChunkStoreReader> AsyncNode::MakeReader(
    bool ordered, bool remove_chunks, int n_chunks_to_buffer) const {
  return std::make_unique<ChunkStoreReader>(chunk_store_.get(), ordered,
                                            remove_chunks, n_chunks_to_buffer);
}

AsyncNode& AsyncNode::SetReaderOptions(bool ordered, bool remove_chunks,
                                       int n_chunks_to_buffer) {

  eglt::MutexLock lock(&mu_);
  if (default_reader_ != nullptr) {
    LOG(WARNING)
        << "Reader already exists on the node, changes to reader "
           "options will be ignored. You may want to reset the reader.";
  }
  EnsureReader(ordered, remove_chunks, n_chunks_to_buffer);
  return *this;
}

AsyncNode& AsyncNode::ResetReader() {
  eglt::MutexLock lock(&mu_);
  default_reader_ = nullptr;
  return *this;
}

ChunkStoreReader* AsyncNode::EnsureReader(bool ordered, bool remove_chunks,
                                          int n_chunks_to_buffer)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (default_reader_ == nullptr) {
    default_reader_ = std::make_unique<ChunkStoreReader>(
        chunk_store_.get(), ordered, remove_chunks, n_chunks_to_buffer);
  }
  return default_reader_.get();
}

ChunkStoreWriter* AsyncNode::EnsureWriter(int n_chunks_to_buffer)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (default_writer_ == nullptr) {
    default_writer_ = std::make_unique<ChunkStoreWriter>(chunk_store_.get(),
                                                         n_chunks_to_buffer);
  }
  return default_writer_.get();
}

auto AsyncNode::Put(Chunk value, int seq, bool final) -> absl::Status {
  eglt::MutexLock lock(&mu_);
  const bool continued = !final && !value.IsNull();
  return PutInternal(NodeFragment{
      .id = std::string(chunk_store_->GetId()),
      .seq = seq,
      .chunk = std::move(value),
      .continued = continued,
  });
}

auto AsyncNode::Put(NodeFragment value) -> absl::Status {
  eglt::MutexLock lock(&mu_);
  return PutInternal(std::move(value));
}

template <>
AsyncNode& operator>>(AsyncNode& node, std::optional<Chunk>& value) {
  auto next_chunk = node.Next<Chunk>();
  CHECK_OK(next_chunk.status()) << "Failed to get next chunk.";
  value = *std::move(next_chunk);
  return node;
}

template <>
AsyncNode& operator<<(AsyncNode& node, NodeFragment value) {
  node.Put(std::move(value)).IgnoreError();
  return node;
}

template <>
AsyncNode& operator<<(AsyncNode& node, Chunk value) {
  const bool final = value.IsNull();
  CHECK_OK(node.Put(std::move(value), /*seq=*/-1, /*final=*/final));
  return node;
}

}  // namespace eglt
