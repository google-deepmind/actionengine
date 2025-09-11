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

#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/stores/chunk_store_reader.h"
#include "actionengine/stores/chunk_store_writer.h"
#include "actionengine/stores/local_chunk_store.h"
#include "actionengine/util/status_macros.h"

namespace act {

AsyncNode::AsyncNode(std::string_view id, NodeMap* absl_nullable node_map,
                     std::unique_ptr<ChunkStore> chunk_store)
    : node_map_(node_map), chunk_store_(std::move(chunk_store)) {
  if (chunk_store_ == nullptr) {
    chunk_store_ = std::make_unique<LocalChunkStore>();
  }
  chunk_store_->SetIdOrDie(id);
}

AsyncNode::AsyncNode(AsyncNode&& other) noexcept {
  act::MutexLock lock(&other.mu_);
  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
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

  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  other.default_reader_ = nullptr;
  other.default_writer_ = nullptr;

  return *this;
}

AsyncNode::~AsyncNode() {
  act::MutexLock lock(&mu_);
  if (default_reader_ != nullptr) {
    default_reader_->Cancel();
    mu_.unlock();
    default_reader_.reset();
    mu_.lock();
  }
  if (default_writer_ != nullptr) {
    mu_.unlock();
    default_writer_->WaitForBufferToDrain();
    default_writer_.reset();
    mu_.lock();
  }
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
      RETURN_IF_ERROR(chunk_store_->SetId(fragment.id));
    }
  }

  return PutInternal(std::move(fragment.GetChunkOrDie()),
                     fragment.seq.value_or(-1), !fragment.continued);
}

absl::Status AsyncNode::PutInternal(Chunk chunk, int seq, bool final)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  ChunkStoreWriter* writer = EnsureWriter();

  RETURN_IF_ERROR(writer->Put(std::move(chunk), seq, final).status());

  return absl::OkStatus();
}

ChunkStoreWriter& AsyncNode::GetWriter() ABSL_LOCKS_EXCLUDED(mu_) {
  act::MutexLock lock(&mu_);
  return *EnsureWriter();
}

absl::Status AsyncNode::GetWriterStatus() const {
  act::MutexLock lock(&mu_);
  if (default_writer_ == nullptr) {
    return absl::OkStatus();
  }
  return default_writer_->GetStatus();
}

auto AsyncNode::GetId() const -> std::string {
  return std::string(chunk_store_->GetId());
}

absl::StatusOr<std::optional<Chunk>> AsyncNode::Next(
    std::optional<absl::Duration> timeout) {
  ChunkStoreReader& reader = GetReader();
  return reader.Next(timeout);
}

std::optional<Chunk> AsyncNode::NextOrDie(
    std::optional<absl::Duration> timeout) {
  auto next = Next(timeout);
  CHECK_OK(next.status());
  return *std::move(next);
}

ChunkStoreReader& AsyncNode::GetReader() ABSL_LOCKS_EXCLUDED(mu_) {
  act::MutexLock lock(&mu_);
  return *EnsureReader();
}

absl::Status AsyncNode::GetReaderStatus() const {
  act::MutexLock lock(&mu_);
  if (default_reader_ == nullptr) {
    return absl::FailedPreconditionError("Reader is not initialized.");
  }
  return default_reader_->GetStatus();
}

std::unique_ptr<ChunkStoreReader> AsyncNode::MakeReader(
    ChunkStoreReaderOptions options) const {
  return std::make_unique<ChunkStoreReader>(chunk_store_.get(), options);
}

AsyncNode& AsyncNode::SetReaderOptions(const ChunkStoreReaderOptions& options) {

  act::MutexLock lock(&mu_);
  EnsureReader()->SetOptions(options);
  return *this;
}

AsyncNode& AsyncNode::ResetReader() {
  act::MutexLock lock(&mu_);
  default_reader_ = nullptr;
  return *this;
}

ChunkStoreReader* AsyncNode::EnsureReader() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (default_reader_ == nullptr) {
    default_reader_ = std::make_unique<ChunkStoreReader>(chunk_store_.get());
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
  act::MutexLock lock(&mu_);
  const bool continued = !final && !value.IsNull();
  return PutInternal(NodeFragment{
      .id = std::string(chunk_store_->GetId()),
      .data = std::move(value),
      .seq = seq,
      .continued = continued,
  });
}

auto AsyncNode::Put(NodeFragment value) -> absl::Status {
  act::MutexLock lock(&mu_);
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

}  // namespace act
