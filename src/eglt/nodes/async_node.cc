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

#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cppitertools/zip.hpp>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/chunk_store_io.h"
#include "eglt/stores/local_chunk_store.h"

namespace eglt {

absl::Status SendToStreamIfNotNullAndOpen(
    EvergreenWireStream* absl_nullable stream, NodeFragment&& fragment) {
  // if stream is null, we don't send anything and it is ok.
  if (stream == nullptr) {
    return absl::OkStatus();
  }

  return stream->Send(SessionMessage{.node_fragments = {std::move(fragment)}});
}

AsyncNode::AsyncNode(std::string_view id, NodeMap* absl_nullable node_map,
                     std::unique_ptr<ChunkStore> chunk_store)
    : node_map_(node_map), chunk_store_(std::move(chunk_store)) {
  if (chunk_store_ == nullptr) {
    chunk_store_ = std::make_unique<LocalChunkStore>();
  }
  chunk_store_->SetId(id);
}

AsyncNode::AsyncNode(AsyncNode&& other) noexcept {
  concurrency::MutexLock lock(&other.mutex_);
  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
  writer_stream_ = other.writer_stream_;
  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  other.default_reader_ = nullptr;
  other.default_writer_ = nullptr;
  other.writer_stream_ = nullptr;
}

AsyncNode& AsyncNode::operator=(AsyncNode&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  concurrency::TwoMutexLock lock(&mutex_, &other.mutex_);

  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
  writer_stream_ = other.writer_stream_;

  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  other.default_reader_ = nullptr;
  other.default_writer_ = nullptr;
  other.writer_stream_ = nullptr;

  return *this;
}

void AsyncNode::BindWriterStream(EvergreenWireStream* absl_nullable stream) {
  concurrency::MutexLock lock(&mutex_);
  writer_stream_ = stream;
}

absl::Status AsyncNode::PutFragment(NodeFragment fragment, const int seq_id) {
  {
    concurrency::MutexLock lock(&mutex_);
    const std::string node_id(chunk_store_->GetId());
    if (!fragment.id.empty()) {
      if (fragment.id != node_id) {
        return absl::FailedPreconditionError(absl::StrCat(
            "Fragment id: ", fragment.id,
            " does not match the node id: ", chunk_store_->GetId()));
      }
      if (node_id.empty()) {
        chunk_store_->SetId(fragment.id);
      }
    }
  }

  if (!fragment.chunk) {
    return absl::OkStatus();
  }

  return PutChunk(std::move(*fragment.chunk), seq_id,
                  /*final=*/!fragment.continued);
}

absl::Status AsyncNode::PutChunk(Chunk chunk, int seq_id, bool final) {
  concurrency::MutexLock lock(&mutex_);
  ChunkStoreWriter* writer = EnsureWriter();

  // I want to be able to move the chunk to writer, but I might also need to
  // send it to the stream.
  Chunk copy_to_stream = chunk;

  auto status_or_seq = writer->Put(std::move(chunk), seq_id, final);
  if (!status_or_seq.ok()) {
    LOG(ERROR) << "Failed to put chunk: " << status_or_seq.status();
    return status_or_seq.status();
  }

  auto stream_sending_status = SendToStreamIfNotNullAndOpen(
      writer_stream_, NodeFragment{
                          .id = std::string(chunk_store_->GetId()),
                          .chunk = std::move(copy_to_stream),
                          .seq = status_or_seq.value(),
                          .continued = !final,
                      });
  if (!stream_sending_status.ok()) {
    LOG(ERROR) << "Failed to send to stream: " << stream_sending_status;
    return stream_sending_status;
  }

  return absl::OkStatus();
}

ChunkStoreWriter& AsyncNode::GetWriter() ABSL_LOCKS_EXCLUDED(mutex_) {
  return *EnsureWriter();
}

absl::Status AsyncNode::GetWriterStatus() const {
  concurrency::MutexLock lock(&mutex_);
  if (default_writer_ == nullptr) {
    return absl::OkStatus();
  }
  return default_writer_->GetStatus();
}

absl::StatusOr<std::vector<Chunk>> AsyncNode::WaitForCompletion() {
  SetReaderOptions(/*ordered=*/true, /*remove_chunks=*/true);
  std::vector<Chunk> chunks;
  const ChunkStoreReader& reader = GetReader();
  while (true) {
    std::optional<Chunk> next_chunk;
    *this >> next_chunk;
    if (absl::Status status = reader.GetStatus(); !status.ok()) {
      return status;
    }
    if (!next_chunk.has_value()) {
      break;
    }
    chunks.push_back(std::move(next_chunk.value()));
  }

  concurrency::MutexLock lock(&mutex_);
  default_reader_ = nullptr;

  return chunks;
}

ChunkStoreReader& AsyncNode::GetReader() ABSL_LOCKS_EXCLUDED(mutex_) {
  concurrency::MutexLock lock(&mutex_);
  return *EnsureReader();
}

absl::Status AsyncNode::GetReaderStatus() const {
  concurrency::MutexLock lock(&mutex_);
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

  concurrency::MutexLock lock(&mutex_);
  if (default_reader_ != nullptr) {
    LOG(WARNING)
        << "Reader already exists on the node, changes to reader "
           "options will be ignored. You may want to reset the reader.";
  }
  EnsureReader(ordered, remove_chunks, n_chunks_to_buffer);
  return *this;
}

AsyncNode& AsyncNode::ResetReader() {
  concurrency::MutexLock lock(&mutex_);
  default_reader_ = nullptr;
  return *this;
}

ChunkStoreReader* AsyncNode::EnsureReader(bool ordered, bool remove_chunks,
                                          int n_chunks_to_buffer)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (default_reader_ == nullptr) {
    default_reader_ = std::make_unique<ChunkStoreReader>(
        chunk_store_.get(), ordered, remove_chunks, n_chunks_to_buffer);
  }
  return default_reader_.get();
}

ChunkStoreWriter* AsyncNode::EnsureWriter(int n_chunks_to_buffer)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (default_writer_ == nullptr) {
    default_writer_ = std::make_unique<ChunkStoreWriter>(chunk_store_.get(),
                                                         n_chunks_to_buffer);
  }
  return default_writer_.get();
}

}  // namespace eglt
