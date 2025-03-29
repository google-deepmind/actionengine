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
#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/chunk_store_io.h"
#include "eglt/nodes/chunk_store_local.h"
#include "eglt/nodes/node_map.h"

namespace eglt {

absl::Status SendToStreamIfNotNullAndOpen(base::EvergreenStream* stream,
                                          base::NodeFragment&& fragment) {
  // if stream is null, we don't send anything and it is ok.
  if (stream == nullptr) { return absl::OkStatus(); }

  return stream->Send(
    base::SessionMessage{.node_fragments = {std::move(fragment)}});
}

AsyncNode::AsyncNode(std::string_view id, NodeMap* node_map,
                     std::unique_ptr<ChunkStore> chunk_store) :
  node_map_(node_map), chunk_store_(std::move(chunk_store)) {
  if (chunk_store_ == nullptr) {
    chunk_store_ = std::make_unique<LocalChunkStore>();
  }
  chunk_store_->SetNodeId(id);
}

AsyncNode::AsyncNode(AsyncNode&& other) noexcept :
  node_map_(other.node_map_),
  chunk_store_(std::move(other.chunk_store_)),
  child_ids_(std::move(other.child_ids_)),
  default_reader_(std::move(other.default_reader_)),
  default_writer_(std::move(other.default_writer_)),
  writer_stream_(other.writer_stream_) {
  other.writer_stream_ = nullptr;
  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
}

AsyncNode& AsyncNode::operator=(AsyncNode&& other) noexcept {
  if (this == &other) { return *this; }
  node_map_ = other.node_map_;
  chunk_store_ = std::move(other.chunk_store_);
  child_ids_ = std::move(other.child_ids_);
  default_reader_ = std::move(other.default_reader_);
  default_writer_ = std::move(other.default_writer_);
  writer_stream_ = other.writer_stream_;
  other.writer_stream_ = nullptr;
  other.node_map_ = nullptr;
  other.chunk_store_ = nullptr;
  return *this;
}

void AsyncNode::BindWriterStream(base::EvergreenStream* stream) {
  writer_stream_ = stream;
}

template <>
auto AsyncNode::Put<base::Chunk>(base::Chunk value, int seq_id,
                                 bool final) -> absl::Status {
  return PutFragment(base::NodeFragment{
    .id = chunk_store_->GetNodeId(),
    .chunk = std::move(value),
    .continued = !final,
  });
}

absl::Status AsyncNode::PutFragment(base::NodeFragment fragment, int seq_id) {
  if (chunk_store_ == nullptr) {
    return absl::FailedPreconditionError("Chunk storage is not initialized.");
  }

  if (!fragment.child_ids.empty()) {
    for (const auto& child_id : fragment.child_ids) {
      child_ids_.insert(child_id);
    }
  }

  auto node_id = chunk_store_->GetNodeId();
  if (!fragment.id.empty()) {
    if (fragment.id != node_id) {
      return absl::FailedPreconditionError(absl::StrCat(
        "Fragment id: ", fragment.id,
        " does not match the node id: ", chunk_store_->GetNodeId()));
    }
    else
      if (node_id.empty()) { chunk_store_->SetNodeId(fragment.id); }
  }

  if (!fragment.chunk) { return absl::OkStatus(); }

  return PutChunk(std::move(*fragment.chunk), seq_id,
                  /*final=*/!fragment.continued);
}

absl::Status AsyncNode::PutChunk(base::Chunk chunk, int seq_id, bool final) {
  if (chunk_store_ == nullptr) {
    return absl::FailedPreconditionError("Chunk storage is not initialized.");
  }

  EnsureWriter();

  // I want to be able to move the chunk to writer, but I might also need to
  // send it to the stream.
  base::Chunk copy_to_stream = chunk;

  auto status_or_seq = default_writer_->Put(std::move(chunk), seq_id, final);
  if (!status_or_seq.ok()) {
    LOG(ERROR) << "Failed to put chunk: " << status_or_seq.status();
    return status_or_seq.status();
  }

  auto stream_sending_status = SendToStreamIfNotNullAndOpen(
    writer_stream_, base::NodeFragment{
      .id = std::string(chunk_store_->GetNodeId()),
      .chunk = std::move(copy_to_stream),
      .seq = status_or_seq.value(),
      .continued = !final,
    });
  if (!stream_sending_status.ok()) {
    LOG(ERROR) << "Failed to send to stream: " << stream_sending_status;
    // just log the error, do not return it, because we have already written
    // the fragment to the fragment store.
  }

  return absl::OkStatus();
}

ChunkStoreWriter& AsyncNode::GetWriter() {
  EnsureWriter();
  return *default_writer_;
}

absl::Status AsyncNode::GetWriterStatus() const {
  if (default_writer_ == nullptr) {
    return absl::FailedPreconditionError("Writer is not initialized.");
  }
  return default_writer_->GetStatus();
}

absl::StatusOr<std::vector<base::Chunk>> AsyncNode::WaitForCompletion() {
  SetReaderOptions(/*ordered=*/true, /*remove_chunks=*/true);
  std::vector<base::Chunk> chunks;
  while (true) {
    std::optional<base::Chunk> next_chunk;
    *this >> next_chunk;
    if (!default_reader_->GetStatus().ok()) {
      return default_reader_->GetStatus();
    }
    if (!next_chunk.has_value()) { break; }
    chunks.push_back(std::move(next_chunk.value()));
  }

  default_reader_ = nullptr;

  if (child_ids_.empty()) { return chunks; }

  auto child_results = WaitForChildren();
  if (!child_results.ok()) { return child_results.status(); }

  for (const auto& child_result : child_results.value()) {
    chunks.insert(chunks.end(), child_result.begin(), child_result.end());
  }

  return chunks;
}

ChunkStoreReader& AsyncNode::GetReader() {
  EnsureReader();
  return *default_reader_;
}

absl::Status AsyncNode::GetReaderStatus() const {
  if (default_reader_ == nullptr) {
    return absl::FailedPreconditionError("Reader is not initialized.");
  }
  return default_reader_->GetStatus();
}

std::unique_ptr<ChunkStoreReader> AsyncNode::MakeReader(
  bool ordered, bool remove_chunks, int n_chunks_to_buffer) {
  return std::make_unique<ChunkStoreReader>(chunk_store_.get(), ordered,
                                            remove_chunks, n_chunks_to_buffer);
}

AsyncNode& AsyncNode::SetReaderOptions(bool ordered, bool remove_chunks,
                                       int n_chunks_to_buffer) {
  if (default_reader_ != nullptr) {
    LOG(WARNING)
            << "Reader already exists on the node, changes to reader "
               "options will be ignored. You may want to reset the reader.";
  }
  EnsureReader(ordered, remove_chunks, n_chunks_to_buffer);
  return *this;
}

AsyncNode& AsyncNode::ResetReader() {
  default_reader_ = nullptr;
  return *this;
}

void AsyncNode::EnsureReader(bool ordered, bool remove_chunks,
                             int n_chunks_to_buffer) {
  if (default_reader_ == nullptr) {
    default_reader_ = std::make_unique<ChunkStoreReader>(
      chunk_store_.get(), ordered, remove_chunks, n_chunks_to_buffer);
  }
}

void AsyncNode::EnsureWriter(int n_chunks_to_buffer) {
  if (default_writer_ == nullptr) {
    default_writer_ = std::make_unique<ChunkStoreWriter>(chunk_store_.get(),
      n_chunks_to_buffer);
  }
}

absl::StatusOr<std::vector<std::vector<base::Chunk>>>
AsyncNode::WaitForChildren() {
  if (node_map_ == nullptr) {
    return absl::FailedPreconditionError(
      "Node map is not initialized or has been destroyed, cannot wait for "
      "children.");
  }

  std::vector<std::string_view> child_ids;
  child_ids.reserve(child_ids_.size());
  std::copy(child_ids_.begin(), child_ids_.end(),
            std::back_inserter(child_ids));

  std::vector<AsyncNode*> children = node_map_->Get(child_ids);

  std::vector<std::vector<base::Chunk>> child_results;
  child_results.reserve(children.size());
  for (const auto& [child, child_id] : iter::zip(children, child_ids)) {
    if (child == nullptr) {
      return absl::InternalError(
        absl::StrCat("Child node is null for id: ", child_id));
    }
    auto child_result = child->WaitForCompletion();
    if (!child_result.ok()) { return child_result.status(); }
    child_results.push_back(child_result.value());
  }
  return child_results;
}

} // namespace eglt
