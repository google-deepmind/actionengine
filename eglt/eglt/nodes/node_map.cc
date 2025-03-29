#include "node_map.h"

#include <string_view>
#include <vector>

#include <eglt/absl_headers.h>
#include <eglt/concurrency/concurrency.h>
#include <eglt/nodes/async_node.h>
#include <eglt/nodes/chunk_store.h>

namespace eglt {
NodeMap::NodeMap(ChunkStoreFactory chunk_store_factory) :
  chunk_store_factory_(std::move(chunk_store_factory)) {}

NodeMap::NodeMap(NodeMap&& other) noexcept {
  concurrency::MutexLock lock(&other.mutex_);

  MoveImpl(std::move(other));
}

NodeMap& NodeMap::operator=(NodeMap&& other) noexcept {
  if (this == &other) { return *this; }

  // TODO (helenapankov): this is a hack to avoid deadlock by establishing a
  // lock order. A longer term solution would be to come up with a utility
  // function similar to std::lock to lock several mutexes in a defined order.
  if (&mutex_ < &other.mutex_) {
    concurrency::MutexLock this_lock(&mutex_);
    concurrency::MutexLock other_lock(&other.mutex_);
    MoveImpl(std::move(other));
  }
  else {
    concurrency::MutexLock other_lock(&other.mutex_);
    concurrency::MutexLock this_lock(&mutex_);
    MoveImpl(std::move(other));
  }

  return *this;
}

AsyncNode* NodeMap::Get(std::string_view id,
                        const ChunkStoreFactory& chunk_store_factory) {
  concurrency::MutexLock lock(&mutex_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(
      id,
      std::make_unique<AsyncNode>(
        id,
        this,
        MakeChunkStore(std::move(chunk_store_factory))));
  }
  return nodes_[id].get();
}

std::vector<AsyncNode*> NodeMap::Get(const std::vector<std::string_view>& ids,
                                     const ChunkStoreFactory&
                                     chunk_store_factory) {
  concurrency::MutexLock lock(&mutex_);
  std::vector<AsyncNode*> nodes;
  nodes.reserve(ids.size());
  for (const auto& id : ids) {
    if (!nodes_.contains(id)) {
      nodes_[id] = std::make_unique<AsyncNode>(
        id,
        this,
        MakeChunkStore(std::move(chunk_store_factory)));
    }

    nodes.push_back(nodes_[id].get());
  }

  return nodes;
}

AsyncNode* NodeMap::operator[](std::string_view id) {
  concurrency::MutexLock lock(&mutex_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(
      id,
      std::make_unique<AsyncNode>(
        id,
        this,
        MakeChunkStore(chunk_store_factory_)));
  }
  return nodes_[id].get();
}

AsyncNode& NodeMap::insert(std::string_view id, AsyncNode&& node) {
  concurrency::MutexLock lock(&mutex_);
  nodes_[id] = std::make_unique<AsyncNode>(std::move(node));
  return *nodes_[id];
}

bool NodeMap::contains(std::string_view id) {
  concurrency::MutexLock lock(&mutex_);
  return nodes_.contains(id);
}

std::unique_ptr<ChunkStore> NodeMap::MakeChunkStore(
  const ChunkStoreFactory& factory) const {
  if (factory) { return factory(); }
  if (chunk_store_factory_) { return chunk_store_factory_(); }
  return nullptr;
}
} // namespace eglt
