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

#include "node_map.h"

#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/nodes/async_node.h"
#include "eglt/stores/chunk_store.h"

namespace eglt {
NodeMap::NodeMap(ChunkStoreFactory chunk_store_factory)
    : chunk_store_factory_(std::move(chunk_store_factory)) {}

NodeMap::NodeMap(NodeMap&& other) noexcept {
  concurrency::MutexLock lock(&other.mu_);

  nodes_ = std::move(other.nodes_);
  chunk_store_factory_ = std::move(other.chunk_store_factory_);
}

NodeMap& NodeMap::operator=(NodeMap&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  concurrency::TwoMutexLock lock(&mu_, &other.mu_);
  nodes_ = std::move(other.nodes_);
  chunk_store_factory_ = std::move(other.chunk_store_factory_);

  return *this;
}

AsyncNode* NodeMap::Get(std::string_view id,
                        const ChunkStoreFactory& chunk_store_factory) {
  concurrency::MutexLock lock(&mu_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(id, std::make_unique<AsyncNode>(
                           id, this, MakeChunkStore(chunk_store_factory)));
  }
  return nodes_[id].get();
}

std::vector<AsyncNode*> NodeMap::Get(
    const std::vector<std::string_view>& ids,
    const ChunkStoreFactory& chunk_store_factory) {
  concurrency::MutexLock lock(&mu_);

  std::vector<AsyncNode*> nodes;
  nodes.reserve(ids.size());

  for (const auto& id : ids) {
    if (!nodes_.contains(id)) {
      nodes_[id] = std::make_unique<AsyncNode>(
          id, this, MakeChunkStore(chunk_store_factory));
    }

    nodes.push_back(nodes_[id].get());
  }

  return nodes;
}

AsyncNode* NodeMap::operator[](std::string_view id) {
  concurrency::MutexLock lock(&mu_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(id, std::make_unique<AsyncNode>(
                           id, this, MakeChunkStore(chunk_store_factory_)));
  }
  return nodes_[id].get();
}

AsyncNode& NodeMap::insert(std::string_view id, AsyncNode&& node) {
  concurrency::MutexLock lock(&mu_);
  nodes_[id] = std::make_unique<AsyncNode>(std::move(node));
  return *nodes_[id];
}

bool NodeMap::contains(std::string_view id) const {
  concurrency::MutexLock lock(&mu_);
  return nodes_.contains(id);
}

std::unique_ptr<ChunkStore> NodeMap::MakeChunkStore(
    const ChunkStoreFactory& factory) const {
  if (factory) {
    return factory();
  }
  if (chunk_store_factory_) {
    return chunk_store_factory_();
  }
  return nullptr;
}
}  // namespace eglt
