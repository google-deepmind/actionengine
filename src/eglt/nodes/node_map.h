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

#ifndef EGLT_NODES_NODE_MAP_H_
#define EGLT_NODES_NODE_MAP_H_

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/nodes/async_node.h"
#include "eglt/stores/chunk_store.h"

namespace eglt {
class AsyncNode;

/**
 * @brief
 *   A thread-safe map of Evergreen nodes.
 *
 * This class is used to manage a collection of nodes, allowing for the
 * retrieval and insertion of nodes by their ID. It also provides a way to
 * use a custom ChunkStore factory for creating chunk stores.
 *
 * @headerfile eglt/nodes/node_map.h
 */
class NodeMap {
 public:
  explicit NodeMap(ChunkStoreFactory chunk_store_factory = {});
  ~NodeMap() = default;

  NodeMap(const NodeMap& other) = delete;
  NodeMap(NodeMap&& other) noexcept;

  NodeMap& operator=(const NodeMap& other) = delete;
  NodeMap& operator=(NodeMap&& other) noexcept;

  auto Get(std::string_view id,
           const ChunkStoreFactory& chunk_store_factory = {}) -> AsyncNode*;
  auto operator[](std::string_view id) -> AsyncNode*;

  auto Get(const std::vector<std::string_view>& ids,
           const ChunkStoreFactory& chunk_store_factory = {})
      -> std::vector<AsyncNode*>;

  [[nodiscard]] std::unique_ptr<AsyncNode> Extract(std::string_view id) {
    std::unique_ptr<AsyncNode> node;
    {
      concurrency::MutexLock lock(&mu_);
      if (const auto map_node = nodes_.extract(id); !map_node.empty()) {
        node = std::move(map_node.mapped());
      }
    }
    return node;
  }

  /// @private
  auto insert(std::string_view id, AsyncNode&& node) -> AsyncNode&;
  /// @private
  bool contains(std::string_view id);

 private:
  concurrency::Mutex mu_;
  absl::flat_hash_map<std::string, std::unique_ptr<AsyncNode>> nodes_
      ABSL_GUARDED_BY(mu_){};

  ChunkStoreFactory chunk_store_factory_;

  std::unique_ptr<ChunkStore> MakeChunkStore(
      const ChunkStoreFactory& factory = {}) const;
};
}  // namespace eglt

#endif  // EGLT_NODES_NODE_MAP_H_
