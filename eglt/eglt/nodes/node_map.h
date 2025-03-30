#ifndef EGLT_NODES_NODE_MAP_H_
#define EGLT_NODES_NODE_MAP_H_

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/nodes/async_node.h"
#include "eglt/nodes/chunk_store.h"

namespace eglt {
class AsyncNode;

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

  auto insert(std::string_view id, AsyncNode&& node) -> AsyncNode&;
  bool contains(std::string_view id);

private:
  concurrency::Mutex mutex_;
  absl::flat_hash_map<std::string, std::unique_ptr<AsyncNode>> nodes_
    ABSL_GUARDED_BY(mutex_){};

  ChunkStoreFactory chunk_store_factory_;

  std::unique_ptr<ChunkStore> MakeChunkStore(
    const ChunkStoreFactory& factory = {}) const;
};
} // namespace eglt

#endif  // EGLT_NODES_NODE_MAP_H_
