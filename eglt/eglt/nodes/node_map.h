#ifndef EGLT_NODES_NODE_MAP_H_
#define EGLT_NODES_NODE_MAP_H_

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <eglt/absl_headers.h>
#include <eglt/concurrency/concurrency.h>
#include <eglt/nodes/chunk_store.h>

namespace eglt {

class AsyncNode;

class NodeMap {
 public:
  explicit NodeMap(ChunkStoreFactory chunk_store_factory = {});
  ~NodeMap() = default;

  NodeMap(const NodeMap& other) = delete;
  NodeMap(NodeMap&& other);

  NodeMap& operator=(const NodeMap& other) = delete;
  NodeMap& operator=(NodeMap&& other);

  auto Get(std::string_view id, ChunkStoreFactory chunk_store_factory = {})
  -> AsyncNode*;
  auto operator[](std::string_view id) -> AsyncNode*;

  auto Get(std::vector<std::string_view> ids,
           ChunkStoreFactory chunk_store_factory = {})
  -> std::vector<AsyncNode*>;

  auto insert(std::string_view id, AsyncNode&& node) -> AsyncNode&;
  bool contains(std::string_view id);

 private:
  void MoveImpl(NodeMap&& other) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    nodes_ = std::move(other.nodes_);
    chunk_store_factory_ = std::move(other.chunk_store_factory_);
  }

  concurrency::Mutex mutex_;
  absl::flat_hash_map<std::string, std::unique_ptr<AsyncNode>> nodes_
      ABSL_GUARDED_BY(mutex_){};

  ChunkStoreFactory chunk_store_factory_;

  std::unique_ptr<ChunkStore> MakeChunkStore(ChunkStoreFactory factory = {});
};

}  // namespace eglt

#endif  // EGLT_NODES_NODE_MAP_H_
