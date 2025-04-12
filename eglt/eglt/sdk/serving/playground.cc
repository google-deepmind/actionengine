#include <iostream>

#include <eglt/data/eg_structs.h>
#include <eglt/data/msgpack.h>

namespace cppack {}  // namespace cppack

int main() {
  eglt::ChunkMetadata meta{
      .mimetype = "text/plain",
      .timestamp = absl::Now(),
  };
  eglt::Chunk chunk{
      .metadata = meta,
      .data = "Hello, World!",
  };

  eglt::NodeFragment node_fragment{
      .chunk = chunk,
      .continued = false,
      .id = "node1",
      .seq = 1,
  };
  eglt::SessionMessage msg{
      .node_fragments = {node_fragment},
  };

  auto vec = cppack::Pack(msg);
  std::cout << "Packed size: " << vec.size() << std::endl;
  auto unpacked = cppack::Unpack<eglt::SessionMessage>(vec);
  LOG(INFO) << "unpacked: " << unpacked << std::endl;
}