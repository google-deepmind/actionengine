#include "eglt/data/msgpack.h"

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"

namespace cppack {

void CppackToBytes(const absl::Status& status, Packer& packer) {
  packer(status.raw_code());
  packer(std::string(status.message()));
}

void CppackFromBytes(absl::Status& status, Unpacker& unpacker) {
  int code;
  std::string message;
  unpacker(code);
  unpacker(message);
  status = absl::Status(static_cast<absl::StatusCode>(code), message);
}

void CppackToBytes(const absl::Time& obj, Packer& packer) {
  const int64_t time = absl::ToUnixMicros(obj);
  packer(time);
}

void CppackFromBytes(absl::Time& obj, Unpacker& unpacker) {
  int64_t time;
  unpacker(time);
  obj = absl::FromUnixMicros(time);
}

void CppackToBytes(const eglt::ChunkMetadata& obj, Packer& packer) {
  packer(obj.mimetype);
  packer(obj.timestamp);
}

void CppackFromBytes(eglt::ChunkMetadata& obj, Unpacker& unpacker) {
  unpacker(obj.mimetype);
  unpacker(obj.timestamp);
}

void CppackToBytes(const eglt::Chunk& obj, Packer& packer) {
  packer(obj.metadata);
  packer(obj.ref);
  const std::vector<uint8_t> data(obj.data.begin(), obj.data.end());
  packer(data);
}

void CppackFromBytes(eglt::Chunk& obj, Unpacker& unpacker) {
  unpacker(obj.metadata);
  unpacker(obj.ref);
  std::vector<uint8_t> data;
  unpacker(data);
  obj.data = std::string(data.begin(), data.end());
}

void CppackToBytes(const eglt::NodeFragment& obj, Packer& packer) {
  packer(obj.chunk);
  packer(obj.continued);
  packer(obj.id);
  packer(obj.seq);
}

void CppackFromBytes(eglt::NodeFragment& obj, Unpacker& unpacker) {
  unpacker(obj.chunk);
  unpacker(obj.continued);
  unpacker(obj.id);
  unpacker(obj.seq);
}

void CppackToBytes(const eglt::Port& obj, Packer& packer) {
  packer(obj.name);
  packer(obj.id);
}

void CppackFromBytes(eglt::Port& obj, Unpacker& unpacker) {
  unpacker(obj.name);
  unpacker(obj.id);
}

void CppackToBytes(const eglt::ActionMessage& obj, Packer& packer) {
  packer(obj.id);
  packer(obj.name);
  packer(obj.inputs);
  packer(obj.outputs);
}

void CppackFromBytes(eglt::ActionMessage& obj, Unpacker& unpacker) {
  unpacker(obj.id);
  unpacker(obj.name);
  unpacker(obj.inputs);
  unpacker(obj.outputs);
}

void CppackToBytes(const eglt::SessionMessage& obj, Packer& packer) {
  packer(obj.node_fragments);
  packer(obj.actions);
}

void CppackFromBytes(eglt::SessionMessage& obj, Unpacker& unpacker) {
  unpacker(obj.node_fragments);
  unpacker(obj.actions);
}

}  // namespace cppack