#ifndef EGLT_DATA_MSGPACK_H_
#define EGLT_DATA_MSGPACK_H_

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"

namespace cppack {

inline void CppackToBytes(const absl::Status& status, cppack::Packer& packer) {
  packer(status.raw_code());
  packer(std::string(status.message()));
}

inline void CppackFromBytes(absl::Status& status, cppack::Unpacker& unpacker) {
  int code;
  std::string message;
  unpacker(code);
  unpacker(message);
  status = absl::Status(static_cast<absl::StatusCode>(code), message);
}

inline void CppackToBytes(const absl::Time& obj, cppack::Packer& packer) {
  const int64_t time = absl::ToUnixMicros(obj);
  packer(time);
}

inline void CppackFromBytes(absl::Time& obj, cppack::Unpacker& unpacker) {
  int64_t time;
  unpacker(time);
  obj = absl::FromUnixMicros(time);
}

inline void CppackToBytes(const eglt::ChunkMetadata& obj,
                          cppack::Packer& packer) {
  packer(obj.mimetype);
  packer(obj.timestamp);
}

inline void CppackFromBytes(eglt::ChunkMetadata& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.mimetype);
  unpacker(obj.timestamp);
}

inline void CppackToBytes(const eglt::Chunk& obj, cppack::Packer& packer) {
  packer(obj.metadata);
  packer(obj.ref);
  packer(obj.data);
}

inline void CppackFromBytes(eglt::Chunk& obj, cppack::Unpacker& unpacker) {
  unpacker(obj.metadata);
  unpacker(obj.ref);
  unpacker(obj.data);
}

inline void CppackToBytes(const eglt::NodeFragment& obj,
                          cppack::Packer& packer) {
  packer(obj.chunk);
  packer(obj.continued);
  packer(obj.id);
  packer(obj.seq);
}

inline void CppackFromBytes(eglt::NodeFragment& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.chunk);
  unpacker(obj.continued);
  unpacker(obj.id);
  unpacker(obj.seq);
}

inline void CppackToBytes(const eglt::Port& obj,
                          cppack::Packer& packer) {
  packer(obj.name);
  packer(obj.id);
}

inline void CppackFromBytes(eglt::Port& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.name);
  unpacker(obj.id);
}

inline void CppackToBytes(const eglt::ActionMessage& obj,
                          cppack::Packer& packer) {
  packer(obj.id);
  packer(obj.name);
  packer(obj.outputs);
  packer(obj.inputs);
}

inline void CppackFromBytes(eglt::ActionMessage& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.id);
  unpacker(obj.name);
  unpacker(obj.outputs);
  unpacker(obj.inputs);
}

inline void CppackToBytes(const eglt::SessionMessage& obj,
                          cppack::Packer& packer) {
  packer(obj.node_fragments);
  packer(obj.actions);
}

inline void CppackFromBytes(eglt::SessionMessage& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.node_fragments);
  unpacker(obj.actions);
}

}  // namespace cppack

#endif  // EGLT_DATA_MSGPACK_H_
