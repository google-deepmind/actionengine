#ifndef EGLT_DATA_MSGPACK_H_
#define EGLT_DATA_MSGPACK_H_

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"

namespace cppack {

inline void CppackToBytes(const absl::Time& obj, cppack::Packer& packer) {
  const int64_t time = absl::ToUnixMicros(obj);
  packer(time);
}

inline void CppackFromBytes(absl::Time& obj, cppack::Unpacker& unpacker) {
  int64_t time;
  unpacker(time);
  obj = absl::FromUnixMicros(time);
}

inline void CppackToBytes(const eglt::base::ChunkMetadata& obj,
                          cppack::Packer& packer) {
  packer(obj.mimetype);
  packer(obj.timestamp);
}

inline void CppackFromBytes(eglt::base::ChunkMetadata& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.mimetype);
  unpacker(obj.timestamp);
}

inline void CppackToBytes(const eglt::base::Chunk& obj,
                          cppack::Packer& packer) {
  packer(obj.metadata);
  packer(obj.ref);
  packer(obj.data);
}

inline void CppackFromBytes(eglt::base::Chunk& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.metadata);
  unpacker(obj.ref);
  unpacker(obj.data);
}

inline void CppackToBytes(const eglt::base::NodeFragment& obj,
                          cppack::Packer& packer) {
  packer(obj.chunk);
  packer(obj.continued);
  packer(obj.id);
  packer(obj.seq);
}

inline void CppackFromBytes(eglt::base::NodeFragment& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.chunk);
  unpacker(obj.continued);
  unpacker(obj.id);
  unpacker(obj.seq);
}

inline void CppackToBytes(const eglt::base::NamedParameter& obj,
                          cppack::Packer& packer) {
  packer(obj.name);
  packer(obj.id);
}

inline void CppackFromBytes(eglt::base::NamedParameter& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.name);
  unpacker(obj.id);
}

inline void CppackToBytes(const eglt::base::ActionMessage& obj,
                          cppack::Packer& packer) {
  packer(obj.id);
  packer(obj.name);
  packer(obj.outputs);
  packer(obj.inputs);
}

inline void CppackFromBytes(eglt::base::ActionMessage& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.id);
  unpacker(obj.name);
  unpacker(obj.outputs);
  unpacker(obj.inputs);
}

inline void CppackToBytes(const eglt::base::SessionMessage& obj,
                          cppack::Packer& packer) {
  packer(obj.node_fragments);
  packer(obj.actions);
}

inline void CppackFromBytes(eglt::base::SessionMessage& obj,
                            cppack::Unpacker& unpacker) {
  unpacker(obj.node_fragments);
  unpacker(obj.actions);
}

}  // namespace cppack

#endif  // EGLT_DATA_MSGPACK_H_
