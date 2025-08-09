#ifndef EGLT_DATA_MSGPACK_H_
#define EGLT_DATA_MSGPACK_H_

#include <absl/status/status.h>
#include <absl/time/time.h>

#include "eglt/data/eg_structs.h"

namespace cppack {
class Packer;
class Unpacker;
}  // namespace cppack

namespace cppack {

void CppackToBytes(const absl::Status& status, Packer& packer);
void CppackFromBytes(absl::Status& status, Unpacker& unpacker);

void CppackToBytes(const absl::Time& obj, Packer& packer);
void CppackFromBytes(absl::Time& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::ChunkMetadata& obj, Packer& packer);
void CppackFromBytes(eglt::ChunkMetadata& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::Chunk& obj, Packer& packer);
void CppackFromBytes(eglt::Chunk& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::NodeFragment& obj, Packer& packer);
void CppackFromBytes(eglt::NodeFragment& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::Port& obj, Packer& packer);
void CppackFromBytes(eglt::Port& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::ActionMessage& obj, Packer& packer);
void CppackFromBytes(eglt::ActionMessage& obj, Unpacker& unpacker);

void CppackToBytes(const eglt::SessionMessage& obj, Packer& packer);
void CppackFromBytes(eglt::SessionMessage& obj, Unpacker& unpacker);

}  // namespace cppack

#endif  // EGLT_DATA_MSGPACK_H_
