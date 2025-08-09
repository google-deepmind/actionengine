#ifndef ACTIONENGINE_DATA_MSGPACK_H_
#define ACTIONENGINE_DATA_MSGPACK_H_

#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/data/types.h"

namespace cppack {
class Packer;
class Unpacker;
}  // namespace cppack

namespace cppack {

void CppackToBytes(const absl::Status& status, Packer& packer);
void CppackFromBytes(absl::Status& status, Unpacker& unpacker);

void CppackToBytes(const absl::Time& obj, Packer& packer);
void CppackFromBytes(absl::Time& obj, Unpacker& unpacker);

void CppackToBytes(const act::ChunkMetadata& obj, Packer& packer);
void CppackFromBytes(act::ChunkMetadata& obj, Unpacker& unpacker);

void CppackToBytes(const act::Chunk& obj, Packer& packer);
void CppackFromBytes(act::Chunk& obj, Unpacker& unpacker);

void CppackToBytes(const act::NodeFragment& obj, Packer& packer);
void CppackFromBytes(act::NodeFragment& obj, Unpacker& unpacker);

void CppackToBytes(const act::Port& obj, Packer& packer);
void CppackFromBytes(act::Port& obj, Unpacker& unpacker);

void CppackToBytes(const act::ActionMessage& obj, Packer& packer);
void CppackFromBytes(act::ActionMessage& obj, Unpacker& unpacker);

void CppackToBytes(const act::SessionMessage& obj, Packer& packer);
void CppackFromBytes(act::SessionMessage& obj, Unpacker& unpacker);

}  // namespace cppack

#endif  // ACTIONENGINE_DATA_MSGPACK_H_
