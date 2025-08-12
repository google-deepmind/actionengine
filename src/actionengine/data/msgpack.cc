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

#include "actionengine/data/msgpack.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "actionengine/data/types.h"
#include "cppack/msgpack.h"

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

void CppackToBytes(const act::ChunkMetadata& obj, Packer& packer) {
  packer(obj.mimetype);
  packer(obj.timestamp);
}

void CppackFromBytes(act::ChunkMetadata& obj, Unpacker& unpacker) {
  unpacker(obj.mimetype);
  unpacker(obj.timestamp);
}

void CppackToBytes(const act::Chunk& obj, Packer& packer) {
  const std::vector<uint8_t> data(obj.data.begin(), obj.data.end());
  packer(data);
  packer(obj.ref);
  if (!obj.metadata) {
    const std::optional<act::ChunkMetadata> empty_metadata;
    packer(empty_metadata);
  } else {
    packer(obj.metadata.value());
  }
}

void CppackFromBytes(act::Chunk& obj, Unpacker& unpacker) {
  std::vector<uint8_t> data;
  unpacker(data);
  obj.data = std::string(data.begin(), data.end());
  unpacker(obj.ref);
  unpacker(obj.metadata);
}

void CppackToBytes(const act::NodeRef& obj, Packer& packer) {
  packer(obj.id);
  packer(obj.offset);
  packer(obj.length);
}

void CppackFromBytes(act::NodeRef& obj, Unpacker& unpacker) {
  unpacker(obj.id);
  unpacker(obj.offset);
  unpacker(obj.length);
}

void CppackToBytes(const act::NodeFragment& obj, Packer& packer) {
  uint8_t data_variant_index = 0;

  if (std::holds_alternative<act::Chunk>(obj.data)) {
    data_variant_index = 0;
    packer(data_variant_index);
    packer(std::get<act::Chunk>(obj.data));
  } else if (std::holds_alternative<act::NodeRef>(obj.data)) {
    data_variant_index = 1;
    packer(data_variant_index);
    packer(std::get<act::NodeRef>(obj.data));
  } else {
    LOG(FATAL) << "NodeFragment data must be either Chunk or NodeRef.";
    ABSL_ASSUME(false);
  }
  packer(obj.continued);
  packer(obj.id);
  packer(obj.seq);
}

void CppackFromBytes(act::NodeFragment& obj, Unpacker& unpacker) {
  uint8_t data_variant_index;
  unpacker(data_variant_index);
  if (data_variant_index == 0) {
    act::Chunk chunk;
    unpacker(chunk);
    obj.data = std::move(chunk);
  } else if (data_variant_index == 1) {
    act::NodeRef node_ref;
    unpacker(node_ref);
    obj.data = std::move(node_ref);
  } else {
    LOG(FATAL) <<
 "NodeFragment data must be either Chunk or NodeRef, got index "
               << data_variant_index << ".";
    ABSL_ASSUME(false);
  }
  unpacker(obj.continued);
  unpacker(obj.id);
  unpacker(obj.seq);
}

void CppackToBytes(const act::Port& obj, Packer& packer) {
  packer(obj.name);
  packer(obj.id);
}

void CppackFromBytes(act::Port& obj, Unpacker& unpacker) {
  unpacker(obj.name);
  unpacker(obj.id);
}

void CppackToBytes(const act::ActionMessage& obj, Packer& packer) {
  packer(obj.id);
  packer(obj.name);
  packer(obj.inputs);
  packer(obj.outputs);
}

void CppackFromBytes(act::ActionMessage& obj, Unpacker& unpacker) {
  unpacker(obj.id);
  unpacker(obj.name);
  unpacker(obj.inputs);
  unpacker(obj.outputs);
}

void CppackToBytes(const act::WireMessage& obj, Packer& packer) {
  packer(obj.node_fragments);
  packer(obj.actions);
}

void CppackFromBytes(act::WireMessage& obj, Unpacker& unpacker) {
  unpacker(obj.node_fragments);
  unpacker(obj.actions);
}

} // namespace cppack