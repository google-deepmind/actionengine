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

#ifndef ACTIONENGINE_STORES_BYTE_CHUNKING_H_
#define ACTIONENGINE_STORES_BYTE_CHUNKING_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include <absl/log/check.h>
#include <absl/status/statusor.h>

#include "actionengine/data/msgpack.h"  // IWYU pragma: keep
#include "actionengine/stores/local_chunk_store.h"

namespace act::data {

using Byte = uint8_t;

enum BytePacketType {
  kCompleteBytes = 0x00,
  kByteChunk = 0x01,
  kLengthSuffixedByteChunk = 0x02,
};

struct CompleteBytesPacket {
  static constexpr Byte kType = BytePacketType::kCompleteBytes;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id);

  std::vector<Byte> serialized_message;
  uint64_t transient_id;
};

struct ByteChunkPacket {
  static constexpr Byte kType = BytePacketType::kByteChunk;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id,
                                            uint32_t seq);

  std::vector<Byte> chunk;
  uint32_t seq;
  uint64_t transient_id;
};

struct LengthSuffixedByteChunkPacket {
  static constexpr Byte kType = BytePacketType::kLengthSuffixedByteChunk;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id, uint32_t seq,
                                            uint32_t length);

  std::vector<Byte> chunk;
  uint32_t length;
  uint32_t seq;
  uint64_t transient_id;
};

using BytePacket = std::variant<CompleteBytesPacket, ByteChunkPacket,
                                LengthSuffixedByteChunkPacket>;

BytePacket ProducePacket(std::vector<Byte>::const_iterator it,
                         std::vector<Byte>::const_iterator end,
                         uint64_t transient_id, uint32_t packet_size,
                         uint32_t seq = 0, int32_t length = -1,
                         bool force_no_length = false);

absl::StatusOr<BytePacket> ParseBytePacket(Byte* data, size_t size);

std::vector<BytePacket> SplitBytesIntoPackets(const std::vector<Byte>& data,
                                              uint64_t transient_id,
                                              uint64_t packet_size = 16384);

uint64_t GetTransientIdFromPacket(const BytePacket& packet);

std::vector<Byte> SerializeBytePacket(BytePacket packet);

class ChunkedBytes {
 public:
  absl::StatusOr<std::vector<Byte>> ConsumeCompleteBytes();
  absl::StatusOr<bool> FeedPacket(BytePacket packet);
  absl::StatusOr<bool> FeedSerializedPacket(std::vector<Byte> data);

 private:
  LocalChunkStore chunk_store_;
  size_t total_message_size_ = 0;
  uint32_t total_expected_chunks_ = -1;
};

}  // namespace act::data

#endif  // ACTIONENGINE_DATA_BYTE_CHUNKING_H_