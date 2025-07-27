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

#include "eglt/stores/byte_chunking.h"

#include <string>
#include <string_view>

#include <absl/base/optimization.h>
#include <absl/container/inlined_vector.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/strings/str_format.h>
#include <absl/time/time.h>

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"

namespace eglt::data {

template <typename T>
static absl::InlinedVector<Byte, 8> NumberToBEBytes(T number) {
  absl::InlinedVector<Byte, 8> bytes;
  bytes.reserve(sizeof(T));
  for (int i = sizeof(T) - 1; i >= 0; --i) {
    bytes.push_back(static_cast<Byte>((number >> (i * 8)) & 0xFF));
  }
  return bytes;
}

template <typename T>
static absl::InlinedVector<Byte, 8> NumberToLEBytes(T number) {
  absl::InlinedVector<Byte, 8> bytes;
  bytes.reserve(sizeof(T));
  for (int i = 0; i < sizeof(T); ++i) {
    bytes.push_back(static_cast<Byte>((number >> (i * 8)) & 0xFF));
  }
  return bytes;
}

absl::StatusOr<BytePacket> ParseBytePacket(std::vector<Byte>&& data) {
  if (data.size() < sizeof(uint64_t) + 1) {
    return absl::InvalidArgumentError(
        "BytePacket data size is less than 9 bytes. No valid packet "
        "is less than 9 bytes.");
  }

  size_t remaining_data_size = data.size();

  const Byte type = *data.rbegin();
  if (type != BytePacketType::kCompleteBytes &&
      type != BytePacketType::kByteChunk &&
      type != BytePacketType::kLengthSuffixedByteChunk) {
    return absl::InvalidArgumentError(
        absl::StrFormat("BytePacket type %d is not supported", type));
  }

  data.pop_back();
  remaining_data_size -= sizeof(Byte);

  const uint64_t transient_id = *reinterpret_cast<uint64_t*>(
      data.data() + remaining_data_size - sizeof(int64_t));
  data.erase(data.end() - sizeof(uint64_t), data.end());
  remaining_data_size -= sizeof(uint64_t);

  // Plain SessionMessage
  if (type == BytePacketType::kCompleteBytes) {
    return CompleteBytesPacket{
        .serialized_message = std::vector(data.begin(), data.end()),
        .transient_id = transient_id};
  }

  const uint32_t seq = *reinterpret_cast<uint32_t*>(
      data.data() + remaining_data_size - sizeof(uint32_t));
  data.erase(data.end() - sizeof(uint32_t), data.end());
  remaining_data_size -= sizeof(uint32_t);

  // SessionMessage Chunk
  if (type == BytePacketType::kByteChunk) {
    std::vector chunk(data.begin(), data.end());
    return ByteChunkPacket{
        .chunk = std::move(chunk), .seq = seq, .transient_id = transient_id};
  }

  // Length Suffix SessionMessage Chunk
  if (type == BytePacketType::kLengthSuffixedByteChunk) {
    if (remaining_data_size < sizeof(uint32_t)) {
      return absl::InvalidArgumentError(
          "Invalid WebRtcEvergreenPacket: marked as "
          "LengthSuffixedSessionMessageChunk but data size is less than 13");
    }

    const uint32_t length = *reinterpret_cast<uint32_t*>(
        data.data() + remaining_data_size - sizeof(uint32_t));
    data.erase(data.end() - sizeof(uint32_t), data.end());
    remaining_data_size -= sizeof(uint32_t);

    std::vector chunk(data.begin(), data.end());
    return LengthSuffixedByteChunkPacket{.chunk = std::move(chunk),
                                         .length = length,
                                         .seq = seq,
                                         .transient_id = transient_id};
  }

  // If we reach here, it means the type is not recognized.
  return absl::InvalidArgumentError(
      "WebRtcEvergreenPacket type is not supported");
}

std::vector<BytePacket> SplitBytesIntoPackets(const std::vector<Byte>& data,
                                              uint64_t transient_id,
                                              uint64_t packet_size) {
  std::vector<BytePacket> packets;

  if (data.size() <= packet_size - sizeof(uint64_t) - 1) {
    // If the data fits into a single packet, create a plain session message.
    packets.emplace_back(CompleteBytesPacket{.serialized_message = data,
                                             .transient_id = transient_id});
    return packets;
  }

  packets.reserve((data.size() + packet_size - 1) / packet_size);

  if (packet_size < 18) {
    LOG(FATAL) << "Packet size must be at least 18 bytes to accommodate the "
                  "header of 17 bytes.";
    ABSL_ASSUME(false);
  }

  // If the data is larger than the packet size, split it into chunks.
  const uint64_t first_chunk_size = packet_size - 17;
  LengthSuffixedByteChunkPacket first_chunk{
      .chunk = std::vector(data.begin(), data.begin() + first_chunk_size),
      .length = 0,  // This will be set later.
      .seq = 0,
      .transient_id = transient_id};
  packets.emplace_back(std::move(first_chunk));

  uint32_t seq = 1;
  uint64_t offset = first_chunk_size;
  while (offset < data.size()) {
    uint64_t remaining_size = static_cast<uint64_t>(data.size()) - offset;
    const uint64_t chunk_size = std::min(packet_size - 13, remaining_size);
    ByteChunkPacket chunk{
        .chunk = std::vector(data.begin() + offset,
                             data.begin() + offset + chunk_size),
        .seq = seq,
        .transient_id = transient_id};

    packets.emplace_back(std::move(chunk));
    offset += chunk_size;
    ++seq;
  }

  std::get<LengthSuffixedByteChunkPacket>(packets[0]).length =
      static_cast<uint32_t>(packets.size());

  return packets;
}

uint64_t GetTransientIdFromPacket(const BytePacket& packet) {
  if (std::holds_alternative<CompleteBytesPacket>(packet)) {
    return std::get<CompleteBytesPacket>(packet).transient_id;
  }
  if (std::holds_alternative<ByteChunkPacket>(packet)) {
    return std::get<ByteChunkPacket>(packet).transient_id;
  }
  if (std::holds_alternative<LengthSuffixedByteChunkPacket>(packet)) {
    return std::get<LengthSuffixedByteChunkPacket>(packet).transient_id;
  }
  return 0;  // Default value if no transient ID is found.
}

std::vector<Byte> SerializeBytePacket(BytePacket packet) {
  std::vector<Byte> bytes;
  uint64_t transient_id = 0;
  Byte type_byte = 0;

  if (std::holds_alternative<CompleteBytesPacket>(packet)) {
    auto [serialized_message, id] =
        std::move(std::get<CompleteBytesPacket>(packet));
    bytes = std::move(serialized_message);
    transient_id = id;
    type_byte = BytePacketType::kCompleteBytes;
  }

  if (std::holds_alternative<ByteChunkPacket>(packet)) {
    auto [chunk, seq, id] = std::move(std::get<ByteChunkPacket>(packet));
    bytes = std::move(chunk);
    transient_id = id;
    type_byte = BytePacketType::kByteChunk;

    auto seq_bytes = NumberToLEBytes(seq);
    bytes.insert(bytes.end(), seq_bytes.begin(), seq_bytes.end());
  }

  if (std::holds_alternative<LengthSuffixedByteChunkPacket>(packet)) {
    auto [chunk, length, seq, id] =
        std::move(std::get<LengthSuffixedByteChunkPacket>(packet));
    bytes = std::move(chunk);
    transient_id = id;
    type_byte = BytePacketType::kLengthSuffixedByteChunk;

    auto length_bytes = NumberToLEBytes(length);
    bytes.insert(bytes.end(), length_bytes.begin(), length_bytes.end());

    auto seq_bytes = NumberToLEBytes(seq);
    bytes.insert(bytes.end(), seq_bytes.begin(), seq_bytes.end());
  }

  auto transient_id_bytes = NumberToLEBytes(transient_id);
  bytes.insert(bytes.end(), transient_id_bytes.begin(),
               transient_id_bytes.end());

  bytes.push_back(type_byte);

  return bytes;
}

absl::StatusOr<std::vector<Byte>> ChunkedBytes::ConsumeCompleteBytes() {
  if (chunk_store_.SizeOrDie() < total_expected_chunks_) {
    return absl::FailedPreconditionError(
        "Cannot consume message, not all chunks received yet");
  }

  std::vector<Byte> message_data;
  message_data.reserve(total_message_size_);

  for (int i = 0; i < total_expected_chunks_; ++i) {
    absl::StatusOr<Chunk> chunk = chunk_store_.Get(i, absl::ZeroDuration());
    if (!chunk.ok()) {
      return chunk.status();
    }
    message_data.insert(message_data.end(), chunk->data.begin(),
                        chunk->data.end());
  }

  return message_data;
}

absl::StatusOr<bool> ChunkedBytes::FeedPacket(BytePacket packet) {
  if (chunk_store_.SizeOrDie() >= total_expected_chunks_ &&
      total_expected_chunks_ != -1) {
    return absl::FailedPreconditionError(
        "Cannot feed more packets, already received all expected chunks");
  }

  if (std::holds_alternative<CompleteBytesPacket>(packet)) {
    auto& serialized_message =
        std::get<CompleteBytesPacket>(packet).serialized_message;
    Chunk data_chunk{
        .metadata = {},
        .data = std::string(std::make_move_iterator(serialized_message.begin()),
                            std::make_move_iterator(serialized_message.end()))};
    total_message_size_ += data_chunk.data.size();
    total_expected_chunks_ = 1;  // This is a single message, not chunked.
    chunk_store_
        .Put(0, std::move(data_chunk),
             /*final=*/true)
        .IgnoreError();
    return true;
  }

  if (std::holds_alternative<ByteChunkPacket>(packet)) {
    auto& chunk = std::get<ByteChunkPacket>(packet);
    Chunk data_chunk{
        .metadata = {},
        .data = std::string(std::make_move_iterator(chunk.chunk.begin()),
                            std::make_move_iterator(chunk.chunk.end()))};
    total_message_size_ += data_chunk.data.size();
    chunk_store_
        .Put(static_cast<int>(chunk.seq), std::move(data_chunk),
             /*final=*/
             chunk.seq == total_expected_chunks_ - 1)
        .IgnoreError();
    return chunk_store_.SizeOrDie() == total_expected_chunks_;
  }

  if (std::holds_alternative<LengthSuffixedByteChunkPacket>(packet)) {
    auto& chunk = std::get<LengthSuffixedByteChunkPacket>(packet);
    if (total_expected_chunks_ != -1) {
      return absl::InvalidArgumentError(
          "Cannot have more than one WebRtcLengthSuffixedSessionMessageChunk "
          "in a sequence");
    }
    total_message_size_ += chunk.chunk.size();
    total_expected_chunks_ =
        chunk.length;  // Set the total expected chunks from this packet.
    chunk_store_
        .Put(static_cast<int>(chunk.seq),
             Chunk{.metadata = {},
                   .data =
                       std::string(std::make_move_iterator(chunk.chunk.begin()),
                                   std::make_move_iterator(chunk.chunk.end()))},
             /*final=*/
             chunk.seq == total_expected_chunks_ - 1)
        .IgnoreError();
    return chunk_store_.SizeOrDie() == total_expected_chunks_;
  }

  return absl::InvalidArgumentError("Unknown WebRtcEvergreenPacket type");
}

absl::StatusOr<bool> ChunkedBytes::FeedSerializedPacket(
    std::vector<Byte> data) {
  if (chunk_store_.SizeOrDie() >= total_expected_chunks_ &&
      total_expected_chunks_ != -1) {
    return absl::FailedPreconditionError(
        "Cannot feed more packets, already received all expected chunks");
  }

  absl::StatusOr<BytePacket> packet = ParseBytePacket(std::move(data));
  if (!packet.ok()) {
    return packet.status();
  }

  return FeedPacket(*std::move(packet));
}

uint32_t ByteChunkPacket::GetSerializedMetadataSize(uint64_t transient_id,
                                                    uint32_t seq) {
  cppack::Packer packer;
  packer.process(transient_id);
  packer.process(seq);
  return packer.vector().size();
}

uint32_t CompleteBytesPacket::GetSerializedMetadataSize(uint64_t transient_id) {
  cppack::Packer packer;
  packer.process(transient_id);
  return packer.vector().size();
}

uint32_t LengthSuffixedByteChunkPacket::GetSerializedMetadataSize(
    uint64_t transient_id, uint32_t seq, uint32_t length) {
  cppack::Packer packer;
  packer.process(transient_id);
  packer.process(seq);
  packer.process(length);
  return packer.vector().size();
}

std::vector<Byte> SerializeBytePacket2(const BytePacket& packet) {
  cppack::Packer packer;
  if (std::holds_alternative<CompleteBytesPacket>(packet)) {
    const auto& complete_packet = std::get<CompleteBytesPacket>(packet);
    packer.process(static_cast<Byte>(CompleteBytesPacket::kType));
    packer.process(complete_packet.transient_id);

    std::vector<Byte> result = packer.vector();
    result.reserve(result.size() + complete_packet.serialized_message.size());
    result.insert(result.end(), complete_packet.serialized_message.begin(),
                  complete_packet.serialized_message.end());
    return result;
  }

  if (std::holds_alternative<ByteChunkPacket>(packet)) {
    const auto& chunk_packet = std::get<ByteChunkPacket>(packet);
    packer.process(static_cast<Byte>(ByteChunkPacket::kType));
    packer.process(chunk_packet.transient_id);
    packer.process(chunk_packet.seq);

    std::vector<Byte> result = packer.vector();
    result.reserve(result.size() + chunk_packet.chunk.size());
    result.insert(result.end(), chunk_packet.chunk.begin(),
                  chunk_packet.chunk.end());
    return result;
  }

  if (std::holds_alternative<LengthSuffixedByteChunkPacket>(packet)) {
    const auto& length_chunk_packet =
        std::get<LengthSuffixedByteChunkPacket>(packet);
    packer.process(static_cast<Byte>(LengthSuffixedByteChunkPacket::kType));
    packer.process(length_chunk_packet.transient_id);
    packer.process(length_chunk_packet.seq);
    packer.process(length_chunk_packet.length);

    std::vector<Byte> result = packer.vector();
    result.reserve(result.size() + length_chunk_packet.chunk.size());
    result.insert(result.end(), length_chunk_packet.chunk.begin(),
                  length_chunk_packet.chunk.end());
    return result;
  }

  return {};
}

BytePacket DeserializeBytePacket2(const std::vector<Byte>& data) {
  cppack::Unpacker unpacker(data.data(), data.size());

  Byte type_byte;
  unpacker.process(type_byte);
  const auto type = static_cast<BytePacketType>(type_byte);

  if (type == BytePacketType::kCompleteBytes) {
    CompleteBytesPacket packet;
    unpacker.process(packet.transient_id);
    const Byte* offset = unpacker.GetDataPtr();
    const auto header_size = offset - data.data();
    packet.serialized_message =
        std::vector(data.begin() + header_size, data.end());
    return packet;
  }

  if (type == BytePacketType::kByteChunk) {
    ByteChunkPacket packet;
    unpacker.process(packet.transient_id);
    unpacker.process(packet.seq);
    const Byte* offset = unpacker.GetDataPtr();
    const auto header_size = offset - data.data();
    packet.chunk = std::vector(data.begin() + header_size, data.end());
    return packet;
  }

  if (type == BytePacketType::kLengthSuffixedByteChunk) {
    LengthSuffixedByteChunkPacket packet;
    unpacker.process(packet.transient_id);
    unpacker.process(packet.seq);
    unpacker.process(packet.length);
    const Byte* offset = unpacker.GetDataPtr();
    const auto header_size = offset - data.data();
    packet.chunk = std::vector(data.begin() + header_size, data.end());
    return packet;
  }

  LOG(FATAL) << "Unknown BytePacketType: " << static_cast<int>(type)
             << ". Cannot deserialize BytePacket.";
  ABSL_ASSUME(false);
}

}  // namespace eglt::data