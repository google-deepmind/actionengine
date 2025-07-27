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

#ifndef EGLT_STORES_BYTE_CHUNKING_H_
#define EGLT_STORES_BYTE_CHUNKING_H_

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

#include "eglt/data/msgpack.h"  // IWYU pragma: keep
#include "eglt/stores/local_chunk_store.h"

namespace eglt::data {

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

inline BytePacket ProducePacket(std::vector<Byte>::const_iterator it,
                                std::vector<Byte>::const_iterator end,
                                uint64_t transient_id, uint32_t packet_size,
                                uint32_t seq = 0, int32_t length = -1,
                                bool force_no_length = false) {
  const auto remaining_size = static_cast<uint32_t>(std::distance(it, end));

  const uint32_t complete_bytes_size =
      CompleteBytesPacket::GetSerializedMetadataSize(transient_id) + 1;
  if (seq == 0 && (complete_bytes_size + remaining_size <= packet_size)) {
    return CompleteBytesPacket{.serialized_message = std::vector(it, end),
                               .transient_id = transient_id};
  }

  const uint32_t length_suffixed_size =
      LengthSuffixedByteChunkPacket::GetSerializedMetadataSize(
          transient_id, seq, length >= 1 ? length : seq + 1) +
      1;
  CHECK(!force_no_length || length <= 0)
      << "force_no_length is true, but length is set to " << length
      << ". Cannot both explicitly set length and force no length.";
  if (!force_no_length &&
          (length_suffixed_size + remaining_size <= packet_size) ||
      length >= 1) {
    // if length is not explicitly defined, we want to produce a packet with
    // length iff it is the last packet in the sequence.
    const uint32_t payload_size =
        std::min(packet_size - length_suffixed_size, remaining_size);
    std::vector chunk(it, it + payload_size);
    return LengthSuffixedByteChunkPacket{
        .chunk = std::move(chunk),
        .length = length >= 1 ? length : seq + 1,
        .seq = seq,
        .transient_id = transient_id};
  }

  const uint32_t byte_chunk_size =
      ByteChunkPacket::GetSerializedMetadataSize(transient_id, seq) + 1;
  const auto chunk_size =
      std::min(packet_size - byte_chunk_size, remaining_size);
  return ByteChunkPacket{.chunk = std::vector(it, it + chunk_size),
                         .seq = seq,
                         .transient_id = transient_id};
}

std::vector<Byte> SerializeBytePacket2(const BytePacket& packet);

BytePacket DeserializeBytePacket2(const std::vector<Byte>& data);

class ByteSplitter {
 public:
  ByteSplitter(const std::vector<Byte>& data, uint64_t transient_id,
               uint32_t packet_size = 16384)
      : data_(&data), transient_id_(transient_id), packet_size_(packet_size) {}

  struct Iterator {
    using iterator_category = std::input_iterator_tag;
    using value_type = BytePacket;
    using pointer = const value_type*;
    using reference = const value_type&;

    Iterator(std::vector<Byte>::const_iterator it,
             std::vector<Byte>::const_iterator end, uint64_t transient_id,
             uint64_t packet_size, uint32_t seq = 0, int32_t length = -1,
             bool force_no_length = false)
        : it_(it),
          end_(end),
          transient_id_(transient_id),
          packet_size_(packet_size),
          seq_(seq),
          length_(length),
          force_no_length_(force_no_length) {}

    reference operator*() {
      EnsurePacket();
      return *produced_packet_;
    }

    pointer operator->() {
      EnsurePacket();
      return &(*produced_packet_);
    }

    Iterator& operator++() {
      EnsurePacket();
      if (!produced_packet_) {
        return *this;  // No packet produced, stay at the same position.
      }

      it_ += payload_size_;

      // Enforce only one LengthSuffixedByteChunkPacket per sequence.
      if (std::holds_alternative<LengthSuffixedByteChunkPacket>(
              *produced_packet_)) {
        length_ = -1;
        force_no_length_ = true;
      }
      ++seq_;
      produced_packet_.reset();
      return *this;
    }

    friend bool operator==(const Iterator& lhs, const Iterator& rhs) {
      const bool lhs_is_end = lhs.it_ == lhs.end_;
      const bool rhs_is_end = rhs.it_ == rhs.end_;
      if (lhs_is_end && rhs_is_end) {
        return true;  // Both iterators are at the end.
      }

      return lhs.it_ == rhs.it_ && lhs.end_ == rhs.end_ &&
             lhs.transient_id_ == rhs.transient_id_ &&
             lhs.packet_size_ == rhs.packet_size_ && lhs.seq_ == rhs.seq_ &&
             lhs.length_ == rhs.length_ &&
             lhs.force_no_length_ == rhs.force_no_length_;
    }

    friend bool operator!=(const Iterator& lhs, const Iterator& rhs) {
      return !(lhs == rhs);
    }

   private:
    void EnsurePacket() {
      if (produced_packet_.has_value()) {
        return;
      }

      if (it_ == end_) {
        return;
      }

      produced_packet_ = ProducePacket(it_, end_, transient_id_, packet_size_,
                                       seq_, length_, force_no_length_);
      if (std::holds_alternative<ByteChunkPacket>(*produced_packet_)) {
        payload_size_ =
            std::get<ByteChunkPacket>(*produced_packet_).chunk.size();
      } else if (std::holds_alternative<LengthSuffixedByteChunkPacket>(
                     *produced_packet_)) {
        payload_size_ =
            std::get<LengthSuffixedByteChunkPacket>(*produced_packet_)
                .chunk.size();
      } else {
        payload_size_ =
            end_ -
            it_;  // For CompleteBytesPacket, the payload size is the remaining size.
      }
    }
    std::vector<Byte>::const_iterator it_;
    std::vector<Byte>::const_iterator end_;
    uint64_t transient_id_;
    uint32_t packet_size_;
    uint32_t seq_ = 0;
    int32_t length_ = -1;  // Length for LengthSuffixedByteChunkPacket.
    bool force_no_length_ =
        false;  // Force no length for LengthSuffixedByteChunkPacket.

    std::optional<BytePacket> produced_packet_;
    uint32_t payload_size_ = 0;
  };

  [[nodiscard]] Iterator begin(uint32_t seq = 0, int32_t length = -1,
                               bool force_no_length = false) const {
    return {data_->cbegin(), data_->cend(),  transient_id_, packet_size_, seq,
            length,          force_no_length};
  }

  [[nodiscard]] Iterator end() const {
    return {data_->cend(), data_->cend(), transient_id_, packet_size_};
  }

 private:
  const std::vector<Byte>* data_;
  uint64_t transient_id_;
  uint32_t packet_size_;
};

absl::StatusOr<BytePacket> ParseBytePacket(std::vector<Byte>&& data);

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

}  // namespace eglt::data

#endif  // EGLT_DATA_BYTE_CHUNKING_H_