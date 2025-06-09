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

#define BOOST_ASIO_NO_DEPRECATED

#include <thread_on_boost/fiber.h>

#include "eglt/net/webrtc/signalling.h"
#include "eglt/net/webrtc/webrtc.h"
#include "eglt/net/websockets/fiber_aware_websocket_stream.h"
#include "eglt/util/boost_asio_utils.h"

// TODO: split this file into multiple files for better organization.

namespace eglt::net {

enum WebRtcEvergreenPacketType {
  kPlainSessionMessage = 0x00,
  kSessionMessageChunk = 0x01,
  kLengthSuffixedSessionMessageChunk = 0x02,
};

struct WebRtcPlainSessionMessage {
  static constexpr uint8_t kType = 0x00;

  std::vector<uint8_t> serialized_message;
  uint64_t transient_id;
};

struct WebRtcSessionMessageChunk {
  static constexpr uint8_t kType = 0x01;

  std::vector<uint8_t> chunk;
  int32_t seq;
  uint64_t transient_id;
};

struct WebRtcLengthSuffixedSessionMessageChunk {
  static constexpr uint8_t kType = 0x02;

  std::vector<uint8_t> chunk;
  int32_t length;
  int32_t seq;
  uint64_t transient_id;
};

using WebRtcEvergreenPacket =
    std::variant<WebRtcPlainSessionMessage, WebRtcSessionMessageChunk,
                 WebRtcLengthSuffixedSessionMessageChunk>;

inline absl::StatusOr<WebRtcEvergreenPacket> ParseWebRtcEvergreenPacket(
    std::vector<uint8_t>&& data) {
  if (data.size() < sizeof(int64_t) + 1) {
    return absl::InvalidArgumentError(
        "WebRtcEvergreenPacket data size is less than 9 bytes. No valid packet "
        "is less than 9 bytes.");
  }

  size_t remaining_data_size = data.size();

  const uint8_t type = *data.rbegin();
  if (type != WebRtcPlainSessionMessage::kType &&
      type != WebRtcSessionMessageChunk::kType &&
      type != WebRtcLengthSuffixedSessionMessageChunk::kType) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "WebRtcEvergreenPacket type %d is not supported", type));
  }

  data.pop_back();
  remaining_data_size -= sizeof(uint8_t);

  const uint64_t transient_id = *reinterpret_cast<uint64_t*>(
      data.data() + remaining_data_size - sizeof(int64_t));
  data.erase(data.end() - sizeof(uint64_t), data.end());
  remaining_data_size -= sizeof(uint64_t);

  // Plain SessionMessage
  if (type == WebRtcEvergreenPacketType::kPlainSessionMessage) {
    return WebRtcPlainSessionMessage{
        .serialized_message = std::vector(data.begin(), data.end()),
        .transient_id = transient_id};
  }

  const int32_t seq = *reinterpret_cast<int32_t*>(
      data.data() + remaining_data_size - sizeof(int32_t));
  data.erase(data.end() - sizeof(int32_t), data.end());
  remaining_data_size -= sizeof(int32_t);

  // SessionMessage Chunk
  if (type == WebRtcEvergreenPacketType::kSessionMessageChunk) {
    std::vector chunk(data.begin(), data.end());
    return WebRtcSessionMessageChunk{
        .chunk = std::move(chunk), .seq = seq, .transient_id = transient_id};
  }

  // Length Suffix SessionMessage Chunk
  if (type == WebRtcEvergreenPacketType::kLengthSuffixedSessionMessageChunk) {
    if (remaining_data_size < sizeof(int32_t)) {
      return absl::InvalidArgumentError(
          "Invalid WebRtcEvergreenPacket: marked as "
          "LengthSuffixedSessionMessageChunk but data size is less than 13");
    }

    const int32_t length = *reinterpret_cast<int32_t*>(
        data.data() + remaining_data_size - sizeof(int32_t));
    data.erase(data.end() - sizeof(int32_t), data.end());
    remaining_data_size -= sizeof(int32_t);

    std::vector chunk(data.begin(), data.end());
    return WebRtcLengthSuffixedSessionMessageChunk{
        .chunk = std::move(chunk),
        .length = length,
        .seq = seq,
        .transient_id = transient_id};
  }

  // If we reach here, it means the type is not recognized.
  return absl::InvalidArgumentError(
      "WebRtcEvergreenPacket type is not supported");
}

inline std::vector<WebRtcEvergreenPacket> SplitDataIntoWebRtcPackets(
    const std::vector<uint8_t>& data, uint64_t transient_id,
    int64_t packet_size = 16384) {
  std::vector<WebRtcEvergreenPacket> packets;

  if (data.size() <= packet_size - sizeof(int64_t) - 1) {
    // If the data fits into a single packet, create a plain session message.
    packets.emplace_back(WebRtcPlainSessionMessage{
        .serialized_message = data, .transient_id = transient_id});
    return packets;
  }

  packets.reserve((data.size() + packet_size - 1) / packet_size);

  if (packet_size < 18) {
    LOG(FATAL) << "Packet size must be at least 18 bytes to accommodate the "
                  "header of 17 bytes.";
    ABSL_ASSUME(false);
  }

  // If the data is larger than the packet size, split it into chunks.
  const int64_t first_chunk_size = packet_size - 17;
  WebRtcLengthSuffixedSessionMessageChunk first_chunk{
      .chunk = std::vector(data.begin(), data.begin() + first_chunk_size),
      .length = 0,  // This will be set later.
      .seq = 0,
      .transient_id = transient_id};
  packets.emplace_back(std::move(first_chunk));

  int32_t seq = 1;
  int64_t offset = first_chunk_size;
  while (offset < data.size()) {
    int64_t remaining_size = static_cast<int64_t>(data.size()) - offset;
    const int64_t chunk_size = std::min(packet_size - 13, remaining_size);
    WebRtcSessionMessageChunk chunk{
        .chunk = std::vector(data.begin() + offset,
                             data.begin() + offset + chunk_size),
        .seq = seq,
        .transient_id = transient_id};

    packets.emplace_back(std::move(chunk));
    offset += chunk_size;
    ++seq;
  }

  std::get<WebRtcLengthSuffixedSessionMessageChunk>(packets[0]).length =
      static_cast<int32_t>(packets.size());

  return packets;
}

inline uint64_t GetTransientIdFromPacket(const WebRtcEvergreenPacket& packet) {
  if (std::holds_alternative<WebRtcPlainSessionMessage>(packet)) {
    return std::get<WebRtcPlainSessionMessage>(packet).transient_id;
  }
  if (std::holds_alternative<WebRtcSessionMessageChunk>(packet)) {
    return std::get<WebRtcSessionMessageChunk>(packet).transient_id;
  }
  if (std::holds_alternative<WebRtcLengthSuffixedSessionMessageChunk>(packet)) {
    return std::get<WebRtcLengthSuffixedSessionMessageChunk>(packet)
        .transient_id;
  }
  return 0;  // Default value if no transient ID is found.
}

template <typename T>
absl::InlinedVector<uint8_t, 8> NumberToBytesBE(T number) {
  absl::InlinedVector<uint8_t, 8> bytes;
  bytes.reserve(sizeof(T));
  for (int i = sizeof(T) - 1; i >= 0; --i) {
    bytes.push_back(static_cast<uint8_t>((number >> (i * 8)) & 0xFF));
  }
  return bytes;
}

template <typename T>
absl::InlinedVector<uint8_t, 8> NumberToBytesLE(T number) {
  absl::InlinedVector<uint8_t, 8> bytes;
  bytes.reserve(sizeof(T));
  for (int i = 0; i < sizeof(T); ++i) {
    bytes.push_back(static_cast<uint8_t>((number >> (i * 8)) & 0xFF));
  }
  return bytes;
}

inline std::vector<uint8_t> SerializeWebRtcPacket(
    WebRtcEvergreenPacket packet) {
  std::vector<uint8_t> bytes;
  uint64_t transient_id = 0;
  uint8_t type_byte = 0;

  if (std::holds_alternative<WebRtcPlainSessionMessage>(packet)) {
    auto [serialized_message, id] =
        std::move(std::get<WebRtcPlainSessionMessage>(packet));
    bytes = std::move(serialized_message);
    transient_id = id;
    type_byte = WebRtcPlainSessionMessage::kType;
  }

  if (std::holds_alternative<WebRtcSessionMessageChunk>(packet)) {
    auto [chunk, seq, id] =
        std::move(std::get<WebRtcSessionMessageChunk>(packet));
    bytes = std::move(chunk);
    transient_id = id;
    type_byte = WebRtcSessionMessageChunk::kType;

    auto seq_bytes = NumberToBytesLE(seq);
    bytes.insert(bytes.end(), seq_bytes.begin(), seq_bytes.end());
  }

  if (std::holds_alternative<WebRtcLengthSuffixedSessionMessageChunk>(packet)) {
    auto [chunk, length, seq, id] =
        std::move(std::get<WebRtcLengthSuffixedSessionMessageChunk>(packet));
    bytes = std::move(chunk);
    transient_id = id;
    type_byte = WebRtcLengthSuffixedSessionMessageChunk::kType;

    auto length_bytes = NumberToBytesLE(length);
    bytes.insert(bytes.end(), length_bytes.begin(), length_bytes.end());

    auto seq_bytes = NumberToBytesLE(seq);
    bytes.insert(bytes.end(), seq_bytes.begin(), seq_bytes.end());
  }

  auto transient_id_bytes = NumberToBytesLE(transient_id);
  bytes.insert(bytes.end(), transient_id_bytes.begin(),
               transient_id_bytes.end());

  bytes.push_back(type_byte);

  return bytes;
}

class ChunkedWebRtcMessage {
 public:
  absl::StatusOr<std::vector<uint8_t>> Consume() {
    if (chunk_store_.Size() < total_expected_chunks_) {
      return absl::FailedPreconditionError(
          "Cannot consume message, not all chunks received yet");
    }

    std::vector<uint8_t> message_data;
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

  absl::StatusOr<bool> FeedPacket(WebRtcEvergreenPacket packet) {
    if (chunk_store_.Size() >= total_expected_chunks_ &&
        total_expected_chunks_ != -1) {
      return absl::FailedPreconditionError(
          "Cannot feed more packets, already received all expected chunks");
    }

    if (std::holds_alternative<WebRtcPlainSessionMessage>(packet)) {
      auto& serialized_message =
          std::get<WebRtcPlainSessionMessage>(packet).serialized_message;
      Chunk data_chunk{.metadata = {},
                       .data = std::string(
                           std::make_move_iterator(serialized_message.begin()),
                           std::make_move_iterator(serialized_message.end()))};
      total_message_size_ += data_chunk.data.size();
      total_expected_chunks_ = 1;  // This is a single message, not chunked.
      chunk_store_
          .Put(0, std::move(data_chunk),
               /*final=*/true)
          .IgnoreError();
      return true;
    }

    if (std::holds_alternative<WebRtcSessionMessageChunk>(packet)) {
      auto& chunk = std::get<WebRtcSessionMessageChunk>(packet);
      Chunk data_chunk{
          .metadata = {},
          .data = std::string(std::make_move_iterator(chunk.chunk.begin()),
                              std::make_move_iterator(chunk.chunk.end()))};
      total_message_size_ += data_chunk.data.size();
      chunk_store_
          .Put(chunk.seq, std::move(data_chunk),
               /*final=*/
               chunk.seq == total_expected_chunks_ - 1)
          .IgnoreError();
      return chunk_store_.Size() == total_expected_chunks_;
    }

    if (std::holds_alternative<WebRtcLengthSuffixedSessionMessageChunk>(
            packet)) {
      auto& chunk = std::get<WebRtcLengthSuffixedSessionMessageChunk>(packet);
      if (total_expected_chunks_ != -1) {
        return absl::InvalidArgumentError(
            "Cannot have more than one WebRtcLengthSuffixedSessionMessageChunk "
            "in a sequence");
      }
      total_message_size_ += chunk.chunk.size();
      total_expected_chunks_ =
          chunk.length;  // Set the total expected chunks from this packet.
      chunk_store_
          .Put(chunk.seq,
               Chunk{.metadata = {},
                     .data = std::string(
                         std::make_move_iterator(chunk.chunk.begin()),
                         std::make_move_iterator(chunk.chunk.end()))},
               /*final=*/
               chunk.seq == total_expected_chunks_ - 1)
          .IgnoreError();
      return chunk_store_.Size() == total_expected_chunks_;
    }

    return absl::InvalidArgumentError("Unknown WebRtcEvergreenPacket type");
  }

  absl::StatusOr<bool> FeedRawPacket(std::vector<uint8_t> data) {
    if (chunk_store_.Size() >= total_expected_chunks_ &&
        total_expected_chunks_ != -1) {
      return absl::FailedPreconditionError(
          "Cannot feed more packets, already received all expected chunks");
    }

    absl::StatusOr<WebRtcEvergreenPacket> packet =
        ParseWebRtcEvergreenPacket(std::move(data));
    if (!packet.ok()) {
      return packet.status();
    }

    return FeedPacket(*std::move(packet));
  }

 private:
  LocalChunkStore chunk_store_;
  size_t total_message_size_ = 0;
  int32_t total_expected_chunks_ = -1;
};

WebRtcWireStream::WebRtcWireStream(
    std::shared_ptr<rtc::DataChannel> data_channel,
    std::shared_ptr<rtc::PeerConnection> connection)
    : id_(data_channel->label()),
      connection_(std::move(connection)),
      data_channel_(std::move(data_channel)) {

  data_channel_->onMessage(
      [this](rtc::binary message) {
        const auto data = reinterpret_cast<uint8_t*>(message.data());
        absl::StatusOr<WebRtcEvergreenPacket> packet =
            ParseWebRtcEvergreenPacket(
                std::vector(data, data + message.size()));

        concurrency::MutexLock lock(&mutex_);

        if (closed_) {
          return;
        }

        if (!packet.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream unpack failed: %s",
                              packet.status().message())));
          return;
        }

        uint64_t transient_id = GetTransientIdFromPacket(*packet);
        auto& chunked_message = chunked_messages_[transient_id];
        if (!chunked_message) {
          chunked_message = std::make_unique<ChunkedWebRtcMessage>();
        }
        auto got_full_message = chunked_message->FeedPacket(*std::move(packet));
        if (!got_full_message.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream chunked message "
                              "feed failed: %s",
                              got_full_message.status().message())));
          return;
        }

        if (!*got_full_message) {
          return;  // Not all chunks received yet, wait for more.
        }

        absl::StatusOr<std::vector<uint8_t>> message_data =
            chunked_message->Consume();
        if (!message_data.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream consume failed: %s",
                              message_data.status().message())));
          return;
        }

        mutex_.Unlock();
        absl::StatusOr<SessionMessage> unpacked =
            cppack::Unpack<SessionMessage>(
                std::vector(*std::move(message_data)));
        mutex_.Lock();

        if (!unpacked.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream unpack failed: %s",
                              unpacked.status().message())));
          return;
        }

        recv_channel_.writer()->WriteUnlessCancelled(*std::move(unpacked));
        chunked_messages_.erase(transient_id);
      },
      [](const rtc::string&) {});

  if (data_channel_ && data_channel_->isOpen()) {
    opened_ = true;
  } else {
    data_channel_->onOpen([this]() {
      concurrency::MutexLock lock(&mutex_);
      status_ = absl::OkStatus();
      opened_ = true;
      cv_.SignalAll();
    });
  }

  data_channel_->onClosed([this]() {
    concurrency::MutexLock lock(&mutex_);
    closed_ = true;
    status_ = absl::CancelledError("WebRtcWireStream closed");
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  });

  data_channel_->onError([this](const std::string& error) {
    concurrency::MutexLock lock(&mutex_);
    closed_ = true;
    status_ = absl::InternalError(
        absl::StrFormat("WebRtcWireStream error: %s", error));
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  });
}

WebRtcWireStream::~WebRtcWireStream() {
  data_channel_->close();
  concurrency::MutexLock lock(&mutex_);
  while (!closed_) {
    cv_.Wait(&mutex_);
  }
  connection_->close();
}

absl::Status WebRtcWireStream::Send(SessionMessage message) {
  uint64_t transient_id = 0;
  {
    concurrency::MutexLock lock(&mutex_);
    if (!status_.ok()) {
      return status_;
    }

    while (!opened_ && !closed_) {
      cv_.Wait(&mutex_);
    }

    if (closed_) {
      return absl::CancelledError("WebRtcWireStream is closed");
    }
    transient_id = next_transient_id_++;
  }

  std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));

  std::vector<WebRtcEvergreenPacket> packets = SplitDataIntoWebRtcPackets(
      message_uint8_t, transient_id,
      static_cast<int64_t>(connection_->remoteMaxMessageSize()));

  absl::Status status;
  for (const auto& packet : packets) {
    std::vector<uint8_t> serialized_packet = SerializeWebRtcPacket(packet);
    const rtc::byte* message_chunk_data =
        reinterpret_cast<rtc::byte*>(serialized_packet.data());
    rtc::binary message_chunk_bytes(
        message_chunk_data, message_chunk_data + serialized_packet.size());
    data_channel_->send(std::move(message_chunk_bytes));
  }
  return status;
}

static constexpr int kMaxMessageSize = 16384;  // 16 KiB
static constexpr uint16_t kDefaultRtcPort = 19002;
static constexpr auto kDefaultStunServer = "stun.l.google.com:19302";

static rtc::Configuration GetDefaultRtcConfig() {
  rtc::Configuration config;
  config.maxMessageSize = kMaxMessageSize;
  config.portRangeBegin = kDefaultRtcPort;
  config.portRangeEnd = kDefaultRtcPort;
  config.iceServers.emplace_back(kDefaultStunServer);
  return config;
}

absl::StatusOr<WebRtcDataChannelConnection> AcceptWebRtcDataChannel(
    std::string_view identity, std::string_view signalling_address,
    uint16_t signalling_port) {
  SignallingClient signalling_client{signalling_address, signalling_port};

  std::string client_id;

  auto config = GetDefaultRtcConfig();
  config.enableIceUdpMux = true;
  auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

  std::shared_ptr<rtc::DataChannel> data_channel;
  concurrency::PermanentEvent data_channel_event;

  signalling_client.OnOffer(
      [&connection, &client_id](std::string_view id,
                                const boost::json::value& message) {
        if (!client_id.empty() && client_id != id) {
          LOG(ERROR) << "Already accepting another client: " << client_id;
          return;
        }
        client_id = std::string(id);

        boost::system::error_code error;

        if (const auto desc_ptr = message.find_pointer("/description", error);
            desc_ptr != nullptr && !error) {
          const auto description = desc_ptr->as_string().c_str();
          connection->setRemoteDescription(rtc::Description(description));
        } else {
          LOG(ERROR) << "WebRtcWireStream no 'description' field in "
                        "offer: "
                     << boost::json::serialize(message);
        }
      });

  signalling_client.OnCandidate([&connection, &client_id](
                                    std::string_view id,
                                    const boost::json::value& message) {
    if (!client_id.empty() || client_id != id) {
      return;
    }

    boost::system::error_code error;

    if (const auto candidate_ptr = message.find_pointer("/candidate", error);
        candidate_ptr != nullptr && !error) {
      const auto candidate_str = candidate_ptr->as_string().c_str();
      connection->addRemoteCandidate(rtc::Candidate(candidate_str));
    } else {
      LOG(ERROR) << "WebRtcWireStream no 'candidate' field in "
                    "candidate message: "
                 << boost::json::serialize(message);
    }
  });

  if (auto status = signalling_client.ConnectWithIdentity(identity);
      !status.ok()) {
    return status;
  }

  connection->onLocalDescription(
      [&signalling_client, &client_id](const rtc::Description& description) {
        const std::string sdp = description.generateSdp("\r\n");

        boost::json::object answer;
        answer["id"] = client_id;
        answer["type"] = "answer";
        answer["description"] = sdp;

        const auto message = boost::json::serialize(answer);
        if (const auto status = signalling_client.Send(message); !status.ok()) {
          LOG(ERROR) << "WebRtcWireStream Send answer failed: " << status;
        }
      });

  connection->onLocalCandidate(
      [&signalling_client, &client_id](const rtc::Candidate& candidate) {
        const auto candidate_str = std::string(candidate);

        boost::json::object candidate_json;
        candidate_json["id"] = client_id;
        candidate_json["type"] = "candidate";
        candidate_json["candidate"] = candidate_str;
        candidate_json["mid"] = candidate.mid();

        const auto message = boost::json::serialize(candidate_json);
        if (const auto status = signalling_client.Send(message); !status.ok()) {
          LOG(ERROR) << "WebRtcWireStream Send candidate failed: " << status;
        }
      });

  connection->onDataChannel(
      [&data_channel,
       &data_channel_event](const std::shared_ptr<rtc::DataChannel>& channel) {
        data_channel = channel;
        data_channel_event.Notify();
      });

  const int selected = concurrency::Select({data_channel_event.OnEvent(),
                                            signalling_client.OnError(),
                                            concurrency::OnCancel()});

  // Callbacks need to be cleaned up before returning, because they use
  // local variables that will be destroyed when this function returns.
  connection->onLocalCandidate({});
  connection->onLocalDescription({});
  connection->onDataChannel({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (selected == 1) {
    return signalling_client.GetStatus();
  }
  if (concurrency::Cancelled()) {
    return absl::CancelledError("WebRtcWireStream connection cancelled");
  }

  return WebRtcDataChannelConnection{
      .data_channel = std::move(data_channel),
      .connection = std::move(connection),
  };
}

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity,
    std::string_view signalling_address, uint16_t signalling_port) {
  SignallingClient signalling_client{signalling_address, signalling_port};

  rtc::Configuration config = GetDefaultRtcConfig();
  config.portRangeBegin = 1025;
  config.portRangeEnd = 65535;

  auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

  signalling_client.OnAnswer(
      [&connection, peer_identity = std::string(peer_identity)](
          std::string_view received_peer_id,
          const boost::json::value& message) {
        if (received_peer_id != peer_identity) {
          return;
        }

        boost::system::error_code error;
        if (const auto desc_ptr = message.find_pointer("/description", error);
            desc_ptr != nullptr && !error) {
          const auto description = desc_ptr->as_string().c_str();
          connection->setRemoteDescription(rtc::Description(description));
        } else {
          LOG(ERROR) << "WebRtcWireStream no 'description' field in "
                        "answer: "
                     << boost::json::serialize(message);
        }
      });

  signalling_client.OnCandidate([&connection,
                                 peer_identity = std::string(peer_identity)](
                                    std::string_view received_peer_id,
                                    const boost::json::value& message) {
    if (received_peer_id != peer_identity) {
      return;
    }

    boost::system::error_code error;

    if (const auto candidate_ptr = message.find_pointer("/candidate", error);
        candidate_ptr != nullptr && !error) {
      const auto candidate_str = candidate_ptr->as_string().c_str();
      connection->addRemoteCandidate(rtc::Candidate(candidate_str));
    } else {
      LOG(ERROR) << "WebRtcWireStream no 'candidate' field in "
                    "candidate message: "
                 << boost::json::serialize(message);
    }
  });

  if (auto status = signalling_client.ConnectWithIdentity(identity);
      !status.ok()) {
    return status;
  }

  connection->onLocalCandidate(
      [peer_id = std::string(peer_identity),
       &signalling_client](const rtc::Candidate& candidate) {
        const auto candidate_str = std::string(candidate);
        boost::json::object candidate_json;
        candidate_json["id"] = peer_id;
        candidate_json["type"] = "candidate";
        candidate_json["candidate"] = candidate_str;
        candidate_json["mid"] = candidate.mid();

        const auto message = boost::json::serialize(candidate_json);
        if (const auto status = signalling_client.Send(message); !status.ok()) {
          LOG(ERROR) << "WebRtcWireStream Send candidate failed: " << status;
        }
      });

  auto init = rtc::DataChannelInit{};
  init.reliability.unordered = true;
  auto data_channel =
      connection->createDataChannel(std::string(identity), std::move(init));

  concurrency::PermanentEvent opened;
  data_channel->onOpen([&opened]() { opened.Notify(); });

  // Send connection offer to the server.
  {
    auto description = connection->createOffer();
    auto sdp = description.generateSdp("\r\n");

    boost::json::object offer;
    offer["id"] = peer_identity;
    offer["type"] = "offer";
    offer["description"] = sdp;
    const auto message = boost::json::serialize(offer);
    if (auto status = signalling_client.Send(message); !status.ok()) {
      LOG(ERROR) << "WebRtcWireStream Send offer failed: " << status;
      return status;
    }
  }

  const int selected = concurrency::Select(
      {opened.OnEvent(), signalling_client.OnError(), concurrency::OnCancel()});
  if (selected == 1) {
    return signalling_client.GetStatus();
  }
  if (concurrency::Cancelled()) {
    return absl::CancelledError("WebRtcWireStream connection cancelled");
  }

  data_channel->onOpen({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (concurrency::Cancelled()) {
    return absl::CancelledError("WebRtcWireStream connection cancelled");
  }

  return WebRtcDataChannelConnection{
      .data_channel = std::move(data_channel),
      .connection = std::move(connection),
  };
}

WebRtcEvergreenServer::WebRtcEvergreenServer(
    eglt::Service* absl_nonnull service, std::string_view address,
    uint16_t port, std::string_view signalling_address,
    uint16_t signalling_port, std::string_view signalling_identity)
    : service_(service),
      address_(address),
      port_(port),
      signalling_address_(signalling_address),
      signalling_port_(signalling_port),
      signalling_identity_(signalling_identity),
      ready_data_connections_(32) {}

WebRtcEvergreenServer::~WebRtcEvergreenServer() {
  concurrency::MutexLock lock(&mutex_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
}

void WebRtcEvergreenServer::Run() {
  concurrency::MutexLock lock(&mutex_);
  main_loop_ = concurrency::NewTree({}, [this]() { RunLoop(); });
}

absl::Status WebRtcEvergreenServer::CancelInternal()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcEvergreenServer Cancel called on either unstarted or already "
        "cancelled server.");
  }
  ready_data_connections_.writer()->Close();
  main_loop_->Cancel();
  return absl::OkStatus();
}

absl::Status WebRtcEvergreenServer::JoinInternal()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcEvergreenServer Join called on either unstarted or already "
        "joined server.");
  }

  const std::unique_ptr<concurrency::Fiber> main_loop = std::move(main_loop_);
  main_loop_ = nullptr;

  mutex_.Unlock();
  main_loop->Join();
  mutex_.Lock();

  return absl::OkStatus();
}

void WebRtcEvergreenServer::RunLoop() {
  DLOG(INFO) << "WebRtcEvergreenServer RunLoop starting.";
  DataChannelConnectionMap connections;
  auto signalling_client =
      InitSignallingClient(signalling_address_, signalling_port_, &connections);

  concurrency::MutexLock lock(&mutex_);
  if (const auto status =
          signalling_client->ConnectWithIdentity(signalling_identity_);
      !status.ok()) {
    LOG(ERROR)
        << "WebRtcEvergreenServer failed to connect to signalling server: "
        << status;
    return;
  }
  DLOG(INFO) << "WebRtcEvergreenServer RunLoop connected to signalling server.";
  const auto channel_reader = ready_data_connections_.reader();

  int retries_remaining = 5000000;

  while (true) {
    WebRtcDataChannelConnection next_connection;
    bool channel_open;

    DLOG(INFO) << "WebRtcEvergreenServer RunLoop waiting for new "
                  "connections.";
    mutex_.Unlock();
    const int selected = concurrency::Select(
        {channel_reader->OnRead(&next_connection, &channel_open),
         signalling_client->OnError(), concurrency::OnCancel()});
    mutex_.Lock();

    // Check if our fiber has been cancelled, which means we should stop.
    if (concurrency::Cancelled()) {
      LOG(INFO) << "WebRtcEvergreenServer RunLoop cancelled.";
      break;
    }

    // If the signalling client has an error, we need to restart it while
    // we still have retries left.
    if (selected == 1) {
      if (retries_remaining <= 0) {
        LOG(ERROR) << "WebRtcEvergreenServer signalling client error: "
                   << signalling_client->GetStatus()
                   << ". No more retries left. Exiting.";
        break;
      }
      LOG(ERROR) << "WebRtcEvergreenServer signalling client error: "
                 << signalling_client->GetStatus()
                 << ". Restarting in 0.5 seconds.";
      mutex_.Unlock();
      concurrency::SleepFor(absl::Seconds(0.5));
      mutex_.Lock();
      signalling_client = InitSignallingClient(signalling_address_,
                                               signalling_port_, &connections);
      if (const auto status =
              signalling_client->ConnectWithIdentity(signalling_identity_);
          !status.ok()) {
        LOG(ERROR) << "WebRtcEvergreenServer failed to reconnect to "
                      "signalling server: "
                   << status;
        return;
      }
      --retries_remaining;
      continue;
    }

    // This happens when the channel is closed for writing and does not have
    // any more data to read.
    if (!channel_open) {
      LOG(INFO) << "WebRtcEvergreenServer RunLoop was cancelled by externally "
                   "closing the channel for new connections.";
    }

    auto stream = std::make_unique<WebRtcWireStream>(
        std::move(next_connection.data_channel),
        std::move(next_connection.connection));

    if (auto service_connection =
            service_->EstablishConnection(std::move(stream));
        !service_connection.ok()) {
      LOG(ERROR) << "WebRtcEvergreenServer EstablishConnection failed: "
                 << service_connection.status();
      continue;
    }
    // At this point, the connection is established and the responsibility
    // of the WebRtcEvergreenServer is done. The service will handle the
    // connection from here on out.
  }
  signalling_client->Cancel();
  signalling_client->Join();
}

std::shared_ptr<SignallingClient> WebRtcEvergreenServer::InitSignallingClient(
    std::string_view signalling_address, uint16_t signalling_port,
    DataChannelConnectionMap* absl_nonnull connections) {
  auto signalling_client =
      std::make_shared<SignallingClient>(signalling_address, signalling_port);

  DLOG(INFO) << "WebRtcEvergreenServer InitSignallingClient "
                "connecting to signalling server at "
             << signalling_address << ":" << signalling_port;

  signalling_client->OnOffer([this, connections, signalling_client](
                                 std::string_view peer_id,
                                 const boost::json::value& message) {
    if (connections->contains(std::string(peer_id))) {
      LOG(ERROR) << "WebRtcEvergreenServer already accepting a connection from "
                    "peer: "
                 << peer_id;
      return;
    }

    boost::system::error_code error;

    std::string description;
    if (const auto desc_ptr = message.find_pointer("/description", error);
        desc_ptr == nullptr || error) {
      LOG(ERROR) << "WebRtcEvergreenServer no 'description' field in offer: "
                 << boost::json::serialize(message);
      return;
    } else {
      description = desc_ptr->as_string().c_str();
    }

    rtc::Configuration config = GetDefaultRtcConfig();
    config.enableIceUdpMux = true;
    config.bindAddress = address_;
    config.portRangeBegin = port_;
    config.portRangeEnd = port_;

    auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

    connection->onLocalDescription([this, peer_id = std::string(peer_id),
                                    connections, signalling_client](
                                       const rtc::Description& description) {
      const std::string sdp = description.generateSdp("\r\n");

      boost::json::object answer;
      answer["id"] = peer_id;
      answer["type"] = "answer";
      answer["description"] = sdp;

      const auto message = boost::json::serialize(answer);
      if (const auto status = signalling_client->Send(message); !status.ok()) {
        LOG(ERROR) << "WebRtcEvergreenServer Send answer failed: " << status;
      }
    });
    connection->onLocalCandidate([this, peer_id = std::string(peer_id),
                                  connections, signalling_client](
                                     const rtc::Candidate& candidate) {
      boost::json::object candidate_json;
      candidate_json["id"] = peer_id;
      candidate_json["type"] = "candidate";
      candidate_json["candidate"] = std::string(candidate);
      candidate_json["mid"] = candidate.mid();

      const auto message = boost::json::serialize(candidate_json);
      if (const auto status = signalling_client->Send(message); !status.ok()) {
        LOG(ERROR) << "WebRtcEvergreenServer Send candidate failed: " << status;
      }
    });
    connection->onDataChannel(
        [this, peer_id = std::string(peer_id),
         connections](std::shared_ptr<rtc::DataChannel> dc) {
          concurrency::MutexLock lock(&mutex_);
          const auto map_node = connections->extract(peer_id);
          CHECK(!map_node.empty())
              << "WebRtcEvergreenServer no connection for peer: " << peer_id;

          WebRtcDataChannelConnection connection_from_map =
              std::move(map_node.mapped());
          connection_from_map.data_channel = std::move(dc);

          ready_data_connections_.writer()->WriteUnlessCancelled(
              std::move(connection_from_map));
        });

    concurrency::MutexLock lock(&mutex_);
    connections->emplace(peer_id, WebRtcDataChannelConnection{
                                      .connection = std::move(connection),
                                      .data_channel = nullptr});

    FindOrDie(*connections, peer_id)
        .connection->setRemoteDescription(rtc::Description(description));
  });

  signalling_client->OnCandidate([this, connections](
                                     std::string_view peer_id,
                                     const boost::json::value& message) {
    if (!connections->contains(peer_id)) {
      return;
    }

    boost::system::error_code error;

    std::string candidate;
    if (const auto candidate_ptr = message.find_pointer("/candidate", error);
        candidate_ptr == nullptr || error) {
      LOG(ERROR) << "WebRtcEvergreenServer no 'candidate' field in "
                    "candidate message: "
                 << boost::json::serialize(message);
      return;
    } else {
      candidate = candidate_ptr->as_string().c_str();
    }

    FindOrDie(*connections, peer_id)
        .connection->addRemoteCandidate(rtc::Candidate(candidate));
  });

  return signalling_client;
}

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> AcceptStreamFromSignalling(
    std::string_view identity, std::string_view address, uint16_t port) {

  absl::StatusOr<WebRtcDataChannelConnection> connection =
      AcceptWebRtcDataChannel(identity, address, port);
  if (!connection.ok()) {
    return connection.status();
  }

  return std::make_unique<WebRtcWireStream>(std::move(connection->data_channel),
                                            std::move(connection->connection));
}

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity, std::string_view peer_identity,
    std::string_view address, uint16_t port) {

  absl::StatusOr<WebRtcDataChannelConnection> connection =
      StartWebRtcDataChannel(identity, peer_identity, address, port);
  if (!connection.ok()) {
    return connection.status();
  }

  return std::make_unique<WebRtcWireStream>(std::move(connection->data_channel),
                                            std::move(connection->connection));
}

}  // namespace eglt::net