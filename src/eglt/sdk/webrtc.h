#ifndef EGLT_SDK_WEBRTC_H_
#define EGLT_SDK_WEBRTC_H_

#include <rtc/rtc.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"

namespace eglt::sdk {

enum WebRtcEvergreenPacketType {
  kPlainSessionMessage = 0x00,
  kSessionMessageChunk = 0x01,
  kLengthSuffixedSessionMessageChunk = 0x02,
};

struct WebRtcPlainSessionMessage {
  static constexpr uint8_t kType = 0x00;

  SessionMessage message;
  int64_t transient_id;
};

struct WebRtcSessionMessageChunk {
  static constexpr uint8_t kType = 0x01;

  std::vector<uint8_t> chunk;
  int64_t transient_id;
};

struct WebRtcLengthSuffixedSessionMessageChunk {
  static constexpr uint8_t kType = 0x02;

  std::vector<uint8_t> chunk;
  int32_t length;
  int64_t transient_id;
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

  const int64_t transient_id = *reinterpret_cast<int64_t*>(
      data.data() + remaining_data_size - sizeof(int64_t));
  data.erase(data.end() - sizeof(int64_t), data.end());
  remaining_data_size -= sizeof(int64_t);

  // Plain SessionMessage
  if (type == WebRtcEvergreenPacketType::kPlainSessionMessage) {
    if (remaining_data_size < sizeof(SessionMessage)) {
      return absl::InvalidArgumentError(
          "WebRtcEvergreenPacket data size is less than SessionMessage size.");
    }
    absl::StatusOr<SessionMessage> session_message =
        cppack::Unpack<SessionMessage>(data);
    if (!session_message.ok()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("WebRtcEvergreenPacket unpack failed: %s",
                          session_message.status().message()));
    }

    return WebRtcPlainSessionMessage{.message = *std::move(session_message),
                                     .transient_id = transient_id};
  }

  // SessionMessage Chunk
  if (type == WebRtcEvergreenPacketType::kSessionMessageChunk) {
    if (remaining_data_size == 0) {
      return WebRtcSessionMessageChunk{.chunk = std::vector<uint8_t>{},
                                       .transient_id = transient_id};
    }
    std::vector chunk(data.begin(), data.end());
    return WebRtcSessionMessageChunk{.chunk = std::move(chunk),
                                     .transient_id = transient_id};
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
        .transient_id = transient_id};
  }

  // If we reach here, it means the type is not recognized.
  return absl::InvalidArgumentError(
      "WebRtcEvergreenPacket type is not supported");
}

struct WebRtcDataChannelConnection {
  std::shared_ptr<rtc::PeerConnection> connection;
  std::shared_ptr<rtc::DataChannel> data_channel;
};

absl::StatusOr<WebRtcDataChannelConnection> AcceptWebRtcDataChannel(
    std::string_view identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80);

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80);

class WebRtcEvergreenWireStream final : public EvergreenWireStream {
 public:
  static constexpr int kBufferSize = 256;

  explicit WebRtcEvergreenWireStream(
      std::shared_ptr<rtc::DataChannel> data_channel,
      std::shared_ptr<rtc::PeerConnection> connection = nullptr)
      : id_(data_channel->label()),
        connection_(std::move(connection)),
        data_channel_(std::move(data_channel)) {

    data_channel_->onMessage(
        [this](rtc::binary message) {
          const auto data = reinterpret_cast<uint8_t*>(message.data());
          absl::StatusOr<SessionMessage> unpacked =
              cppack::Unpack<SessionMessage>(
                  std::vector(data, data + message.size()));

          concurrency::MutexLock lock(&mutex_);

          if (closed_) {
            return;
          }

          if (!unpacked.ok()) {
            recv_channel_.writer()->Close();
            closed_ = true;
            status_ = absl::InternalError(
                absl::StrFormat("WebRtcEvergreenWireStream unpack failed: %s",
                                unpacked.status().message()));
            return;
          }

          recv_channel_.writer()->WriteUnlessCancelled(*std::move(unpacked));
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
      status_ = absl::CancelledError("WebRtcEvergreenWireStream closed");
      recv_channel_.writer()->Close();
      cv_.SignalAll();
    });

    data_channel_->onError([this](const std::string& error) {
      concurrency::MutexLock lock(&mutex_);
      closed_ = true;
      status_ = absl::InternalError(
          absl::StrFormat("WebRtcEvergreenWireStream error: %s", error));
      recv_channel_.writer()->Close();
      cv_.SignalAll();
    });
  }

  ~WebRtcEvergreenWireStream() override {
    data_channel_->close();
    concurrency::MutexLock lock(&mutex_);
    while (!closed_) {
      cv_.Wait(&mutex_);
    }
    connection_->close();
  }

  absl::Status Send(SessionMessage message) override {
    concurrency::MutexLock lock(&mutex_);
    if (!status_.ok()) {
      return status_;
    }

    while (!opened_ && !closed_) {
      cv_.Wait(&mutex_);
    }

    if (closed_) {
      return absl::CancelledError("WebRtcEvergreenWireStream is closed");
    }

    std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));
    const rtc::byte* message_data =
        reinterpret_cast<rtc::byte*>(message_uint8_t.data());
    rtc::binary message_bytes(message_data,
                              message_data + message_uint8_t.size());

    data_channel_->send(message_bytes);
    return absl::OkStatus();
  }

  std::optional<SessionMessage> Receive() override {
    SessionMessage message;
    if (!recv_channel_.reader()->Read(&message)) {
      return std::nullopt;
    }
    return std::move(message);
  }

  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status Accept() override { return absl::OkStatus(); }

  void HalfClose() override {
    data_channel_->close();
    // concurrency::MutexLock lock(&mutex_);
    // if (closed_) {
    //   DLOG(INFO) << "WebRtcEvergreenWireStream HalfClose: already closed";
    //   return;
    // }
    // closed_ = true;
    //
    // status_ = absl::CancelledError("WebRtcEvergreenWireStream HalfClosed");
    // recv_channel_.writer()->Close();
    // cv_.SignalAll();
  }

  absl::Status GetStatus() const override {
    concurrency::MutexLock lock(&mutex_);
    return status_;
  }

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override {
    return data_channel_.get();
  }

 private:
  mutable concurrency::Mutex mutex_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);

  absl::Status status_ ABSL_GUARDED_BY(mutex_);

  const std::string id_;
  std::shared_ptr<rtc::PeerConnection> connection_;
  std::shared_ptr<rtc::DataChannel> data_channel_;
  concurrency::Channel<SessionMessage> recv_channel_{kBufferSize};

  bool opened_ ABSL_GUARDED_BY(mutex_) = false;
  bool closed_ ABSL_GUARDED_BY(mutex_) = false;
};

class SignallingClient;

class WebRtcEvergreenServer {
 public:
  explicit WebRtcEvergreenServer(
      eglt::Service* absl_nonnull service, std::string_view address = "0.0.0.0",
      uint16_t port = 20000, std::string_view signalling_address = "localhost",
      uint16_t signalling_port = 80,
      std::string_view signalling_identity = "server");

  ~WebRtcEvergreenServer();

  void Run();

  absl::Status Cancel() {
    concurrency::MutexLock lock(&mutex_);
    return CancelInternal();
  }

  absl::Status Join() {
    concurrency::MutexLock lock(&mutex_);
    return JoinInternal();
  }

 private:
  using DataChannelConnectionMap =
      absl::flat_hash_map<std::string, WebRtcDataChannelConnection>;
  void RunLoop();

  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  std::shared_ptr<SignallingClient> InitSignallingClient(
      std::string_view signalling_address, uint16_t signalling_port,
      DataChannelConnectionMap* absl_nonnull connections);

  eglt::Service* absl_nonnull const service_;

  const std::string address_;
  const uint16_t port_;
  const std::string signalling_address_;
  const uint16_t signalling_port_;
  const std::string signalling_identity_;

  concurrency::Channel<WebRtcDataChannelConnection> ready_data_connections_;
  concurrency::Mutex mutex_;
  std::unique_ptr<concurrency::Fiber> main_loop_;
};

absl::StatusOr<std::unique_ptr<WebRtcEvergreenWireStream>>
AcceptStreamFromSignalling(std::string_view identity = "server",
                           std::string_view address = "localhost",
                           uint16_t port = 80);

absl::StatusOr<std::unique_ptr<WebRtcEvergreenWireStream>>
StartStreamWithSignalling(std::string_view identity = "client",
                          std::string_view peer_identity = "server",
                          std::string_view address = "localhost",
                          uint16_t port = 80);

}  // namespace eglt::sdk

#endif  // EGLT_SDK_WEBRTC_H_