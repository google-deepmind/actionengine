#ifndef EGLT_SDK_WEBRTC_H_
#define EGLT_SDK_WEBRTC_H_

#include <rtc/rtc.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"

namespace eglt::sdk {

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

class ChunkedWebRtcMessage;

class WebRtcEvergreenWireStream final : public EvergreenWireStream {
 public:
  static constexpr int kBufferSize = 256;

  explicit WebRtcEvergreenWireStream(
      std::shared_ptr<rtc::DataChannel> data_channel,
      std::shared_ptr<rtc::PeerConnection> connection = nullptr);

  ~WebRtcEvergreenWireStream() override;

  absl::Status Send(SessionMessage message) override;

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
  void CloseOnError(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    LOG(ERROR) << "WebRtcEvergreenWireStream error: " << status.message();
    closed_ = true;
    status_ = std::move(status);
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  }

  mutable concurrency::Mutex mutex_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);

  absl::Status status_ ABSL_GUARDED_BY(mutex_);

  const std::string id_;
  std::shared_ptr<rtc::PeerConnection> connection_;
  std::shared_ptr<rtc::DataChannel> data_channel_;
  concurrency::Channel<SessionMessage> recv_channel_{kBufferSize};

  absl::flat_hash_map<uint64_t, std::unique_ptr<ChunkedWebRtcMessage>>
      chunked_messages_ ABSL_GUARDED_BY(mutex_) = {};
  uint64_t next_transient_id_ ABSL_GUARDED_BY(mutex_) = 0;

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