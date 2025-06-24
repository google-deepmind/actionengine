#ifndef EGLT_NET_WEBRTC_WEBRTC_H_
#define EGLT_NET_WEBRTC_WEBRTC_H_

#include <rtc/rtc.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"
#include "eglt/stores/byte_chunking.h"

namespace eglt::net {

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

class WebRtcWireStream final : public WireStream {
 public:
  static constexpr int kBufferSize = 256;
  static constexpr absl::Duration kHalfCloseTimeout = absl::Seconds(5);

  explicit WebRtcWireStream(
      std::shared_ptr<rtc::DataChannel> data_channel,
      std::shared_ptr<rtc::PeerConnection> connection = nullptr);

  ~WebRtcWireStream() override;

  absl::Status Send(SessionMessage message) override;

  std::optional<SessionMessage> Receive() override {
    const absl::Time now = absl::Now();
    concurrency::MutexLock lock(&mu_);

    const absl::Time deadline =
        !half_closed_ ? absl::InfiniteFuture() : now + kHalfCloseTimeout;

    SessionMessage message;
    bool ok;

    mu_.Unlock();
    const int selected = thread::SelectUntil(
        deadline, {recv_channel_.reader()->OnRead(&message, &ok)});
    mu_.Lock();

    if (selected == 0 && !ok) {
      return std::nullopt;
    }

    if (message.actions.empty() && message.node_fragments.empty() ||
        selected == -1) {
      // If the message is empty, it means the stream was half-closed by the
      // other end, or the other end has acknowledged our half-close.

      if (selected == -1 && half_closed_) {
        LOG(INFO) << "No acknowledgement received for a previous half-close "
                     "message within the timeout period. ";
      }

      // Check this because we might have already closed the channel due to
      // an error or onClosed() for the underlying data channel.
      if (!closed_) {
        CHECK(recv_channel_.length() == 0)
            << "WebRtcWireStream received a half-close message, but there are "
               "still messages in the receive channel. This should not happen.";
        recv_channel_.writer()->Close();
      }

      if (half_closed_) {
        // We initiated the half-close, so we don't need to call the
        // half-close callback, only to acknowledge
        return std::nullopt;
      }

      auto half_close_callback = std::move(half_close_callback_);
      mu_.Unlock();
      half_close_callback(this);
      mu_.Lock();

      if (const auto status = HalfCloseInternal(); !status.ok()) {
        LOG(ERROR) << "WebRtcWireStream HalfCloseInternal failed: "
                   << status.message();
      }

      return std::nullopt;
    }

    return std::move(message);
  }

  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status Accept() override { return absl::OkStatus(); }

  absl::Status HalfClose() override {
    concurrency::MutexLock lock(&mu_);
    return HalfCloseInternal();
  }

  void OnHalfClose(absl::AnyInvocable<void(WireStream*)> fn) override {
    concurrency::MutexLock lock(&mu_);
    CHECK(!half_closed_)
        << "WebRtcWireStream::OnHalfClose called after the stream was already "
           "half-closed";
    half_close_callback_ = std::move(fn);
  }

  absl::Status GetStatus() const override {
    concurrency::MutexLock lock(&mu_);
    return status_;
  }

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override {
    return data_channel_.get();
  }

 private:
  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (half_closed_) {
      return absl::FailedPreconditionError(
          "WebRtcWireStream already half-closed");
    }

    half_closed_ = true;
    return absl::OkStatus();
  }

  void CloseOnError(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    LOG(ERROR) << "WebRtcWireStream error: " << status.message();
    closed_ = true;
    status_ = std::move(status);
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  }

  mutable concurrency::Mutex mu_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mu_);

  absl::Status status_ ABSL_GUARDED_BY(mu_);

  const std::string id_;
  std::shared_ptr<rtc::PeerConnection> connection_;
  std::shared_ptr<rtc::DataChannel> data_channel_;
  thread::Channel<SessionMessage> recv_channel_{kBufferSize};

  absl::flat_hash_map<uint64_t, std::unique_ptr<data::ChunkedBytes>>
      chunked_messages_ ABSL_GUARDED_BY(mu_) = {};
  uint64_t next_transient_id_ ABSL_GUARDED_BY(mu_) = 0;

  bool opened_ ABSL_GUARDED_BY(mu_) = false;
  bool closed_ ABSL_GUARDED_BY(mu_) = false;

  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  absl::AnyInvocable<void(WireStream*)> half_close_callback_ = [](WireStream*) {
  };
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
    concurrency::MutexLock lock(&mu_);
    return CancelInternal();
  }

  absl::Status Join() {
    concurrency::MutexLock lock(&mu_);
    return JoinInternal();
  }

 private:
  using DataChannelConnectionMap =
      absl::flat_hash_map<std::string, WebRtcDataChannelConnection>;
  void RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::shared_ptr<SignallingClient> InitSignallingClient(
      std::string_view signalling_address, uint16_t signalling_port,
      DataChannelConnectionMap* absl_nonnull connections);

  eglt::Service* absl_nonnull const service_;

  const std::string address_;
  const uint16_t port_;
  const std::string signalling_address_;
  const uint16_t signalling_port_;
  const std::string signalling_identity_;

  thread::Channel<WebRtcDataChannelConnection> ready_data_connections_;
  concurrency::Mutex mu_;
  std::unique_ptr<thread::Fiber> main_loop_;
};

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> AcceptStreamFromSignalling(
    std::string_view identity = "server",
    std::string_view address = "localhost", uint16_t port = 80);

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity = "client",
    std::string_view peer_identity = "server",
    std::string_view address = "localhost", uint16_t port = 80);

}  // namespace eglt::net

#endif  // EGLT_NET_WEBRTC_WEBRTC_H_