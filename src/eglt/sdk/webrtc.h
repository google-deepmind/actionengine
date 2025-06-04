#ifndef EGLT_SDK_WEBRTC_H_
#define EGLT_SDK_WEBRTC_H_

#include <rtc/rtc.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"

namespace eglt::sdk {

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
    concurrency::MutexLock lock(&mutex_);
    if (closed_) {
      DLOG(INFO) << "WebRtcEvergreenWireStream HalfClose: already closed";
      return;
    }
    closed_ = true;
    data_channel_->close();
    status_ = absl::CancelledError("WebRtcEvergreenWireStream HalfClosed");
    recv_channel_.writer()->Close();
    cv_.SignalAll();
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

std::unique_ptr<WebRtcEvergreenWireStream> AcceptStreamFromSignalling(
    std::string_view address = "demos.helena.direct", uint16_t port = 19000);

std::unique_ptr<WebRtcEvergreenWireStream> StartStreamWithSignalling(
    std::string_view id = "client", std::string_view peer_id = "server",
    std::string_view address = "demos.helena.direct", uint16_t port = 19000);

}  // namespace eglt::sdk

#endif  // EGLT_SDK_WEBRTC_H_