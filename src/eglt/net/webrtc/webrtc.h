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

#ifndef EGLT_NET_WEBRTC_WEBRTC_H_
#define EGLT_NET_WEBRTC_WEBRTC_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/hash/hash.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <rtc/datachannel.hpp>
#include <rtc/peerconnection.hpp>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/net/webrtc/signalling.h"
#include "eglt/service/service.h"
#include "eglt/stores/byte_chunking.h"

namespace eglt::net {

struct TurnServer {
  static absl::StatusOr<TurnServer> FromString(std::string_view url);

  bool operator==(const TurnServer& other) const {
    return hostname == other.hostname && port == other.port &&
           username == other.username && password == other.password;
  }

  std::string hostname;
  uint16_t port = 3478;
  std::string username;
  std::string password;
};

bool AbslParseFlag(std::string_view text, TurnServer* absl_nonnull server,
                   std::string* absl_nonnull error);

std::string AbslUnparseFlag(const TurnServer& server);

bool AbslParseFlag(std::string_view text,
                   std::vector<eglt::net::TurnServer>* absl_nonnull servers,
                   std::string* absl_nonnull error);

std::string AbslUnparseFlag(const std::vector<eglt::net::TurnServer>& servers);

struct RtcConfig {
  static constexpr int kDefaultMaxMessageSize =
      16384;  // 16 KiB to match the defaults of several browsers

  [[nodiscard]] rtc::Configuration BuildLibdatachannelConfig() const;

  std::optional<size_t> max_message_size = kDefaultMaxMessageSize;

  uint16_t port_range_begin = 19002;
  uint16_t port_range_end = 19002;
  bool enable_ice_udp_mux = true;

  std::vector<std::string> stun_servers = {
      "stun.l.google.com:19302",
  };
  std::vector<TurnServer> turn_servers;
};

struct WebRtcDataChannelConnection {
  std::shared_ptr<rtc::PeerConnection> connection;
  std::shared_ptr<rtc::DataChannel> data_channel;
};

absl::StatusOr<WebRtcDataChannelConnection> AcceptWebRtcDataChannel(
    std::string_view identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80,
    std::optional<RtcConfig> rtc_config = std::nullopt);

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80,
    std::optional<RtcConfig> rtc_config = std::nullopt);

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
    eglt::MutexLock lock(&mu_);

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

  thread::Case OnReceive(std::optional<SessionMessage>* absl_nonnull message,
                         absl::Status* absl_nonnull status) override {
    return thread::NonSelectableCase();
  }

  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status Accept() override { return absl::OkStatus(); }

  absl::Status HalfClose() override {
    eglt::MutexLock lock(&mu_);
    return HalfCloseInternal();
  }

  void OnHalfClose(absl::AnyInvocable<void(WireStream*)> fn) override {
    eglt::MutexLock lock(&mu_);
    CHECK(!half_closed_)
        << "WebRtcWireStream::OnHalfClose called after the stream was already "
           "half-closed";
    half_close_callback_ = std::move(fn);
  }

  absl::Status GetStatus() const override {
    eglt::MutexLock lock(&mu_);
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

  mutable eglt::Mutex mu_;
  mutable eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);

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

class WebRtcEvergreenServer {
 public:
  explicit WebRtcEvergreenServer(
      eglt::Service* absl_nonnull service, std::string_view address = "0.0.0.0",
      uint16_t port = 20000, std::string_view signalling_address = "localhost",
      uint16_t signalling_port = 80,
      std::string_view signalling_identity = "server",
      std::optional<RtcConfig> rtc_config = std::nullopt);

  ~WebRtcEvergreenServer();

  void Run();

  absl::Status Cancel() {
    eglt::MutexLock lock(&mu_);
    return CancelInternal();
  }

  absl::Status Join() {
    eglt::MutexLock lock(&mu_);
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
  const std::optional<RtcConfig> rtc_config_;

  thread::Channel<WebRtcDataChannelConnection> ready_data_connections_;
  eglt::Mutex mu_;
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