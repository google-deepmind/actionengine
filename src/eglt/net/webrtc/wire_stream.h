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

#ifndef EGLT_NET_WEBRTC_WIRE_STREAM_H_
#define EGLT_NET_WEBRTC_WIRE_STREAM_H_

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

  bool operator==(const TurnServer& other) const;

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
      "stun.l.google.com:19302",  // Google's public STUN server
  };
  std::vector<TurnServer> turn_servers;
};

struct WebRtcDataChannelConnection {
  std::shared_ptr<rtc::PeerConnection> connection;
  std::shared_ptr<rtc::DataChannel> data_channel;
};

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80,
    std::optional<RtcConfig> rtc_config = std::nullopt);

/**
 * WebRtcWireStream is a concrete implementation of WireStream that
 * uses WebRTC for communication.
 *
 * @headerfile eglt/net/webrtc/wire_stream.h
 *
 * It supports sending and receiving ActionEngine session messages over
 * a WebRTC data channel. This class is designed to be used in both
 * client and server contexts, allowing for flexible communication patterns.
 */
class WebRtcWireStream final : public WireStream {
 public:
  static constexpr int kBufferSize = 256;
  static constexpr absl::Duration kHalfCloseTimeout = absl::Seconds(5);

  explicit WebRtcWireStream(
      std::shared_ptr<rtc::DataChannel> data_channel,
      std::shared_ptr<rtc::PeerConnection> connection = nullptr);

  ~WebRtcWireStream() override;

  absl::Status Send(SessionMessage message) override;

  absl::StatusOr<std::optional<SessionMessage>> Receive(
      absl::Duration timeout) override;

  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status Accept() override { return absl::OkStatus(); }

  void HalfClose() override {
    eglt::MutexLock lock(&mu_);
    HalfCloseInternal().IgnoreError();
  }

  void Abort() override;

  absl::Status GetStatus() const override;

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override {
    return data_channel_.get();
  }

 private:
  absl::Status SendInternal(SessionMessage message)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CloseOnError(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

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
};

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity = "client",
    std::string_view peer_identity = "server",
    std::string_view address = "localhost", uint16_t port = 80);

}  // namespace eglt::net

#endif  // EGLT_NET_WEBRTC_WIRE_STREAM_H_