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

#include "actionengine/net/webrtc/wire_stream.h"

#include <cstddef>
#include <functional>
#include <utility>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <boost/system/detail/error_code.hpp>
#include <rtc/candidate.hpp>
#include <rtc/common.hpp>
#include <rtc/configuration.hpp>
#include <rtc/description.hpp>
#include <rtc/reliability.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/msgpack.h"
#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/stores/byte_chunking.h"
#include "actionengine/util/status_macros.h"
#include "cppack/msgpack.h"

namespace act::net {

absl::StatusOr<TurnServer> TurnServer::FromString(std::string_view url) {
  std::string_view username_password;
  std::string_view hostname_port;

  if (size_t at_pos = url.find('@'); at_pos != std::string_view::npos) {
    username_password = url.substr(0, at_pos);
    hostname_port = url.substr(at_pos + 1);
  }

  if (hostname_port.empty()) {
    return absl::InvalidArgumentError(
        "TurnServer URL must contain a hostname and port");
  }

  std::string_view hostname;
  uint16_t port = 3478;  // Default TURN port

  if (size_t colon_pos = hostname_port.find(':');
      colon_pos == std::string_view::npos) {
    hostname = hostname_port;
  } else {
    hostname = hostname_port.substr(0, colon_pos);
    if (std::string_view port_str = hostname_port.substr(colon_pos + 1);
        !absl::SimpleAtoi(port_str, &port)) {
      return absl::InvalidArgumentError(
          "TurnServer URL contains an invalid port");
    }
  }

  std::string username;
  std::string password;
  if (username_password.empty()) {
    username = "actionengine";
    password = "";
  } else {
    if (size_t colon_pos = username_password.find(':');
        colon_pos == std::string_view::npos) {
      username = username_password;
      password = "";
    } else {
      username = username_password.substr(0, colon_pos);
      password = username_password.substr(colon_pos + 1);
    }
  }

  TurnServer server;
  server.hostname = std::string(hostname);
  server.port = port;
  server.username = std::move(username);
  server.password = std::move(password);

  return server;
}

bool TurnServer::operator==(const TurnServer& other) const {
  return hostname == other.hostname && port == other.port &&
         username == other.username && password == other.password;
}

bool AbslParseFlag(std::string_view text, TurnServer* server,
                   std::string* error) {
  auto result = TurnServer::FromString(text);
  if (!result.ok()) {
    *error = result.status().message();
    return false;
  }
  *server = std::move(result.value());
  return true;
}

std::string AbslUnparseFlag(const TurnServer& server) {
  if (server.username.empty()) {
    return absl::StrFormat("%s:%d", server.hostname, server.port);
  }

  return absl::StrFormat("%s:%s@%s:%d", server.username, server.password,
                         server.hostname, server.port);
}

bool AbslParseFlag(std::string_view text,
                   std::vector<act::net::TurnServer>* servers,
                   std::string* error) {
  if (text.empty()) {
    return true;
  }
  for (const auto& server_str : absl::StrSplit(text, ',')) {
    act::net::TurnServer server;
    if (!AbslParseFlag(server_str, &server, error)) {
      return false;
    }
    servers->push_back(std::move(server));
  }
  return true;
}

std::string AbslUnparseFlag(const std::vector<act::net::TurnServer>& servers) {
  if (servers.empty()) {
    return "";
  }
  return absl::StrJoin(servers, ",",
                       [](std::string* out, const TurnServer& server) {
                         *out = AbslUnparseFlag(server);
                       });
}

rtc::Configuration RtcConfig::BuildLibdatachannelConfig() const {
  rtc::Configuration config;
  config.maxMessageSize = max_message_size;
  config.portRangeBegin = port_range_begin;
  config.portRangeEnd = port_range_end;
  config.enableIceUdpMux = enable_ice_udp_mux;

  for (const auto& server : stun_servers) {
    config.iceServers.emplace_back(server);
  }
  for (const auto& server : turn_servers) {
    config.iceServers.emplace_back(server.hostname, server.port,
                                   server.username, server.password);
  }

  return config;
}

WebRtcWireStream::WebRtcWireStream(
    std::shared_ptr<rtc::DataChannel> data_channel,
    std::shared_ptr<rtc::PeerConnection> connection)
    : id_(data_channel->label()),
      connection_(std::move(connection)),
      data_channel_(std::move(data_channel)) {

  data_channel_->onMessage(
      [this](rtc::binary message) {
        const size_t message_size = message.size();
        const auto data = reinterpret_cast<uint8_t*>(std::move(message).data());
        absl::StatusOr<data::BytePacket> packet =
            data::ParseBytePacket(data, message_size);

        act::MutexLock lock(&mu_);

        if (closed_) {
          return;
        }

        if (!packet.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream unpack failed: %s",
                              packet.status().message())));
          return;
        }

        const uint64_t transient_id = GetTransientIdFromPacket(*packet);
        auto& chunked_message = chunked_messages_[transient_id];
        if (!chunked_message) {
          chunked_message = std::make_unique<data::ChunkedBytes>();
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
            chunked_message->ConsumeCompleteBytes();
        if (!message_data.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream consume failed: %s",
                              message_data.status().message())));
          return;
        }

        mu_.unlock();
        absl::StatusOr<WireMessage> unpacked =
            cppack::Unpack<WireMessage>(std::vector(*std::move(message_data)));
        mu_.lock();

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
      act::MutexLock lock(&mu_);
      status_ = absl::OkStatus();
      opened_ = true;
      cv_.SignalAll();
    });
  }

  data_channel_->onClosed([this]() {
    act::MutexLock lock(&mu_);
    half_closed_ = true;

    if (!closed_) {
      status_ = absl::CancelledError("WebRtcWireStream closed");
      recv_channel_.writer()->Close();
    }

    closed_ = true;
    cv_.SignalAll();
  });

  data_channel_->onError([this](const std::string& error) {
    act::MutexLock lock(&mu_);
    half_closed_ = true;

    if (!closed_) {
      status_ = absl::InternalError(
          absl::StrFormat("WebRtcWireStream error: %s", error));
      recv_channel_.writer()->Close();
    }

    closed_ = true;
    cv_.SignalAll();
  });
}

WebRtcWireStream::~WebRtcWireStream() {
  data_channel_->close();
  act::MutexLock lock(&mu_);
  while (!closed_) {
    cv_.Wait(&mu_);
  }
  connection_->close();
}

absl::Status WebRtcWireStream::Send(WireMessage message) {
  act::MutexLock lock(&mu_);

  if (!status_.ok()) {
    return status_;
  }

  if (half_closed_) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream is half-closed, cannot send messages");
  }

  return SendInternal(std::move(message));
}

absl::Status WebRtcWireStream::SendInternal(WireMessage message)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  while (!opened_ && !closed_) {
    cv_.Wait(&mu_);
  }

  if (closed_) {
    return absl::CancelledError("WebRtcWireStream is closed");
  }

  const uint64_t transient_id = next_transient_id_++;

  mu_.unlock();

  const std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));

  const std::vector<data::BytePacket> packets = data::SplitBytesIntoPackets(
      message_uint8_t, transient_id,
      static_cast<int64_t>(connection_->remoteMaxMessageSize()));

  absl::Status status;
  for (const auto& packet : packets) {
    std::vector<uint8_t> serialized_packet = data::SerializeBytePacket(packet);
    const rtc::byte* message_chunk_data =
        reinterpret_cast<rtc::byte*>(serialized_packet.data());
    rtc::binary message_chunk_bytes(
        message_chunk_data, message_chunk_data + serialized_packet.size());
    data_channel_->send(std::move(message_chunk_bytes));
  }

  mu_.lock();
  return status;
}

absl::StatusOr<std::optional<WireMessage>> WebRtcWireStream::Receive(
    absl::Duration timeout) {
  const absl::Time now = absl::Now();
  act::MutexLock lock(&mu_);

  const absl::Time deadline =
      !half_closed_ ? now + timeout : now + kHalfCloseTimeout;

  WireMessage message;
  bool ok;

  mu_.unlock();
  const int selected = thread::SelectUntil(
      deadline, {recv_channel_.reader()->OnRead(&message, &ok)});
  mu_.lock();

  if (selected == 0 && !ok) {
    return std::nullopt;
  }

  if (selected == -1) {
    return absl::DeadlineExceededError(
        "WebRtcWireStream Receive timed out while waiting for a message.");
  }

  if (message.actions.empty() && message.node_fragments.empty()) {
    // If the message is empty, it means the stream was half-closed by the
    // other end, or the other end has acknowledged our half-close.

    // Check this because we might have already closed the channel due to
    // an error or onClosed() for the underlying data channel.
    if (!closed_) {
      CHECK(recv_channel_.length() == 0)
          << "WebRtcWireStream received a half-close message, but there are "
             "still messages in the receive channel. This should not happen.";
      recv_channel_.writer()->Close();
      closed_ = true;
    }

    if (half_closed_) {
      // We initiated the half-close, so we don't need to call the
      // half-close callback, only to acknowledge
      return std::nullopt;
    }

    if (absl::Status half_close_status = HalfCloseInternal();
        !half_close_status.ok()) {
      return half_close_status;
    }

    return std::nullopt;
  }

  return message;
}

void WebRtcWireStream::Abort() {
  act::MutexLock lock(&mu_);
  if (closed_) {
    return;
  }
  // TODO: communicate an -aborted- status to the other end.
  HalfCloseInternal().IgnoreError();
  CloseOnError(absl::CancelledError("WebRtcWireStream aborted"));
}

absl::Status WebRtcWireStream::GetStatus() const {
  act::MutexLock lock(&mu_);
  return status_;
}

absl::Status WebRtcWireStream::HalfCloseInternal() {
  if (half_closed_) {
    return absl::OkStatus();
  }

  half_closed_ = true;
  RETURN_IF_ERROR(SendInternal(WireMessage{}));

  return absl::OkStatus();
}

void WebRtcWireStream::CloseOnError(absl::Status status)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  LOG(ERROR) << "WebRtcWireStream error: " << status.message();

  if (!closed_) {
    status_ = std::move(status);
    recv_channel_.writer()->Close();
  }

  closed_ = true;
  cv_.SignalAll();
}

static absl::StatusOr<rtc::Description> ParseDescriptionFromMessage(
    const boost::json::value& message) {
  boost::system::error_code error;
  if (const auto desc_ptr = message.find_pointer("/description", error);
      desc_ptr != nullptr && !error) {
    const auto description = desc_ptr->as_string().c_str();
    return rtc::Description(description);
  }

  return absl::InvalidArgumentError(
      "No 'description' field in signalling message: " +
      boost::json::serialize(message));
}

static absl::StatusOr<rtc::Candidate> ParseCandidateFromMessage(
    const boost::json::value& message) {
  boost::system::error_code error;
  if (const auto candidate_ptr = message.find_pointer("/candidate", error);
      candidate_ptr != nullptr && !error) {
    const auto candidate_str = candidate_ptr->as_string().c_str();
    return rtc::Candidate(candidate_str);
  }

  return absl::InvalidArgumentError(
      "No 'candidate' field in signalling message: " +
      boost::json::serialize(message));
}

static std::string MakeCandidateMessage(std::string_view peer_id,
                                        const rtc::Candidate& candidate) {
  boost::json::object candidate_json;
  candidate_json["id"] = peer_id;
  candidate_json["type"] = "candidate";
  candidate_json["candidate"] = std::string(candidate);
  candidate_json["mid"] = candidate.mid();

  return boost::json::serialize(candidate_json);
}

static std::string MakeOfferMessage(std::string_view peer_id,
                                    const rtc::Description& description) {
  boost::json::object offer;
  offer["id"] = peer_id;
  offer["type"] = "offer";
  offer["description"] = description.generateSdp("\r\n");

  return boost::json::serialize(offer);
}

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity,
    std::string_view signalling_address, uint16_t signalling_port,
    std::optional<RtcConfig> rtc_config) {
  SignallingClient signalling_client{signalling_address, signalling_port};

  RtcConfig config = std::move(rtc_config).value_or(RtcConfig());
  config.port_range_begin = 1025;
  config.port_range_end = 65535;

  auto connection =
      std::make_unique<rtc::PeerConnection>(config.BuildLibdatachannelConfig());

  thread::PermanentEvent done;
  absl::Status status;

  signalling_client.OnAnswer([&connection,
                              peer_identity = std::string(peer_identity), &done,
                              &status](std::string_view received_peer_id,
                                       const boost::json::value& message) {
    if (received_peer_id != peer_identity) {
      return;
    }

    absl::StatusOr<rtc::Description> description =
        ParseDescriptionFromMessage(message);
    if (!description.ok()) {
      status = description.status();
      done.Notify();
      return;
    }
    connection->setRemoteDescription(*std::move(description));
  });

  signalling_client.OnCandidate(
      [&connection, peer_identity = std::string(peer_identity), &done, &status](
          std::string_view received_peer_id,
          const boost::json::value& message) {
        if (received_peer_id != peer_identity) {
          return;
        }

        absl::StatusOr<rtc::Candidate> candidate =
            ParseCandidateFromMessage(message);
        if (!candidate.ok()) {
          status = candidate.status();
          done.Notify();
          return;
        }
        connection->addRemoteCandidate(*std::move(candidate));
      });

  connection->onLocalCandidate([peer_id = std::string(peer_identity),
                                &signalling_client, &done,
                                &status](const rtc::Candidate& candidate) {
    const std::string message = MakeCandidateMessage(peer_id, candidate);
    status.Update(signalling_client.Send(message));
    if (!status.ok()) {
      done.Notify();
    }
  });

  RETURN_IF_ERROR(signalling_client.ConnectWithIdentity(identity));

  std::string offer_message =
      MakeOfferMessage(peer_identity, connection->createOffer());
  RETURN_IF_ERROR(signalling_client.Send(offer_message));

  auto init = rtc::DataChannelInit{};
  init.reliability.unordered = true;
  auto data_channel =
      connection->createDataChannel(std::string(identity), std::move(init));

  data_channel->onOpen([&done]() { done.Notify(); });
  connection->onIceStateChange([&done, connection = connection.get()](
                                   rtc::PeerConnection::IceState state) {
    if (state == rtc::PeerConnection::IceState::Failed) {
      connection->resetCallbacks();
      connection->close();
      done.Notify();
    }
  });

  thread::Select(
      {done.OnEvent(), signalling_client.OnError(), thread::OnCancel()});
  signalling_client.ResetCallbacks();

  RETURN_IF_ERROR(status);
  RETURN_IF_ERROR(signalling_client.GetStatus());
  if (thread::Cancelled()) {
    return absl::CancelledError("WebRtcWireStream connection cancelled");
  }

  // data_channel->resetCallbacks();

  if (!data_channel->isOpen()) {
    return absl::InternalError(
        "WebRtcWireStream data channel is not open, likely due to a failed "
        "connection.");
  }

  return WebRtcDataChannelConnection{
      .connection = std::move(connection),
      .data_channel = std::move(data_channel),
  };
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

}  // namespace act::net