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

#include "eglt/net/webrtc/webrtc.h"

#include <cstddef>
#include <functional>

#include <absl/strings/str_format.h>
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

#include "cppack/msgpack.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/webrtc/signalling.h"
#include "eglt/stores/byte_chunking.h"
#include "eglt/util/map_util.h"

// TODO: split this file into multiple files for better organization.

namespace eglt::net {

WebRtcWireStream::WebRtcWireStream(
    std::shared_ptr<rtc::DataChannel> data_channel,
    std::shared_ptr<rtc::PeerConnection> connection)
    : id_(data_channel->label()),
      connection_(std::move(connection)),
      data_channel_(std::move(data_channel)) {

  data_channel_->onMessage(
      [this](rtc::binary message) {
        const auto data = reinterpret_cast<uint8_t*>(message.data());
        absl::StatusOr<data::BytePacket> packet =
            data::ParseBytePacket(std::vector(data, data + message.size()));

        eglt::MutexLock lock(&mu_);

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

        mu_.Unlock();
        absl::StatusOr<SessionMessage> unpacked =
            cppack::Unpack<SessionMessage>(
                std::vector(*std::move(message_data)));
        mu_.Lock();

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
      eglt::MutexLock lock(&mu_);
      status_ = absl::OkStatus();
      opened_ = true;
      cv_.SignalAll();
    });
  }

  data_channel_->onClosed([this]() {
    eglt::MutexLock lock(&mu_);
    closed_ = true;
    status_ = absl::CancelledError("WebRtcWireStream closed");
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  });

  data_channel_->onError([this](const std::string& error) {
    eglt::MutexLock lock(&mu_);
    closed_ = true;
    status_ = absl::InternalError(
        absl::StrFormat("WebRtcWireStream error: %s", error));
    recv_channel_.writer()->Close();
    cv_.SignalAll();
  });
}

WebRtcWireStream::~WebRtcWireStream() {
  data_channel_->close();
  eglt::MutexLock lock(&mu_);
  while (!closed_) {
    cv_.Wait(&mu_);
  }
  connection_->close();
}

absl::Status WebRtcWireStream::Send(SessionMessage message) {
  uint64_t transient_id = 0;
  {
    eglt::MutexLock lock(&mu_);
    if (!status_.ok()) {
      return status_;
    }

    while (!opened_ && !closed_) {
      cv_.Wait(&mu_);
    }

    if (closed_) {
      return absl::CancelledError("WebRtcWireStream is closed");
    }
    transient_id = next_transient_id_++;
  }

  std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));

  std::vector<data::BytePacket> packets = data::SplitBytesIntoPackets(
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
  thread::PermanentEvent data_channel_event;

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

  const int selected =
      thread::Select({data_channel_event.OnEvent(), signalling_client.OnError(),
                      thread::OnCancel()});

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
  if (thread::Cancelled()) {
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

  thread::PermanentEvent opened;
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

  const int selected = thread::Select(
      {opened.OnEvent(), signalling_client.OnError(), thread::OnCancel()});
  if (selected == 1) {
    return signalling_client.GetStatus();
  }
  if (thread::Cancelled()) {
    return absl::CancelledError("WebRtcWireStream connection cancelled");
  }

  data_channel->onOpen({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (thread::Cancelled()) {
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
  eglt::MutexLock lock(&mu_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
}

void WebRtcEvergreenServer::Run() {
  eglt::MutexLock l(&mu_);
  main_loop_ = thread::NewTree({}, [this]() {
    eglt::MutexLock lock(&mu_);
    RunLoop();
  });
}

absl::Status WebRtcEvergreenServer::CancelInternal()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
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
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcEvergreenServer Join called on either unstarted or already "
        "joined server.");
  }

  const std::unique_ptr<thread::Fiber> main_loop = std::move(main_loop_);
  main_loop_ = nullptr;

  mu_.Unlock();
  main_loop->Join();
  mu_.Lock();

  return absl::OkStatus();
}

void WebRtcEvergreenServer::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  DLOG(INFO) << "WebRtcEvergreenServer RunLoop starting.";
  DataChannelConnectionMap connections;
  auto signalling_client =
      InitSignallingClient(signalling_address_, signalling_port_, &connections);

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
    mu_.Unlock();
    const int selected =
        thread::Select({channel_reader->OnRead(&next_connection, &channel_open),
                        signalling_client->OnError(), thread::OnCancel()});
    mu_.Lock();

    // Check if our fiber has been cancelled, which means we should stop.
    if (thread::Cancelled()) {
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
      mu_.Unlock();
      eglt::SleepFor(absl::Seconds(0.5));
      mu_.Lock();
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
          eglt::MutexLock lock(&mu_);
          const auto map_node = connections->extract(peer_id);
          CHECK(!map_node.empty())
              << "WebRtcEvergreenServer no connection for peer: " << peer_id;

          WebRtcDataChannelConnection connection_from_map =
              std::move(map_node.mapped());
          connection_from_map.data_channel = std::move(dc);

          ready_data_connections_.writer()->WriteUnlessCancelled(
              std::move(connection_from_map));
        });

    eglt::MutexLock lock(&mu_);
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