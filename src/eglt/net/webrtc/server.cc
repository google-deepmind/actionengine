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

#include "eglt/net/webrtc/server.h"

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
#include "eglt/net/webrtc/wire_stream.h"
#include "eglt/stores/byte_chunking.h"
#include "eglt/util/map_util.h"

// TODO: split this file into multiple files for better organization.

namespace eglt::net {

WebRtcEvergreenServer::WebRtcEvergreenServer(
    eglt::Service* absl_nonnull service, std::string_view address,
    uint16_t port, std::string_view signalling_address,
    uint16_t signalling_port, std::string_view signalling_identity,
    std::optional<RtcConfig> rtc_config)
    : service_(service),
      address_(address),
      port_(port),
      signalling_address_(signalling_address),
      signalling_port_(signalling_port),
      signalling_identity_(signalling_identity),
      rtc_config_(std::move(rtc_config)),
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

  if (main_loop_ != nullptr) {
    mu_.Unlock();
    main_loop_->Join();
    mu_.Lock();
    main_loop_ = nullptr;
  }

  return absl::OkStatus();
}

void WebRtcEvergreenServer::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  DataChannelConnectionMap connections;
  std::shared_ptr<SignallingClient> signalling_client;

  const auto channel_reader = ready_data_connections_.reader();

  int retries_remaining = 5000000;

  while (true) {
    if (signalling_client == nullptr) {
      signalling_client = InitSignallingClient(signalling_address_,
                                               signalling_port_, &connections);
      if (const auto status =
              signalling_client->ConnectWithIdentity(signalling_identity_);
          !status.ok()) {
        LOG(ERROR)
            << "WebRtcEvergreenServer failed to connect to signalling server: "
            << status;
        break;
      }
    }

    WebRtcDataChannelConnection next_connection;
    bool channel_open;

    mu_.Unlock();
    const int selected =
        thread::Select({channel_reader->OnRead(&next_connection, &channel_open),
                        signalling_client->OnError(), thread::OnCancel()});
    mu_.Lock();

    // Check if our fiber has been cancelled, which means we should stop.
    if (thread::Cancelled() || selected == 2) {
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
      signalling_client = nullptr;
      --retries_remaining;
      continue;
    }

    // This happens when the channel is closed for writing and does not have
    // any more data to read.
    if (!channel_open) {
      LOG(INFO) << "WebRtcEvergreenServer RunLoop was cancelled by externally "
                   "closing the channel for new connections.";
    }

    if (next_connection.connection->state() ==
            rtc::PeerConnection::State::Failed ||
        next_connection.connection->state() ==
            rtc::PeerConnection::State::Closed) {
      LOG(ERROR) << "WebRtcEvergreenServer RunLoop could not accept a new "
                    "connection because the connection is in a failed or "
                    "closed state.";
      continue;
    }

    if (next_connection.data_channel == nullptr) {
      LOG(ERROR)
          << "WebRtcEvergreenServer RunLoop received a connection without"
             "a data channel. This should not happen.";
      continue;
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

  for (auto& [peer_id, connection] : connections) {
    connection.data_channel->resetCallbacks();
    connection.connection->resetCallbacks();
    if (connection.data_channel) {
      connection.data_channel->close();
    }
    if (connection.connection) {
      connection.connection->close();
    }
  }
  connections.clear();
}

std::shared_ptr<SignallingClient> WebRtcEvergreenServer::InitSignallingClient(
    std::string_view signalling_address, uint16_t signalling_port,
    DataChannelConnectionMap* absl_nonnull connections) {
  auto signalling_client =
      std::make_shared<SignallingClient>(signalling_address, signalling_port);

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

    RtcConfig config = rtc_config_.value_or(RtcConfig());
    config.enable_ice_udp_mux = true;
    config.port_range_begin = port_;
    config.port_range_end = port_;
    if (config.stun_servers.empty()) {
      config.stun_servers.emplace_back("stun.l.google.com:19302");
    }
    rtc::Configuration libdatachannel_config =
        config.BuildLibdatachannelConfig();
    libdatachannel_config.bindAddress = address_;

    auto connection =
        std::make_unique<rtc::PeerConnection>(std::move(libdatachannel_config));

    connection->onLocalDescription([this, peer_id = std::string(peer_id),
                                    signalling_client](
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
                                  signalling_client](
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
    // connection->onIceStateChange([this, peer_id = std::string(peer_id),
    //                               connections,
    //                               connection_ptr = connection.get()](
    //                                  rtc::PeerConnection::IceState state) {
    //   if (state == rtc::PeerConnection::IceState::Failed) {
    //     const auto map_node = connections->extract(peer_id);
    //     CHECK(!map_node.empty())
    //         << "WebRtcEvergreenServer no connection for peer: " << peer_id;
    //     if (map_node.mapped().data_channel) {
    //       map_node.mapped().data_channel->close();
    //     }
    //     map_node.mapped().connection->resetCallbacks();
    //     map_node.mapped().connection->close();
    //     return;
    //   }
    //
    //   // if (state == rtc::PeerConnection::IceState::Completed) {
    //   //   connection_ptr->onLocalDescription([](const rtc::Description&) {});
    //   //   connection_ptr->onLocalCandidate([](const rtc::Candidate&) {});
    //   //   connection_ptr->onIceStateChange([](rtc::PeerConnection::IceState) {});
    //   // }
    // });

    connection->onDataChannel([this, peer_id = std::string(peer_id),
                               connection_ptr = connection.get(), connections](
                                  std::shared_ptr<rtc::DataChannel> dc) {
      const auto map_node = connections->extract(peer_id);
      CHECK(!map_node.empty())
          << "WebRtcEvergreenServer no connection for peer: " << peer_id;

      WebRtcDataChannelConnection connection_from_map =
          std::move(map_node.mapped());
      connection_from_map.data_channel = std::move(dc);

      ready_data_connections_.writer()->WriteUnlessCancelled(
          std::move(connection_from_map));
    });

    connections->emplace(peer_id, WebRtcDataChannelConnection{
                                      .connection = std::move(connection),
                                      .data_channel = nullptr});

    FindOrDie(*connections, peer_id)
        .connection->setRemoteDescription(rtc::Description(description));
  });

  signalling_client->OnCandidate([this, connections](
                                     std::string_view peer_id,
                                     const boost::json::value& message) {
    eglt::MutexLock lock(&mu_);
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

}  // namespace eglt::net