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

#include "actionengine/net/webrtc/server.h"

#include <functional>
#include <utility>
#include <vector>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <boost/system/detail/error_code.hpp>
#include <rtc/candidate.hpp>
#include <rtc/configuration.hpp>
#include <rtc/datachannel.hpp>
#include <rtc/description.hpp>
#include <rtc/peerconnection.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/net/webrtc/wire_stream.h"
#include "actionengine/util/map_util.h"

// TODO: split this file into multiple files for better organization.

namespace act::net {

WebRtcServer::WebRtcServer(act::Service* absl_nonnull service,
                           std::string_view address, uint16_t port,
                           std::string_view signalling_address,
                           uint16_t signalling_port,
                           std::string_view signalling_identity,
                           std::optional<RtcConfig> rtc_config)
    : service_(service),
      address_(address),
      port_(port),
      signalling_address_(signalling_address),
      signalling_port_(signalling_port),
      signalling_identity_(signalling_identity),
      rtc_config_(std::move(rtc_config)),
      ready_data_connections_(32) {}

WebRtcServer::~WebRtcServer() {
  act::MutexLock lock(&mu_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
}

void WebRtcServer::Run() {
  act::MutexLock l(&mu_);
  main_loop_ = thread::NewTree({}, [this]() {
    act::MutexLock lock(&mu_);
    RunLoop();
  });
}

absl::Status WebRtcServer::Cancel() {
  act::MutexLock lock(&mu_);
  return CancelInternal();
}

absl::Status WebRtcServer::Join() {
  act::MutexLock lock(&mu_);
  absl::Status status = JoinInternal();
  return status;
}

absl::Status WebRtcServer::CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcServer Cancel called on either unstarted or already "
        "cancelled server.");
  }
  ready_data_connections_.writer()->Close();
  main_loop_->Cancel();
  return absl::OkStatus();
}

absl::Status WebRtcServer::JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {

  if (main_loop_ != nullptr) {
    mu_.unlock();
    main_loop_->Join();
    mu_.lock();
    main_loop_ = nullptr;
  }

  return absl::OkStatus();
}

static std::string MakeCandidateMessage(std::string_view peer_id,
                                        const rtc::Candidate& candidate) {
  boost::json::object message;
  message["type"] = "candidate";
  message["id"] = peer_id;
  message["candidate"] = std::string(candidate);
  message["mid"] = candidate.mid();
  return boost::json::serialize(message);
}

static std::string MakeAnswerMessage(std::string_view peer_id,
                                     const rtc::Description& description) {
  boost::json::object message;
  message["type"] = "answer";
  message["id"] = peer_id;
  message["description"] = description.generateSdp("\r\n");
  return boost::json::serialize(message);
}

static absl::StatusOr<rtc::Description> ParseDescriptionFromOfferMessage(
    const boost::json::value& message) {
  boost::system::error_code error;
  if (const auto type_ptr = message.find_pointer("/type", error);
      type_ptr == nullptr || error || type_ptr->as_string() != "offer") {
    return absl::InvalidArgumentError("Not an offer message: " +
                                      boost::json::serialize(message));
  }

  const auto desc_ptr = message.find_pointer("/description", error);
  if (desc_ptr == nullptr || error) {
    return absl::InvalidArgumentError(
        "No 'description' field in signalling message: " +
        boost::json::serialize(message));
  }

  return rtc::Description(desc_ptr->as_string().c_str());
}

static absl::StatusOr<rtc::Candidate> ParseCandidateFromMessage(
    const boost::json::value& message) {
  boost::system::error_code error;
  if (const auto type_ptr = message.find_pointer("/type", error);
      type_ptr == nullptr || error || type_ptr->as_string() != "candidate") {
    return absl::InvalidArgumentError("Not a candidate message: " +
                                      boost::json::serialize(message));
  }

  const auto candidate_ptr = message.find_pointer("/candidate", error);
  if (candidate_ptr == nullptr || error) {
    return absl::InvalidArgumentError(
        "No 'candidate' field in signalling message: " +
        boost::json::serialize(message));
  }

  return rtc::Candidate(candidate_ptr->as_string().c_str());
}

void WebRtcServer::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
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
        LOG(ERROR) << "WebRtcServer failed to connect to "
                      "signalling server: "
                   << status;
        break;
      }
    }

    absl::StatusOr<WebRtcDataChannelConnection> next_connection_or;
    bool channel_open;

    mu_.unlock();
    const int selected = thread::Select(
        {channel_reader->OnRead(&next_connection_or, &channel_open),
         signalling_client->OnError(), thread::OnCancel()});
    mu_.lock();

    // Check if our fiber has been cancelled, which means we should stop.
    if (thread::Cancelled()) {
      break;
    }

    // If the signalling client has an error, we need to restart it while
    // we still have retries left.
    if (selected == 1) {
      if (retries_remaining <= 0) {
        LOG(ERROR) << "WebRtcServer signalling client error: "
                   << signalling_client->GetStatus()
                   << ". No more retries left. Exiting.";
        break;
      }
      LOG(ERROR) << "WebRtcServer signalling client error: "
                 << signalling_client->GetStatus()
                 << ". Restarting in 0.5 seconds.";
      mu_.unlock();
      act::SleepFor(absl::Seconds(0.5));
      mu_.lock();
      signalling_client = nullptr;
      --retries_remaining;
      continue;
    }

    // This happens when the channel is closed for writing and does not have
    // any more data to read.
    if (!channel_open) {
      LOG(INFO) << "WebRtcServer RunLoop was cancelled by externally "
                   "closing the channel for new connections.";
    }

    if (!next_connection_or.status().ok()) {
      LOG(ERROR) << "WebRtcServer RunLoop received an error while waiting "
                    "for a new connection: "
                 << next_connection_or.status();
      continue;
    }

    WebRtcDataChannelConnection next_connection =
        *std::move(next_connection_or);

    if (next_connection.connection->state() ==
            rtc::PeerConnection::State::Failed ||
        next_connection.connection->state() ==
            rtc::PeerConnection::State::Closed) {
      LOG(ERROR) << "WebRtcServer RunLoop could not accept a new "
                    "connection because the connection is in a failed or "
                    "closed state.";
      continue;
    }

    if (next_connection.data_channel == nullptr) {
      LOG(ERROR) << "WebRtcServer RunLoop received a connection without"
                    "a data channel. This should not happen.";
      continue;
    }

    auto stream = std::make_unique<WebRtcWireStream>(
        std::move(next_connection.data_channel),
        std::move(next_connection.connection));

    if (auto service_connection =
            service_->EstablishConnection(std::move(stream));
        !service_connection.ok()) {
      LOG(ERROR) << "WebRtcServer EstablishConnection failed: "
                 << service_connection.status();
      continue;
    } else {
      DLOG(INFO) << "WebRtcServer established a new connection from peer: "
                 << (*service_connection)->stream_id;
    }
    // At this point, the connection is established and the responsibility
    // of the WebRtcServer is done. The service will handle the
    // connection from here on out.
  }

  // Clean up all connections that might not be ready yet.
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

  // Very important: callbacks for the signalling client must be reset
  // because their closures contain shared pointers to the client itself.
  // If we don't reset them, the client will never be destroyed.
  signalling_client->ResetCallbacks();
  DCHECK(signalling_client.use_count() == 1)
      << "WebRtcServer signalling client should be the only owner of "
         "the SignallingClient instance at this point, but it is not. "
         "This indicates a bug in the code.";
  signalling_client.reset();
}

std::shared_ptr<SignallingClient> WebRtcServer::InitSignallingClient(
    std::string_view signalling_address, uint16_t signalling_port,
    DataChannelConnectionMap* absl_nonnull connections) {
  auto signalling_client =
      std::make_shared<SignallingClient>(signalling_address, signalling_port);

  auto abort_establishment_with_error = [connections, this](
                                            std::string_view peer_id,
                                            const absl::Status& status) {
    DCHECK(!status.ok()) << "abort_establishment_with_error called with an OK "
                            "status, this should not "
                            "happen.";
    if (const auto it = connections->find(peer_id); it != connections->end()) {
      if (it->second.data_channel) {
        it->second.data_channel->close();
      }
      it->second.connection->resetCallbacks();
      it->second.connection->close();
      connections->erase(it);
    }
    ready_data_connections_.writer()->Write(status);
  };

  signalling_client->OnOffer([this, connections, signalling_client,
                              &abort_establishment_with_error](
                                 std::string_view peer_id,
                                 const boost::json::value& message) {
    if (connections->contains(std::string(peer_id))) {
      abort_establishment_with_error(
          peer_id,
          absl::AlreadyExistsError(absl::StrFormat(
              "Connection with peer ID '%s' already exists.", peer_id)));
      return;
    }

    absl::StatusOr<rtc::Description> description =
        ParseDescriptionFromOfferMessage(message);
    if (!description.ok()) {
      abort_establishment_with_error(peer_id, description.status());
      return;
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

    connection->onLocalDescription(
        [peer_id = std::string(peer_id), signalling_client,
         &abort_establishment_with_error](
            const rtc::Description& local_description) {
          const std::string answer_message =
              MakeAnswerMessage(peer_id, local_description);
          if (const auto answer_status =
                  signalling_client->Send(answer_message);
              !answer_status.ok()) {
            abort_establishment_with_error(peer_id, answer_status);
          }
        });

    connection->onLocalCandidate(
        [peer_id = std::string(peer_id), signalling_client,
         &abort_establishment_with_error](const rtc::Candidate& candidate) {
          const std::string candidate_message =
              MakeCandidateMessage(peer_id, candidate);
          if (const auto candidate_status =
                  signalling_client->Send(candidate_message);
              !candidate_status.ok()) {
            abort_establishment_with_error(peer_id, candidate_status);
          }
        });

    connection->onIceStateChange([peer_id = std::string(peer_id),
                                  connection_ptr = connection.get(),
                                  &abort_establishment_with_error](
                                     rtc::PeerConnection::IceState state) {
      // Unsuccessful connections should be cleaned up: data channel closed,
      // if any, and connection closed.
      if (state == rtc::PeerConnection::IceState::Failed) {
        abort_establishment_with_error(
            peer_id,
            absl::InternalError(absl::StrFormat(
                "WebRtcServer connection with peer ID '%s' failed.", peer_id)));
        return;
      }

      // Successful connections should have their callbacks reset, as the
      // callbacks' closures contain shared pointers to the signalling client,
      // which would prevent it from being destroyed.
      if (state == rtc::PeerConnection::IceState::Completed ||
          state == rtc::PeerConnection::IceState::Connected) {
        connection_ptr->onLocalDescription([](const rtc::Description&) {});
        connection_ptr->onLocalCandidate([](const rtc::Candidate&) {});
        connection_ptr->onIceStateChange([](rtc::PeerConnection::IceState) {});
      }
    });

    connection->onDataChannel([this, peer_id = std::string(peer_id),
                               connection_ptr = connection.get(), connections](
                                  std::shared_ptr<rtc::DataChannel> dc) {
      const auto map_node = connections->extract(peer_id);
      DCHECK(!map_node.empty())
          << "WebRtcServer no connection for peer: " << peer_id;

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
        .connection->setRemoteDescription(*std::move(description));
  });

  signalling_client->OnCandidate(
      [connections, &abort_establishment_with_error](
          std::string_view peer_id, const boost::json::value& message) {
        if (!connections->contains(peer_id)) {
          return;
        }

        absl::StatusOr<rtc::Candidate> candidate =
            ParseCandidateFromMessage(message);
        if (!candidate.ok()) {
          abort_establishment_with_error(peer_id, candidate.status());
          return;
        }

        FindOrDie(*connections, peer_id)
            .connection->addRemoteCandidate(*std::move(candidate));
      });

  return signalling_client;
}

}  // namespace act::net