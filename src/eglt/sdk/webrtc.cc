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

#include <thread_on_boost/fiber.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json/src.hpp>

#include "eglt/sdk/boost_asio_utils.h"
#include "eglt/sdk/fiber_aware_websocket_stream.h"
#include "eglt/sdk/webrtc.h"

namespace eglt::sdk {

namespace asio = boost::asio;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

// void(peer_id, message)
using PeerJsonHandler =
    std::function<void(std::string_view, boost::json::value)>;

// This max message size will still be subject to the underlying protocol's
// limits. SCTP allows up to 64 KiB, and to be safe in all scenarios, this
// should actually be 16 KiB. When sending messages, the only correct way is
// to check the limit on the particular PeerConnection.
static constexpr int kMaxMessageSize = 30 * 1024 * 1024;  // 30 MiB
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

class SignallingClient {
 public:
  explicit SignallingClient(std::string_view address = "localhost",
                            uint16_t port = 80)
      : address_(address), port_(port) {}

  SignallingClient(const SignallingClient&) = delete;
  SignallingClient& operator=(const SignallingClient&) = delete;

  ~SignallingClient() { CloseStreamAndJoinLoop(); }

  void OnOffer(PeerJsonHandler on_offer) { on_offer_ = std::move(on_offer); }

  void OnCandidate(PeerJsonHandler on_candidate) {
    on_candidate_ = std::move(on_candidate);
  }

  void OnAnswer(PeerJsonHandler on_answer) {
    on_answer_ = std::move(on_answer);
  }

  concurrency::Case OnError() const { return error_event_.OnEvent(); }

  absl::Status GetStatus() const {
    concurrency::MutexLock lock(&mutex_);
    return loop_status_;
  }

  absl::Status ConnectWithIdentity(std::string_view identity) {
    if (!on_answer_ && !on_offer_ && !on_candidate_) {
      return absl::FailedPreconditionError(
          "WebsocketEvergreenServer no handlers set: connecting in this "
          "state would lose messages");
    }

    identity_ = std::string(identity);

    auto boost_stream = std::make_unique<BoostWebsocketStream>(
        *GetDefaultAsioExecutionContext());

    boost_stream->set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::request_type& req) {
          req.set(beast::http::field::user_agent,
                  "Action Engine / Evergreen Light 0.1.0 "
                  "WebsocketEvergreenWireStream client");
        }));

    absl::Status status =
        ResolveAndConnect(boost_stream.get(), address_, port_);
    if (!status.ok()) {
      return status;
    }

    stream_ = FiberAwareWebsocketStream(
        std::move(boost_stream), [this](BoostWebsocketStream* stream) {
          return DoHandshake(stream, absl::StrFormat("%s:%d", address_, port_),
                             absl::StrFormat("/%s", identity_));
        });

    {
      concurrency::MutexLock lock(&mutex_);
      loop_status_ = stream_.Start();
      if (!loop_status_.ok()) {
        return loop_status_;
      }
    }

    loop_ = std::make_unique<thread::Fiber>([this]() { RunLoop(); });
    return absl::OkStatus();
  }

  absl::Status Send(const std::string& message) {
    return stream_.WriteText(message);
  }

  void Cancel() {
    concurrency::MutexLock lock(&mutex_);
    stream_.Close().IgnoreError();
    loop_->Cancel();
    loop_status_ = absl::CancelledError("WebsocketEvergreenServer cancelled");
  }

  void Join() {
    if (loop_ != nullptr) {
      loop_->Join();
      loop_ = nullptr;
    }
  }

 private:
  void RunLoop() {
    std::string message;
    boost::json::value parsed_message;
    absl::Status status;

    while (!thread::Cancelled()) {
      message.clear();

      status = stream_.ReadText(&message);
      if (!status.ok()) {
        break;
      }

      boost::system::error_code error;
      parsed_message = boost::json::parse(message, error);
      if (error) {
        LOG(ERROR) << "WebsocketEvergreenServer parse() failed: "
                   << error.message();
        continue;
      }

      std::string client_id;
      if (auto id_ptr = parsed_message.find_pointer("/id", error);
          id_ptr == nullptr || error) {
        LOG(ERROR) << "WebsocketEvergreenServer no 'id' field in message: "
                   << message;
        continue;
      } else {
        client_id = id_ptr->as_string().c_str();
      }

      std::string type;
      if (auto type_ptr = parsed_message.find_pointer("/type", error);
          type_ptr == nullptr || error) {
        LOG(ERROR) << "WebsocketEvergreenServer no 'type' field in message: "
                   << message;
        continue;
      } else {
        type = type_ptr->as_string().c_str();
      }

      if (type != "offer" && type != "candidate" && type != "answer") {
        LOG(ERROR) << "WebsocketEvergreenServer unknown message type: " << type
                   << " in message: " << message;
        continue;
      }

      if (type == "offer" && on_offer_) {
        on_offer_(client_id, std::move(parsed_message));
        continue;
      }

      if (type == "candidate" && on_candidate_) {
        on_candidate_(client_id, std::move(parsed_message));
        continue;
      }

      if (type == "answer" && on_answer_) {
        on_answer_(client_id, std::move(parsed_message));
        continue;
      }
    }
    {
      concurrency::MutexLock lock(&mutex_);
      loop_status_ = status;
      if (!loop_status_.ok()) {
        error_event_.Notify();
      }
    }
  }

  void CloseStreamAndJoinLoop() {
    stream_.Close().IgnoreError();
    if (loop_ != nullptr) {
      loop_->Cancel();
      loop_->Join();
      loop_ = nullptr;
    }
  }

  std::string identity_;
  const std::string address_;
  const uint16_t port_;

  PeerJsonHandler on_offer_;
  PeerJsonHandler on_candidate_;
  PeerJsonHandler on_answer_;

  FiberAwareWebsocketStream stream_;
  std::unique_ptr<thread::Fiber> loop_;
  absl::Status loop_status_ ABSL_GUARDED_BY(mutex_);
  mutable concurrency::Mutex mutex_;
  concurrency::PermanentEvent error_event_;
};

absl::StatusOr<WebRtcDataChannelConnection> AcceptWebRtcDataChannel(
    std::string_view identity, std::string_view signalling_address,
    uint16_t signalling_port) {
  SignallingClient signalling_client{signalling_address, signalling_port};

  std::string client_id;

  auto config = GetDefaultRtcConfig();
  config.enableIceUdpMux = true;
  auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

  std::shared_ptr<rtc::DataChannel> data_channel;
  concurrency::PermanentEvent data_channel_event;

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
          LOG(ERROR) << "WebRtcEvergreenWireStream no 'description' field in "
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
      LOG(ERROR) << "WebRtcEvergreenWireStream no 'candidate' field in "
                    "candidate message: "
                 << boost::json::serialize(message);
    }
  });

  if (auto status = signalling_client.ConnectWithIdentity(identity);
      !status.ok()) {
    return status;
  }

  connection->onLocalDescription([&signalling_client, &client_id](
                                     const rtc::Description& description) {
    const std::string sdp = description.generateSdp("\r\n");

    boost::json::object answer;
    answer["id"] = client_id;
    answer["type"] = "answer";
    answer["description"] = sdp;

    const auto message = boost::json::serialize(answer);
    if (const auto status = signalling_client.Send(message); !status.ok()) {
      LOG(ERROR) << "WebRtcEvergreenWireStream Send answer failed: " << status;
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
          LOG(ERROR) << "WebRtcEvergreenWireStream Send candidate failed: "
                     << status;
        }
      });

  connection->onDataChannel(
      [&data_channel,
       &data_channel_event](const std::shared_ptr<rtc::DataChannel>& channel) {
        data_channel = channel;
        data_channel_event.Notify();
      });

  const int selected = concurrency::Select({data_channel_event.OnEvent(),
                                            signalling_client.OnError(),
                                            concurrency::OnCancel()});

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
  if (concurrency::Cancelled()) {
    return absl::CancelledError(
        "WebRtcEvergreenWireStream connection cancelled");
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
          LOG(ERROR) << "WebRtcEvergreenWireStream no 'description' field in "
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
      LOG(ERROR) << "WebRtcEvergreenWireStream no 'candidate' field in "
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
          LOG(ERROR) << "WebRtcEvergreenWireStream Send candidate failed: "
                     << status;
        }
      });

  auto init = rtc::DataChannelInit{};
  init.reliability.unordered = true;
  auto data_channel =
      connection->createDataChannel(std::string(identity), std::move(init));

  concurrency::PermanentEvent opened;
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
      LOG(ERROR) << "WebRtcEvergreenWireStream Send offer failed: " << status;
      return status;
    }
  }

  const int selected = concurrency::Select(
      {opened.OnEvent(), signalling_client.OnError(), concurrency::OnCancel()});
  if (selected == 1) {
    return signalling_client.GetStatus();
  }
  if (concurrency::Cancelled()) {
    return absl::CancelledError(
        "WebRtcEvergreenWireStream connection cancelled");
  }

  data_channel->onOpen({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (concurrency::Cancelled()) {
    return absl::CancelledError(
        "WebRtcEvergreenWireStream connection cancelled");
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
  concurrency::MutexLock lock(&mutex_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
}

void WebRtcEvergreenServer::Run() {
  concurrency::MutexLock lock(&mutex_);
  main_loop_ = concurrency::NewTree({}, [this]() { RunLoop(); });
}

absl::Status WebRtcEvergreenServer::CancelInternal()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
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
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcEvergreenServer Join called on either unstarted or already "
        "joined server.");
  }

  const std::unique_ptr<concurrency::Fiber> main_loop = std::move(main_loop_);
  main_loop_ = nullptr;

  mutex_.Unlock();
  main_loop->Join();
  mutex_.Lock();

  return absl::OkStatus();
}

void WebRtcEvergreenServer::RunLoop() {
  DLOG(INFO) << "WebRtcEvergreenServer RunLoop starting.";
  DataChannelConnectionMap connections;
  auto signalling_client =
      InitSignallingClient(signalling_address_, signalling_port_, &connections);

  concurrency::MutexLock lock(&mutex_);
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
    mutex_.Unlock();
    const int selected = concurrency::Select(
        {channel_reader->OnRead(&next_connection, &channel_open),
         signalling_client->OnError(), concurrency::OnCancel()});
    mutex_.Lock();

    // Check if our fiber has been cancelled, which means we should stop.
    if (concurrency::Cancelled()) {
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
      mutex_.Unlock();
      concurrency::SleepFor(absl::Seconds(0.5));
      mutex_.Lock();
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

    auto stream = std::make_unique<WebRtcEvergreenWireStream>(
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
          concurrency::MutexLock lock(&mutex_);
          const auto map_node = connections->extract(peer_id);
          CHECK(!map_node.empty())
              << "WebRtcEvergreenServer no connection for peer: " << peer_id;

          WebRtcDataChannelConnection connection_from_map =
              std::move(map_node.mapped());
          connection_from_map.data_channel = std::move(dc);

          ready_data_connections_.writer()->WriteUnlessCancelled(
              std::move(connection_from_map));
        });

    concurrency::MutexLock lock(&mutex_);
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

absl::StatusOr<std::unique_ptr<WebRtcEvergreenWireStream>>
AcceptStreamFromSignalling(std::string_view identity, std::string_view address,
                           uint16_t port) {

  absl::StatusOr<WebRtcDataChannelConnection> connection =
      AcceptWebRtcDataChannel(identity, address, port);
  if (!connection.ok()) {
    return connection.status();
  }

  return std::make_unique<WebRtcEvergreenWireStream>(
      std::move(connection->data_channel), std::move(connection->connection));
}

absl::StatusOr<std::unique_ptr<WebRtcEvergreenWireStream>>
StartStreamWithSignalling(std::string_view identity,
                          std::string_view peer_identity,
                          std::string_view address, uint16_t port) {

  absl::StatusOr<WebRtcDataChannelConnection> connection =
      StartWebRtcDataChannel(identity, peer_identity, address, port);
  if (!connection.ok()) {
    return connection.status();
  }

  return std::make_unique<WebRtcEvergreenWireStream>(
      std::move(connection->data_channel), std::move(connection->connection));
}

}  // namespace eglt::sdk