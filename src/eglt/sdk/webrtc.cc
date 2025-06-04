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
#include "eglt/sdk/webrtc.h"

namespace eglt::sdk {

namespace asio = boost::asio;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

// void(peer_id, message)
using PeerJsonHandler =
    std::function<void(std::string_view, boost::json::value)>;

static constexpr int kMaxMessageSize = 10 * 1024 * 1024;  // 10 MiB
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
  explicit SignallingClient(std::string_view address = "demos.helena.direct",
                            uint16_t port = 19000)
      : address_(address),
        port_(port),
        stream_(GetDefaultAsioExecutionContext()) {}

  SignallingClient(const SignallingClient&) = delete;
  SignallingClient& operator=(const SignallingClient&) = delete;

  ~SignallingClient() {
    concurrency::MutexLock lock(&mutex_);
    while (write_pending_) {
      cv_.Wait(&mutex_);
    }
    CloseStreamAndJoinLoop();
  }

  void OnOffer(PeerJsonHandler on_offer) {
    concurrency::MutexLock lock(&mutex_);
    on_offer_ = std::move(on_offer);
  }

  void OnCandidate(PeerJsonHandler on_candidate) {
    concurrency::MutexLock lock(&mutex_);
    on_candidate_ = std::move(on_candidate);
  }

  void OnAnswer(PeerJsonHandler on_answer) {
    concurrency::MutexLock lock(&mutex_);
    on_answer_ = std::move(on_answer);
  }

  absl::Status ConnectWithIdentity(std::string_view identity) {
    concurrency::MutexLock lock(&mutex_);

    if (!on_answer_ && !on_offer_ && !on_candidate_) {
      return absl::FailedPreconditionError(
          "WebsocketEvergreenServer no handlers set: connecting in this "
          "state would lose messages");
    }

    identity_ = std::string(identity);

    boost::system::error_code error;
    stream_.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::request_type& req) {
          req.set(beast::http::field::user_agent,
                  "Action Engine / Evergreen Light 0.1.0 "
                  "WebsocketEvergreenWireStream client");
        }));

    tcp::resolver resolver(*GetDefaultAsioExecutionContext());
    const auto endpoints =
        resolver.resolve(address_, std::to_string(port_), error);
    if (error) {
      return absl::InternalError(absl::StrFormat(
          "WebsocketEvergreenServer resolve() failed: %s", error.message()));
    }

    const tcp::endpoint endpoint =
        asio::connect(beast::get_lowest_layer(stream_), endpoints, error);
    if (error) {
      return absl::InternalError(absl::StrFormat(
          "WebsocketEvergreenServer connect() failed: %s", error.message()));
    }

    stream_.handshake(absl::StrFormat("%s:%d", address_, endpoint.port()),
                      absl::StrFormat("/%s", identity_), error);

    if (error) {
      loop_status_ = absl::InternalError(absl::StrFormat(
          "WebsocketEvergreenServer handshake() failed: %s", error.message()));
    }
    cv_.SignalAll();
    if (loop_status_ != absl::OkStatus()) {
      return loop_status_;
    }

    loop_ = std::make_unique<thread::Fiber>([this]() { RunLoop(); });
    return absl::OkStatus();
  }

  absl::Status Send(const std::string& message) {
    concurrency::MutexLock lock(&mutex_);

    while (!stream_.is_open() && loop_status_.ok() &&
           !concurrency::Cancelled()) {
      cv_.Wait(&mutex_);
    }
    if (!loop_status_.ok()) {
      return loop_status_;
    }
    if (concurrency::Cancelled()) {
      return absl::CancelledError("WebsocketEvergreenServer Send cancelled");
    }
    if (!stream_.is_open()) {
      return absl::FailedPreconditionError(
          "WebsocketEvergreenServer stream is not open");
    }

    while (write_pending_) {
      cv_.Wait(&mutex_);
    }

    boost::system::error_code error;
    thread::PermanentEvent write_done;
    write_pending_ = true;

    stream_.text(true);
    stream_.async_write(
        asio::buffer(message),
        [&error, &write_done, this](
            const boost::system::error_code& async_error, std::size_t) {
          concurrency::MutexLock lock(&mutex_);
          error = async_error;
          write_pending_ = false;
          cv_.SignalAll();
          write_done.Notify();
        });

    mutex_.Unlock();
    thread::Select({write_done.OnEvent(), thread::OnCancel()});
    mutex_.Lock();

    if (error) {
      return absl::InternalError(absl::StrFormat(
          "WebsocketEvergreenServer Send failed: %s", error.message()));
    }

    return absl::OkStatus();
  }

  void Cancel() {
    concurrency::MutexLock lock(&mutex_);
    CloseStreamAndJoinLoop();
    loop_status_ = absl::CancelledError("WebsocketEvergreenServer cancelled");
    cv_.SignalAll();
  }

  void Join() {
    concurrency::MutexLock lock(&mutex_);
    if (loop_ != nullptr) {
      concurrency::Fiber* loop = loop_.get();
      mutex_.Unlock();
      loop->Join();
      mutex_.Lock();
      loop_ = nullptr;
    }
  }

 private:
  void RunLoop() {
    boost::system::error_code error;
    std::string message;
    boost::json::value parsed_message;
    absl::Status status;

    concurrency::MutexLock lock(&mutex_);

    while (!thread::Cancelled()) {
      message.clear();

      asio::dynamic_string_buffer buffer(message);
      thread::PermanentEvent read_done;
      stream_.async_read(
          buffer,
          [&error, &read_done](const boost::system::error_code& async_error,
                               std::size_t) {
            error = async_error;
            read_done.Notify();
          });

      mutex_.Unlock();
      thread::Select({read_done.OnEvent()});
      mutex_.Lock();

      if (error) {
        if (error == beast::websocket::error::closed ||
            error == boost::system::errc::operation_canceled) {
          break;
        }
        status = absl::InternalError(absl::StrFormat(
            "WebsocketEvergreenServer read() failed: %s", error.message()));
        break;
      }

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
    loop_status_ = status;
  }

  void CloseStreamAndJoinLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    boost::system::error_code error;
    if (stream_.is_open()) {
      stream_.next_layer().cancel(error);
    }
    if (error) {
      LOG(ERROR) << "WebsocketEvergreenServer CloseStreamAndJoinLoop cancel "
                    "failed: "
                 << error.message();
    }
    if (loop_ != nullptr) {
      loop_->Cancel();
      concurrency::Fiber* loop = loop_.get();
      mutex_.Unlock();
      loop->Join();
      mutex_.Lock();
      loop_ = nullptr;
    }
  }

  mutable concurrency::Mutex mutex_;
  concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);

  std::string identity_ ABSL_GUARDED_BY(mutex_);
  const std::string address_;
  const uint16_t port_;

  PeerJsonHandler on_offer_ ABSL_GUARDED_BY(mutex_);
  PeerJsonHandler on_candidate_ ABSL_GUARDED_BY(mutex_);
  PeerJsonHandler on_answer_ ABSL_GUARDED_BY(mutex_);

  beast::websocket::stream<tcp::socket> stream_ ABSL_GUARDED_BY(mutex_);
  std::unique_ptr<thread::Fiber> loop_ ABSL_GUARDED_BY(mutex_);
  absl::Status loop_status_ ABSL_GUARDED_BY(mutex_);

  bool write_pending_ ABSL_GUARDED_BY(mutex_) = false;
};

std::unique_ptr<WebRtcEvergreenWireStream> AcceptStreamFromSignalling(
    std::string_view address, uint16_t port) {
  SignallingClient signalling_client{address, port};

  thread::Mutex mutex;
  thread::CondVar cv;

  std::string client_id;

  auto config = GetDefaultRtcConfig();
  config.enableIceUdpMux = true;
  auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

  std::shared_ptr<rtc::DataChannel> data_channel;

  connection->onLocalDescription(
      [&signalling_client, &client_id](const rtc::Description& description) {
        const std::string sdp = description.generateSdp("\r\n");

        boost::json::object answer;
        answer["id"] = client_id;
        answer["type"] = "answer";
        answer["description"] = sdp;

        const auto message = boost::json::serialize(answer);
        signalling_client.Send(message).IgnoreError();
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
        signalling_client.Send(message).IgnoreError();
      });

  connection->onDataChannel(
      [&data_channel, &cv,
       &mutex](const std::shared_ptr<rtc::DataChannel>& channel) {
        concurrency::MutexLock lock(&mutex);
        data_channel = channel;
        cv.SignalAll();
      });

  signalling_client.OnOffer(
      [&connection, &mutex, &client_id](std::string_view id,
                                        const boost::json::value& message) {
        concurrency::MutexLock lock(&mutex);

        if (!client_id.empty() && client_id != id) {
          LOG(ERROR) << "Already accepting another client: " << client_id;
          return;
        }
        client_id = std::string(id);

        boost::system::error_code error;

        if (const auto desc_ptr = message.find_pointer("/description", error);
            desc_ptr != nullptr && !error) {
          const auto description = desc_ptr->as_string().c_str();
          mutex.Unlock();
          connection->setRemoteDescription(rtc::Description(description));
          mutex.Lock();
        } else {
          LOG(ERROR) << "WebRtcEvergreenWireStream no 'description' field in "
                        "offer: "
                     << boost::json::serialize(message);
        }
      });

  signalling_client.OnCandidate([&connection, &mutex, &client_id](
                                    std::string_view id,
                                    const boost::json::value& message) {
    concurrency::MutexLock lock(&mutex);

    if (!client_id.empty() || client_id != id) {
      // LOG(ERROR) << "Received candidate for unknown client: " << id;
      return;
    }

    boost::system::error_code error;

    if (const auto candidate_ptr = message.find_pointer("/candidate", error);
        candidate_ptr != nullptr && !error) {
      const auto candidate_str = candidate_ptr->as_string().c_str();
      mutex.Unlock();
      connection->addRemoteCandidate(rtc::Candidate(candidate_str));
      mutex.Lock();
    } else {
      LOG(ERROR) << "WebRtcEvergreenWireStream no 'candidate' field in "
                    "candidate message: "
                 << boost::json::serialize(message);
    }
  });

  signalling_client.ConnectWithIdentity("server").IgnoreError();

  while (data_channel == nullptr && !concurrency::Cancelled()) {
    concurrency::MutexLock lock(&mutex);
    cv.Wait(&mutex);
  }

  // Callbacks need to be cleaned up before returning, because they use
  // local variables that will be destroyed when this function returns.
  connection->onLocalCandidate({});
  connection->onLocalDescription({});
  connection->onDataChannel({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (concurrency::Cancelled()) {
    LOG(ERROR) << "WebRtcEvergreenWireStream connection cancelled";
    return nullptr;
  }

  return std::make_unique<WebRtcEvergreenWireStream>(std::move(data_channel),
                                                     std::move(connection));
}

std::unique_ptr<WebRtcEvergreenWireStream> StartStreamWithSignalling(
    std::string_view id, std::string_view peer_id, std::string_view address,
    uint16_t port) {
  SignallingClient signalling_client{address, port};

  rtc::Configuration config = GetDefaultRtcConfig();
  config.portRangeBegin = 1025;
  config.portRangeEnd = 65535;

  auto connection = std::make_unique<rtc::PeerConnection>(std::move(config));

  connection->onLocalCandidate(
      [id = std::string(id), peer_id = std::string(peer_id),
       &signalling_client](const rtc::Candidate& candidate) {
        const auto candidate_str = std::string(candidate);
        boost::json::object candidate_json;
        candidate_json["id"] = peer_id;
        candidate_json["type"] = "candidate";
        candidate_json["candidate"] = candidate_str;
        candidate_json["mid"] = candidate.mid();

        const auto message = boost::json::serialize(candidate_json);
        signalling_client.Send(message).IgnoreError();
      });

  auto init = rtc::DataChannelInit{};
  init.reliability.unordered = true;
  auto data_channel =
      connection->createDataChannel(std::string(id), std::move(init));

  thread::Mutex mutex;
  thread::CondVar cv;

  bool opened = false;
  data_channel->onOpen([&opened, &mutex, &cv]() {
    LOG(INFO) << "WebsocketEvergreenServer data channel opened.";
    concurrency::MutexLock lock(&mutex);
    opened = true;
    cv.SignalAll();
  });

  signalling_client.OnAnswer([&connection, peer_id = std::string(peer_id)](
                                 std::string_view received_peer_id,
                                 const boost::json::value& message) {
    if (received_peer_id != peer_id) {
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

  signalling_client.OnCandidate([&connection, peer_id = std::string(peer_id)](
                                    std::string_view received_peer_id,
                                    const boost::json::value& message) {
    if (received_peer_id != peer_id) {
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

  signalling_client.ConnectWithIdentity(id).IgnoreError();

  // Send connection offer to the server.
  {
    auto description = connection->createOffer();
    auto sdp = description.generateSdp("\r\n");

    boost::json::object offer;
    offer["id"] = peer_id;
    offer["type"] = "offer";
    offer["description"] = sdp;
    const auto message = boost::json::serialize(offer);
    signalling_client.Send(message).IgnoreError();
  }

  concurrency::MutexLock lock(&mutex);
  while (!opened && !concurrency::Cancelled()) {
    cv.Wait(&mutex);
  }

  data_channel->onOpen({});

  signalling_client.Cancel();
  signalling_client.Join();

  if (concurrency::Cancelled()) {
    LOG(ERROR) << "WebRtcEvergreenWireStream connection cancelled";
    return nullptr;
  }

  return std::make_unique<WebRtcEvergreenWireStream>(std::move(data_channel),
                                                     std::move(connection));
}

}  // namespace eglt::sdk