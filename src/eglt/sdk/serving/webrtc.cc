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

#include "eglt/sdk/serving/webrtc.h"

namespace eglt::sdk {

namespace asio = boost::asio;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

static constexpr auto kDefaultStunServer = "stun.l.google.com:19302";

static asio::thread_pool& GetDefaultAsioExecutionContext() {
  static auto* context = new asio::thread_pool(4);
  return *context;
}

class SignallingClient {
 public:
  explicit SignallingClient(std::string_view address = "demos.helena.direct",
                            uint16_t port = 19000,
                            std::string_view target = "/server")
      : stream_(GetDefaultAsioExecutionContext()) {
    boost::system::error_code error;

    tcp::resolver resolver(GetDefaultAsioExecutionContext());
    const auto endpoints =
        resolver.resolve(address, std::to_string(port),
                         boost::asio::ip::resolver_query_base::flags(), error);
    if (error) {
      LOG(FATAL) << "WebsocketEvergreenServer resolve() failed: "
                 << error.message();
      ABSL_ASSUME(false);
    }

    const tcp::endpoint endpoint =
        asio::connect(beast::get_lowest_layer(stream_), endpoints, error);
    if (error) {
      LOG(FATAL) << "WebsocketEvergreenServer connect() failed: "
                 << error.message();
      ABSL_ASSUME(false);
    }

    stream_.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::request_type& req) {
          req.set(beast::http::field::user_agent,
                  "Action Engine / Evergreen Light 0.1.0 "
                  "WebsocketEvergreenWireStream client");
        }));

    stream_.handshake(absl::StrFormat("%s:%d", address, endpoint.port()),
                      target, error);
    if (error) {
      LOG(FATAL) << "WebsocketEvergreenServer handshake() failed: "
                 << error.message();
      ABSL_ASSUME(false);
    }
  }

  ~SignallingClient() {
    boost::system::error_code error;
    if (stream_.is_open()) {
      stream_.next_layer().cancel(error);
    }
    if (error) {
      LOG(ERROR) << "WebsocketEvergreenServer ~SignallingClient cancel failed: "
                 << error.message();
    }
    if (loop_ != nullptr) {
      loop_->Cancel();
      loop_->Join();
    }
  }

  void OnMessage(std::function<void(boost::json::value)> on_message) {
    if (on_offer_ || on_candidate_) {
      LOG(FATAL)
          << "SignallingClient already has handlers set for offer or candidate";
      ABSL_ASSUME(false);
    }
    on_message_ = std::move(on_message);
  }

  void OnOffer(std::function<void(boost::json::value)> on_offer) {
    if (on_message_) {
      LOG(FATAL)
          << "SignallingClient already has a handler set for general message";
      ABSL_ASSUME(false);
    }
    on_offer_ = std::move(on_offer);
  }

  void OnCandidate(std::function<void(boost::json::value)> on_candidate) {
    if (on_message_) {
      LOG(FATAL)
          << "SignallingClient already has a handler set for general message";
      ABSL_ASSUME(false);
    }
    on_candidate_ = std::move(on_candidate);
  }

  absl::Status Send(std::string message) {
    boost::system::error_code error;
    thread::PermanentEvent write_done;
    stream_.text(true);
    stream_.async_write(
        asio::buffer(message),
        [&error, &write_done](const boost::system::error_code& async_error,
                              std::size_t) {
          error = async_error;
          write_done.Notify();
        });
    thread::Select({write_done.OnEvent()});
    if (error) {
      return absl::InternalError(absl::StrFormat(
          "WebsocketEvergreenServer Send failed: %s", error.message()));
    }
    return absl::OkStatus();
  }

  void Start() {
    if (loop_ != nullptr) {
      LOG(FATAL) << "SignallingClient loop already running";
      ABSL_ASSUME(false);
    }
    loop_ = std::make_unique<thread::Fiber>([this]() { RunLoop(); });
  }

  void Stop() {
    boost::system::error_code error;
    if (stream_.is_open()) {
      stream_.next_layer().cancel(error);
    }

    if (error) {
      LOG(ERROR) << "WebsocketEvergreenServer Stop cancel failed: "
                 << error.message();
    }
    if (loop_ != nullptr) {
      loop_->Cancel();
      loop_->Join();
      loop_ = nullptr;
    }
  }

 private:
  void RunLoop() {
    boost::system::error_code error;
    std::string message;

    while (!thread::Cancelled()) {
      asio::dynamic_string_buffer buffer(message);
      thread::PermanentEvent read_done;
      stream_.async_read(
          buffer,
          [&error, &read_done](const boost::system::error_code& async_error,
                               std::size_t) {
            error = async_error;
            read_done.Notify();
          });
      thread::Select({read_done.OnEvent()});
      if (error) {
        if (error == beast::websocket::error::closed ||
            error == boost::system::errc::operation_canceled) {
          LOG(INFO) << "WebsocketEvergreenServer connection closed.";
          return;
        }
        LOG(FATAL) << "WebsocketEvergreenServer read() failed: "
                   << error.message();
        ABSL_ASSUME(false);
      }

      auto json = boost::json::parse(message);
      message.clear();

      if (std::string type = json.at("type").as_string().c_str();
          type == "offer") {
        std::string description = json.at("description").as_string().c_str();
        if (on_offer_) {
          on_offer_(std::move(json));
        }
      } else if (type == "candidate") {
        std::string candidate = json.at("candidate").as_string().c_str();
        if (on_candidate_) {
          on_candidate_(std::move(json));
        }
      } else {
        LOG(ERROR) << "Unknown or unsupported message type: " << type;
      }

      if (on_message_) {
        on_message_(std::move(json));
      }
    }
  }
  beast::websocket::stream<tcp::socket> stream_;
  std::unique_ptr<thread::Fiber> loop_;
  std::function<void(boost::json::value)> on_message_;
  std::function<void(boost::json::value)> on_offer_;
  std::function<void(boost::json::value)> on_candidate_;
};

std::unique_ptr<WebRtcEvergreenWireStream> AcceptStreamFromSignalling(
    std::string_view address, uint16_t port, std::string_view target,
    uint16_t rtc_port) {
  rtc::Configuration config;
  config.maxMessageSize = 10 * 1024 * 1024;
  config.portRangeBegin = rtc_port;
  config.portRangeEnd = rtc_port;
  config.iceServers.emplace_back("stun.l.google.com:19302");

  auto connection = std::make_unique<rtc::PeerConnection>(config);

  thread::CondVar cv;
  thread::Mutex mutex;
  thread::MutexLock lock(&mutex);
  std::shared_ptr<rtc::DataChannel> data_channel;
  connection->onDataChannel(
      [&data_channel, &cv](std::shared_ptr<rtc::DataChannel> dc) {
        std::cout << "[Got a DataChannel with label: " << dc->label() << "]"
                  << std::endl;
        data_channel = std::move(dc);
        cv.SignalAll();
      });

  SignallingClient signalling_client{address, port, target};

  std::string client_id;

  signalling_client.OnOffer([&connection,
                             &client_id](const boost::json::value& offer) {
    const std::string description = offer.at("description").as_string().c_str();
    client_id = offer.at("id").as_string().c_str();
    connection->setRemoteDescription(rtc::Description(description));
  });

  signalling_client.OnCandidate(
      [&connection](const boost::json::value& candidate) {
        const std::string candidate_str =
            candidate.at("candidate").as_string().c_str();
        connection->addRemoteCandidate(candidate_str);
      });

  connection->onLocalDescription(
      [&client_id, &signalling_client](const rtc::Description& description) {
        const std::string sdp = description.generateSdp();

        boost::json::object answer;
        answer["id"] = client_id;
        answer["type"] = "answer";
        answer["description"] = sdp;

        auto message = boost::json::serialize(answer);
        signalling_client.Send(std::move(message)).IgnoreError();
      });

  connection->onLocalCandidate(
      [&client_id, &signalling_client](const rtc::Candidate& candidate) {
        const auto candidate_str = std::string(candidate);

        boost::json::object candidate_json;
        candidate_json["id"] = client_id;
        candidate_json["type"] = "candidate";
        candidate_json["candidate"] = candidate_str;
        candidate_json["mid"] = candidate.mid();

        auto message = boost::json::serialize(candidate_json);
        signalling_client.Send(std::move(message)).IgnoreError();
      });

  signalling_client.Start();
  while (data_channel == nullptr) {
    LOG(INFO) << "Waiting for DataChannel..." << std::endl;
    cv.Wait(&mutex);
  }
  signalling_client.Stop();

  return std::make_unique<WebRtcEvergreenWireStream>(std::move(connection),
                                                     std::move(data_channel));
}

}  // namespace eglt::sdk