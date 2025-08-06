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

#include "eglt/net/webrtc/signalling.h"

#include <boost/json/parse.hpp>

#include "eglt/util/boost_asio_utils.h"

namespace eglt::net {

SignallingClient::SignallingClient(std::string_view address, uint16_t port)
    : address_(address), port_(port) {}

SignallingClient::~SignallingClient() {
  eglt::MutexLock lock(&mu_);
  CancelInternal();
  JoinInternal();
}

absl::Status SignallingClient::ConnectWithIdentity(std::string_view identity) {
  eglt::MutexLock l(&mu_);

  if (!on_answer_ && !on_offer_ && !on_candidate_) {
    return absl::FailedPreconditionError(
        "WebsocketEvergreenServer no handlers set: connecting in this "
        "state would lose messages");
  }

  identity_ = std::string(identity);

  auto boost_stream = std::make_unique<BoostWebsocketStream>(
      *util::GetDefaultAsioExecutionContext());

  boost_stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine / Evergreen Light 0.1.0 "
                "WebsocketWireStream client");
      }));
  boost_stream->set_option(boost::beast::websocket::stream_base::timeout(
      boost::beast::websocket::stream_base::timeout::suggested(
          boost::beast::role_type::server)));

  if (absl::Status status =
          ResolveAndConnect(boost_stream.get(), address_, port_);
      !status.ok()) {
    return status;
  }

  stream_ = FiberAwareWebsocketStream(
      std::move(boost_stream), [this](BoostWebsocketStream* stream) {
        return DoHandshake(stream, absl::StrFormat("%s:%d", address_, port_),
                           absl::StrFormat("/%s", identity_));
      });

  loop_status_ = stream_.Start();
  if (!loop_status_.ok()) {
    return loop_status_;
  }

  loop_ = std::make_unique<thread::Fiber>([this]() {
    eglt::MutexLock lock(&mu_);
    RunLoop();
  });

  return absl::OkStatus();
}

void SignallingClient::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  std::string message;
  boost::json::value parsed_message;
  absl::Status status;

  while (!thread::Cancelled()) {
    message.clear();

    mu_.Unlock();
    status = stream_.ReadText(&message);
    mu_.Lock();

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

  loop_status_ = status;
  if (!loop_status_.ok()) {
    error_event_.Notify();
  }
}

void SignallingClient::CloseStreamAndJoinLoop()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (loop_ != nullptr) {
    loop_->Cancel();
    loop_->Join();
    loop_ = nullptr;
  }
}
}  // namespace eglt::net