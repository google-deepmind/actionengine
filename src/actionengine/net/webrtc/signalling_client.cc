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

#include <absl/strings/str_format.h>
#include <absl/time/time.h>
#include <boost/json/parse.hpp>

#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/util/boost_asio_utils.h"
#include "boost/asio/detail/impl/kqueue_reactor.hpp"
#include "boost/asio/detail/impl/reactive_socket_service_base.ipp"
#include "boost/asio/detail/impl/service_registry.hpp"
#include "boost/asio/execution/context_as.hpp"
#include "boost/asio/execution/prefer_only.hpp"
#include "boost/asio/impl/any_io_executor.ipp"
#include "boost/asio/impl/execution_context.hpp"
#include "boost/asio/impl/thread_pool.hpp"
#include "boost/asio/thread_pool.hpp"
#include "boost/beast/core/detail/config.hpp"
#include "boost/beast/core/detail/type_traits.hpp"
#include "boost/beast/core/impl/saved_handler.ipp"
#include "boost/beast/core/impl/string.ipp"
#include "boost/beast/core/role.hpp"
#include "boost/beast/http/field.hpp"
#include "boost/beast/http/fields.hpp"
#include "boost/beast/http/impl/fields.hpp"
#include "boost/beast/http/message.hpp"
#include "boost/beast/websocket/detail/service.ipp"
#include "boost/beast/websocket/impl/stream.hpp"
#include "boost/beast/websocket/rfc6455.hpp"
#include "boost/beast/websocket/stream.hpp"
#include "boost/beast/websocket/stream_base.hpp"
#include "boost/intrusive/detail/algo_type.hpp"
#include "boost/intrusive/link_mode.hpp"
#include "boost/json/string.hpp"
#include "boost/json/value.hpp"
#include "boost/move/detail/addressof.hpp"
#include "boost/smart_ptr/make_shared_object.hpp"
#include "boost/system/detail/error_code.hpp"

namespace act::net {

SignallingClient::SignallingClient(std::string_view address, uint16_t port)
    : address_(address),
      port_(port),
      thread_pool_(std::make_unique<boost::asio::thread_pool>(2)) {}

SignallingClient::~SignallingClient() {
  act::MutexLock lock(&mu_);
  CancelInternal();
  JoinInternal();
}

void SignallingClient::ResetCallbacks() {
  act::MutexLock lock(&mu_);
  on_offer_ = nullptr;
  on_candidate_ = nullptr;
  on_answer_ = nullptr;
}

absl::Status SignallingClient::ConnectWithIdentity(std::string_view identity) {
  act::MutexLock l(&mu_);

  if (!on_answer_ && !on_offer_ && !on_candidate_) {
    return absl::FailedPreconditionError(
        "WebsocketActionEngineServer no handlers set: connecting in this "
        "state would lose messages");
  }

  identity_ = std::string(identity);

  auto boost_stream = std::make_unique<BoostWebsocketStream>(*thread_pool_);

  boost_stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine 0.1.3 "
                "WebsocketWireStream client");
      }));
  boost_stream->set_option(boost::beast::websocket::stream_base::timeout(
      boost::beast::websocket::stream_base::timeout::suggested(
          boost::beast::role_type::server)));

  if (absl::Status status =
          ResolveAndConnect(*thread_pool_, boost_stream.get(), address_, port_);
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

  loop_ = thread::NewTree({}, [this]() {
    act::MutexLock lock(&mu_);
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

    mu_.unlock();
    status = stream_.ReadText(absl::InfiniteDuration(), &message);
    mu_.lock();

    if (!status.ok()) {
      break;
    }

    boost::system::error_code error;
    parsed_message = boost::json::parse(message, error);
    if (error) {
      LOG(ERROR) << "WebsocketActionEngineServer parse() failed: "
                 << error.message();
      continue;
    }

    std::string client_id;
    if (auto id_ptr = parsed_message.find_pointer("/id", error);
        id_ptr == nullptr || error) {
      LOG(ERROR) << "WebsocketActionEngineServer no 'id' field in message: "
                 << message;
      continue;
    } else {
      client_id = id_ptr->as_string().c_str();
    }

    std::string type;
    if (auto type_ptr = parsed_message.find_pointer("/type", error);
        type_ptr == nullptr || error) {
      LOG(ERROR) << "WebsocketActionEngineServer no 'type' field in message: "
                 << message;
      continue;
    } else {
      type = type_ptr->as_string().c_str();
    }

    if (type != "offer" && type != "candidate" && type != "answer") {
      LOG(ERROR) << "WebsocketActionEngineServer unknown message type: " << type
                 << " in message: " << message;
      continue;
    }

    if (type == "offer" && on_offer_) {
      mu_.unlock();
      on_offer_(client_id, std::move(parsed_message));
      mu_.lock();
      continue;
    }

    if (type == "candidate" && on_candidate_) {
      mu_.unlock();
      on_candidate_(client_id, std::move(parsed_message));
      mu_.lock();
      continue;
    }

    if (type == "answer" && on_answer_) {
      mu_.unlock();
      on_answer_(client_id, std::move(parsed_message));
      mu_.lock();
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
}  // namespace act::net
