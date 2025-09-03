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

#include "actionengine/net/websockets/websockets.h"

#include <new>
#include <optional>
#include <utility>
#include <vector>

#include <absl/base/optimization.h>
#include <absl/log/log.h>
#include <absl/time/time.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cppack/msgpack.h>

#include "actionengine/data/msgpack.h"
#include "actionengine/util/boost_asio_utils.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"

namespace act::net {

WebsocketWireStream::WebsocketWireStream(
    std::unique_ptr<BoostWebsocketStream> stream, std::string_view id)
    : stream_({std::move(stream)}),
      id_(id.empty() ? GenerateUUID4() : std::string(id)) {
  DLOG(INFO) << absl::StrFormat("WESt %s created", id_);
}

WebsocketWireStream::WebsocketWireStream(FiberAwareWebsocketStream stream,
                                         std::string_view id)
    : stream_(std::move(stream)),
      id_(id.empty() ? GenerateUUID4() : std::string(id)) {}

absl::Status WebsocketWireStream::Send(WireMessage message) {
  act::MutexLock lock(&mu_);

  if (half_closed_) {
    return absl::FailedPreconditionError(
        "WebsocketWireStream is half-closed, cannot send messages");
  }

  if (!status_.ok()) {
    return status_;
  }

  return SendInternal(std::move(message));
}

absl::StatusOr<std::optional<WireMessage>> WebsocketWireStream::Receive(
    absl::Duration timeout) {
  act::MutexLock lock(&mu_);

  if (closed_) {
    return absl::FailedPreconditionError(
        "WebsocketWireStream is closed, cannot receive messages");
  }

  std::vector<uint8_t> buffer;

  // Receive from underlying websocket stream.
  mu_.unlock();
  absl::Status status = stream_.Read(timeout, &buffer);
  mu_.lock();
  if (!status.ok()) {
    return status;
  }

  // Unpack the received data into a WireMessage.
  mu_.unlock();
  absl::StatusOr<WireMessage> unpacked = cppack::Unpack<WireMessage>(buffer);
  mu_.lock();
  if (!unpacked.ok()) {
    return unpacked.status();
  }

  // Empty WireMessage indicates a half-close.
  if (unpacked->actions.empty() && unpacked->node_fragments.empty()) {
    // If the message is empty, it means the stream was half-closed by the
    // other end, or the other end has acknowledged our half-close.

    if (half_closed_) {
      // We initiated the half-close, so we don't need to call the
      // half-close callback, only to acknowledge
      return std::nullopt;
    }

    if (absl::Status half_close_status = HalfCloseInternal();
        !half_close_status.ok()) {
      return half_close_status;
    }

    // If we reach here, it means we have successfully half-closed the stream
    // in response to the other end's half-close message.
    return std::nullopt;
  }

  return *std::move(unpacked);
}

absl::Status WebsocketWireStream::Start() {
  // In this case, the client EG stream is not responsible for handshaking.
  return absl::OkStatus();
}

absl::Status WebsocketWireStream::Accept() {
  DLOG(INFO) << absl::StrFormat("WESt %s Accept()", id_);
  return stream_.Accept();
}

void WebsocketWireStream::HalfClose() {
  act::MutexLock lock(&mu_);
  HalfCloseInternal().IgnoreError();
}

void WebsocketWireStream::Abort() {
  act::MutexLock lock(&mu_);
  if (closed_) {
    return;
  }

  // TODO: communicate an -unsuccessful- close instead.
  HalfCloseInternal().IgnoreError();

  stream_.CancelRead();
  closed_ = true;
  status_ = absl::CancelledError("WebsocketWireStream aborted");
}

absl::Status WebsocketWireStream::SendInternal(WireMessage message) {
  mu_.unlock();
  auto status = stream_.Write(cppack::Pack(std::move(message)));
  mu_.lock();

  return status;
}

absl::Status WebsocketWireStream::HalfCloseInternal() {
  if (half_closed_) {
    return absl::OkStatus();
  }

  half_closed_ = true;
  RETURN_IF_ERROR(SendInternal(WireMessage{}));

  return absl::OkStatus();
}

WebsocketServer::WebsocketServer(act::Service* service,
                                 std::string_view address, uint16_t port)
    : service_(service),
      acceptor_(std::make_unique<boost::asio::ip::tcp::acceptor>(
          *util::GetDefaultAsioExecutionContext())) {
  boost::system::error_code error;

  acceptor_->open(boost::asio::ip::tcp::v4(), error);
  if (error) {
    status_ = absl::InternalError(error.message());
    LOG(FATAL) << "WebsocketServer open() failed: " << status_;
    ABSL_ASSUME(false);
  }

  acceptor_->set_option(boost::asio::ip::tcp::no_delay(true));
  acceptor_->set_option(boost::asio::socket_base::reuse_address(true), error);
  if (error) {
    status_ = absl::InternalError(error.message());
    LOG(FATAL) << "WebsocketServer set_option() failed: " << status_;
    ABSL_ASSUME(false);
  }

  acceptor_->bind(boost::asio::ip::tcp::endpoint(
                      boost::asio::ip::make_address(address), port),
                  error);
  if (error) {
    status_ = absl::InternalError(error.message());
    LOG(FATAL) << "WebsocketServer bind() failed: " << status_;
    ABSL_ASSUME(false);
  }

  acceptor_->listen(boost::asio::socket_base::max_listen_connections, error);
  if (error) {
    status_ = absl::InternalError(error.message());
    LOG(FATAL) << "WebsocketServer listen() failed: " << status_;
    ABSL_ASSUME(false);
  }

  DLOG(INFO) << "WebsocketServer created at " << address << ":" << port;
}

WebsocketServer::~WebsocketServer() {
  act::MutexLock lock(&mu_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
  DLOG(INFO) << "WebsocketServer::~WebsocketServer()";
}

void WebsocketServer::Run() {
  act::MutexLock l(&mu_);

  main_loop_ = thread::NewTree({}, [this]() {
    act::MutexLock lock(&mu_);

    while (!thread::Cancelled()) {
      boost::asio::ip::tcp::socket socket{
          boost::asio::make_strand(*util::GetDefaultAsioExecutionContext())};

      DLOG(INFO) << "WES waiting for connection.";
      boost::system::error_code error;
      thread::PermanentEvent accepted;
      acceptor_->async_accept(
          socket, [&error, &accepted](const boost::system::error_code& ec) {
            error = ec;
            accepted.Notify();
          });

      mu_.unlock();
      thread::Select({accepted.OnEvent(),
                      thread::OnCancel()});  // Wait for accept to complete.
      mu_.lock();

      cancelled_ = thread::Cancelled() ||
                   error == boost::system::errc::operation_canceled ||
                   cancelled_;
      if (cancelled_) {
        DLOG(INFO) << "WebsocketServer canceled and is exiting "
                      "its main loop";
        break;
      }

      if (error) {
        DLOG(ERROR) << "WebsocketServer accept() failed: " << error.message();
        switch (error.value()) {
          case boost::system::errc::operation_canceled:
            status_ = absl::OkStatus();
            break;
          default:
            DLOG(ERROR) << "WebsocketServer accept() failed.";
            status_ = absl::InternalError(error.message());
            break;
        }
        // Any code reaching here means the service is shutting down.
        break;
      }

      mu_.unlock();
      auto stream = std::make_unique<BoostWebsocketStream>(std::move(socket));
      PrepareServerStream(stream.get()).IgnoreError();
      auto connection = service_->EstablishConnection(
          std::make_shared<WebsocketWireStream>(std::move(stream)));
      mu_.lock();

      if (!connection.ok()) {
        status_ = connection.status();
        DLOG(ERROR) << "WebsocketServer EstablishConnection failed: "
                    << status_;
        // continuing here
      }
    }
    acceptor_->close();
  });
}

absl::Status WebsocketServer::Cancel() {
  act::MutexLock lock(&mu_);
  return CancelInternal();
}

absl::Status WebsocketServer::Join() {
  act::MutexLock lock(&mu_);
  return JoinInternal();
}

absl::Status WebsocketServer::CancelInternal() {
  if (cancelled_) {
    return absl::OkStatus();
  }
  cancelled_ = true;
  DLOG(INFO) << "WebsocketServer Cancel()";
  acceptor_->close();
  // util::GetDefaultAsioExecutionContext()->stop();
  main_loop_->Cancel();

  if (boost::system::error_code error; error) {
    status_ = absl::InternalError(error.message());
    return status_;
  }

  return absl::OkStatus();
}

absl::Status WebsocketServer::JoinInternal() {
  while (joining_) {
    join_cv_.Wait(&mu_);
  }
  if (main_loop_ == nullptr) {
    return status_;
  }
  joining_ = true;

  mu_.unlock();
  main_loop_->Join();
  mu_.lock();

  main_loop_ = nullptr;
  joining_ = false;
  join_cv_.SignalAll();

  DLOG(INFO) << "WebsocketServer main_loop_ joined";
  return status_;
}

absl::StatusOr<std::unique_ptr<WebsocketWireStream>> MakeWebsocketWireStream(
    std::string_view address, uint16_t port, std::string_view target,
    std::string_view id, PrepareStreamFn prepare_stream) {

  absl::StatusOr<FiberAwareWebsocketStream> ws_stream =
      FiberAwareWebsocketStream::Connect(
          *util::GetDefaultAsioExecutionContext(), address, port, target,
          std::move(prepare_stream));

  if (!ws_stream.ok()) {
    return ws_stream.status();
  }

  if (absl::Status handshake_status = ws_stream->Start();
      !handshake_status.ok()) {
    return handshake_status;
  }

  std::string session_id = id.empty() ? GenerateUUID4() : std::string(id);
  return std::make_unique<WebsocketWireStream>(*std::move(ws_stream),
                                               session_id);
}

}  // namespace act::net