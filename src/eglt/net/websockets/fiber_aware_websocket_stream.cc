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

#include "eglt/net/websockets/fiber_aware_websocket_stream.h"

#include "eglt/util/boost_asio_utils.h"

namespace eglt::net {

static constexpr absl::Duration kDebugWarningTimeout = absl::Seconds(5);

absl::Status PrepareClientStream(BoostWebsocketStream* stream) {
  stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine / ActionEngine Light 0.1.0 "
                "WebsocketWireStream client");
      }));
  stream->write_buffer_bytes(16);

  stream->set_option(boost::beast::websocket::stream_base::timeout{
      std::chrono::seconds(30), std::chrono::seconds(1800), true});

  boost::beast::websocket::permessage_deflate permessage_deflate_option;
  permessage_deflate_option.msg_size_threshold = 1024;  // 1 KiB
  permessage_deflate_option.server_enable = true;
  permessage_deflate_option.client_enable = true;
  stream->set_option(permessage_deflate_option);

  return absl::OkStatus();
}

absl::Status PrepareServerStream(BoostWebsocketStream* stream) {
  stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine / ActionEngine Light 0.1.0 "
                "WebsocketWireStream server");
      }));
  stream->write_buffer_bytes(16);

  stream->set_option(boost::beast::websocket::stream_base::timeout{
      std::chrono::seconds(30), std::chrono::seconds(1800), true});

  boost::beast::websocket::permessage_deflate permessage_deflate_option;
  permessage_deflate_option.msg_size_threshold = 1024;  // 1 KiB
  permessage_deflate_option.server_enable = true;
  permessage_deflate_option.client_enable = true;
  stream->set_option(permessage_deflate_option);

  return absl::OkStatus();
}

FiberAwareWebsocketStream::FiberAwareWebsocketStream(
    std::unique_ptr<BoostWebsocketStream> stream,
    PerformHandshakeFn handshake_fn)
    : stream_(std::move(stream)), handshake_fn_(std::move(handshake_fn)) {}

FiberAwareWebsocketStream::FiberAwareWebsocketStream(
    FiberAwareWebsocketStream&& other) noexcept {
  eglt::MutexLock lock(&other.mu_);

  bool debug_warning_logged = false;
  while (other.write_pending_ || other.read_pending_) {
    other.cv_.WaitWithTimeout(&other.mu_, kDebugWarningTimeout);
    if (!debug_warning_logged) {
      LOG(WARNING) << "FiberAwareWebsocketStream move constructor waiting for "
                      "pending operations to finish. You should not move a "
                      "stream while it has pending operations.";
      debug_warning_logged = true;
    }
  }
  stream_ = std::move(other.stream_);
  handshake_fn_ = std::move(other.handshake_fn_);
}

FiberAwareWebsocketStream& FiberAwareWebsocketStream::operator=(
    FiberAwareWebsocketStream&& other) noexcept {
  if (this == &other) {
    return *this;  // Handle self-assignment
  }

  concurrency::TwoMutexLock lock(&mu_, &other.mu_);

  bool other_debug_warning_logged = false;
  while (other.write_pending_ || other.read_pending_) {
    other.cv_.WaitWithTimeout(&other.mu_, kDebugWarningTimeout);
    if (!other_debug_warning_logged) {
      LOG(WARNING)
          << "FiberAwareWebsocketStream move assignment waiting for "
             "pending operations to finish. You should not move from a "
             "stream while it has pending operations.";
      other_debug_warning_logged = true;
    }
  }

  CloseInternal().IgnoreError();

  bool debug_warning_logged = false;
  while (write_pending_ || read_pending_) {
    cv_.WaitWithTimeout(&mu_, kDebugWarningTimeout);
    if (!debug_warning_logged) {
      LOG(WARNING)
          << "FiberAwareWebsocketStream move assignment waiting for "
             "pending operations to finish. You should not move into a "
             "stream while it has pending operations.";
      debug_warning_logged = true;
    }
  }

  stream_ = std::move(other.stream_);
  handshake_fn_ = std::move(other.handshake_fn_);
  other.stream_ = nullptr;  // Ensure the moved-from object is empty

  return *this;
}

FiberAwareWebsocketStream::~FiberAwareWebsocketStream() {
  eglt::MutexLock lock(&mu_);

  bool timeout_logged = false;
  while (write_pending_ || read_pending_) {
    cv_.WaitWithTimeout(&mu_, kDebugWarningTimeout);
    if ((write_pending_ || read_pending_) && !timeout_logged) {
      LOG(WARNING) << "FiberAwareWebsocketStream destructor waiting for "
                      "pending operations to finish. You may have "
                      "forgotten to call Close() on the stream.";
      timeout_logged = true;
    }
  }

  if (stream_ == nullptr) {
    return;  // Stream already closed or moved
  }

  CloseInternal()
      .IgnoreError();  // Close the stream gracefully, ignoring errors
}

absl::StatusOr<FiberAwareWebsocketStream> FiberAwareWebsocketStream::Connect(
    std::string_view address, uint16_t port, std::string_view target,
    PrepareStreamFn prepare_stream_fn) {
  return Connect(*util::GetDefaultAsioExecutionContext(), address, port, target,
                 std::move(prepare_stream_fn));
}

BoostWebsocketStream& FiberAwareWebsocketStream::GetStream() const {
  return *stream_;
}

absl::Status FiberAwareWebsocketStream::Write(
    const std::vector<uint8_t>& message_bytes) noexcept {
  eglt::MutexLock lock(&mu_);

  while (write_pending_) {
    cv_.Wait(&mu_);
  }
  if (!stream_->is_open()) {
    return absl::FailedPreconditionError(
        "Websocket stream is not open for writing");
  }

  write_pending_ = true;

  boost::system::error_code error;
  thread::PermanentEvent write_done;
  stream_->binary(true);
  stream_->async_write(
      boost::asio::buffer(message_bytes),
      [&error, &write_done](const boost::system::error_code& ec, std::size_t) {
        error = ec;
        write_done.Notify();
      });

  mu_.Unlock();
  thread::Select({write_done.OnEvent()});
  mu_.Lock();
  write_pending_ = false;
  cv_.SignalAll();

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_send,
                                     error);
    }

    return absl::CancelledError("WsWrite cancelled");
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsWrite cancelled");
  }

  if (error) {
    LOG(ERROR) << absl::StrFormat("Cannot write to websocket stream: %v",
                                  error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

// TODO: This is absolutely the same as the previous Write method,
//       consider refactoring to avoid code duplication.
absl::Status FiberAwareWebsocketStream::WriteText(
    const std::string& message) noexcept {
  eglt::MutexLock lock(&mu_);

  while (write_pending_) {
    cv_.Wait(&mu_);
  }
  if (!stream_->is_open()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "Websocket stream is not open for writing. Message: %s", message));
  }

  write_pending_ = true;

  boost::system::error_code error;
  thread::PermanentEvent write_done;
  stream_->async_write(
      boost::asio::buffer(message),
      [&error, &write_done](const boost::system::error_code& ec, std::size_t) {
        error = ec;
        write_done.Notify();
      });

  mu_.Unlock();
  thread::Select({write_done.OnEvent()});
  mu_.Lock();
  write_pending_ = false;
  cv_.SignalAll();

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_send,
                                     error);
    }
    return absl::CancelledError("WsWrite cancelled");
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsWrite cancelled");
  }

  if (error) {
    LOG(ERROR) << absl::StrFormat("Cannot write to websocket stream: %v",
                                  error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Close() const noexcept {
  eglt::MutexLock lock(&mu_);
  return CloseInternal();
}

absl::Status FiberAwareWebsocketStream::CloseInternal() const noexcept
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  // TODO(hpnkv): Find out how to cancel Read to be able to close the stream
  //   w/ a graceful WebSocket close code.

  // Already closed or moved
  if (stream_ == nullptr) {
    return absl::OkStatus();
  }

  if (!stream_->is_open()) {
    return absl::OkStatus();
  }

  boost::system::error_code error;

  thread::PermanentEvent close_done;
  stream_->async_close(
      boost::beast::websocket::close_code::normal,
      [&error, &close_done](const boost::system::error_code& async_error) {
        error = async_error;
        close_done.Notify();
      });

  mu_.Unlock();
  thread::Select({close_done.OnEvent()});
  mu_.Lock();

  return absl::OkStatus();
}

absl::Status ResolveAndConnect(BoostWebsocketStream* stream,
                               std::string_view address, uint16_t port) {
  return ResolveAndConnect(*util::GetDefaultAsioExecutionContext(), stream,
                           address, port);
}

absl::Status DoHandshake(BoostWebsocketStream* stream, std::string_view host,
                         std::string_view target) {
  boost::system::error_code error;
  thread::PermanentEvent handshake_done;
  stream->async_handshake(
      host, target,
      [&error, &handshake_done](const boost::system::error_code& ec) {
        error = ec;
        handshake_done.Notify();
      });

  thread::Select({handshake_done.OnEvent()});
  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsHandshake cancelled");
  }
  if (error) {
    return absl::InternalError(error.message());
  }
  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Accept() const noexcept {
  eglt::MutexLock lock(&mu_);

  stream_->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::response_type& res) {
        res.set(boost::beast::http::field::server,
                "Action Engine / ActionEngine Light 0.1.0 "
                "WebsocketActionEngineServer");
      }));
  stream_->write_buffer_bytes(16);

  boost::system::error_code error;
  thread::PermanentEvent accept_done;

  stream_->async_accept(
      [&error, &accept_done](const boost::system::error_code& ec) {
        error = ec;
        accept_done.Notify();
      });

  mu_.Unlock();
  thread::Select({accept_done.OnEvent(), thread::OnCancel()});
  mu_.Lock();

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_receive,
                                     error);
      if (error && error != boost::asio::error::not_connected) {
        LOG(ERROR) << absl::StrFormat(
            "Cannot shut down receive on websocket stream: %v",
            error.message());
      }
    }

    return absl::CancelledError("WsAccept cancelled");
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsAccept cancelled");
  }

  if (error) {
    LOG(ERROR) << absl::StrFormat("Cannot accept websocket stream: %v",
                                  error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Read(
    std::vector<uint8_t>* absl_nonnull buffer,
    bool* absl_nullable got_text) noexcept {
  eglt::MutexLock lock(&mu_);

  while (read_pending_) {
    cv_.Wait(&mu_);
  }

  if (!stream_->is_open()) {
    return absl::FailedPreconditionError(
        "Websocket stream is not open for reading");
  }

  read_pending_ = true;

  boost::system::error_code error;
  thread::PermanentEvent read_done;
  std::vector<uint8_t> temp_buffer;
  temp_buffer.reserve(64);  // Reserve some space to avoid some reallocations
  auto dynamic_buffer = boost::asio::dynamic_buffer(temp_buffer);
  stream_->async_read(
      dynamic_buffer,
      [&error, &read_done](const boost::system::error_code& ec, std::size_t) {
        error = ec;
        read_done.Notify();
      });

  mu_.Unlock();
  thread::Select({read_done.OnEvent()});
  mu_.Lock();
  *buffer = std::move(temp_buffer);
  read_pending_ = false;
  cv_.SignalAll();

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_receive,
                                     error);
      if (error && error != boost::asio::error::not_connected) {
        LOG(ERROR) << absl::StrFormat(
            "Cannot shut down receive on websocket stream: %v",
            error.message());
      }
    }
    return absl::CancelledError("WsRead cancelled");
  }

  if (got_text != nullptr) {
    *got_text = stream_->got_text();
  }
  if (!error) {
    return absl::OkStatus();
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsRead cancelled");
  }

  if (error) {
    LOG(ERROR) << absl::StrFormat("Cannot read from websocket stream: %v",
                                  error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::ReadText(
    std::string* absl_nonnull buffer) noexcept {
  bool got_text = false;
  std::vector<uint8_t> temp_buffer;

  if (absl::Status status = Read(&temp_buffer, &got_text); !status.ok()) {
    return status;
  }
  if (!got_text) {
    return absl::FailedPreconditionError(
        "Websocket stream did not receive text data");
  }

  // Convert the received bytes to a string.
  try {
    *buffer = std::string(std::make_move_iterator(temp_buffer.begin()),
                          std::make_move_iterator(temp_buffer.end()));
  } catch (const std::exception& e) {
    LOG(ERROR) << absl::StrFormat(
        "Failed to convert received bytes to string: %s", e.what());
    return absl::InternalError("Failed to convert received bytes to string");
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Start() const noexcept {
  if (handshake_fn_) {
    return handshake_fn_(stream_.get());
  }
  return absl::OkStatus();
}

}  // namespace eglt::net