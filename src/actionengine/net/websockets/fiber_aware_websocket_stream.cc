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

#include "actionengine/net/websockets/fiber_aware_websocket_stream.h"

#include <variant>

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>

#include "actionengine/util/boost_asio_utils.h"
#include "actionengine/util/status_macros.h"

namespace act::net {

static constexpr absl::Duration kDebugWarningTimeout = absl::Seconds(5);

BoostWebsocketStream::BoostWebsocketStream(
    boost::asio::ip::tcp::socket&& socket)
    : stream_(PlainStream(std::move(socket))) {}

BoostWebsocketStream::BoostWebsocketStream(
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket>&& stream)
    : stream_(SslStream(std::move(stream))) {}

void BoostWebsocketStream::binary(bool value) {
  std::visit([value](auto& s) { s.binary(value); }, stream_);
}

void BoostWebsocketStream::text(bool value) {
  std::visit([value](auto& s) { s.text(value); }, stream_);
}

bool BoostWebsocketStream::got_text() const {
  return std::visit([](auto& s) { return s.got_text(); }, stream_);
}

bool BoostWebsocketStream::is_open() const {
  return std::visit([](auto& s) { return s.is_open(); }, stream_);
}

void BoostWebsocketStream::write_buffer_bytes(std::size_t bytes) {
  std::visit([bytes](auto& s) { s.write_buffer_bytes(bytes); }, stream_);
}

boost::asio::ip::tcp::socket& BoostWebsocketStream::next_layer() {
  if (std::holds_alternative<PlainStream>(stream_)) {
    return std::get<PlainStream>(stream_).next_layer();
  }
  return std::get<SslStream>(stream_).next_layer().next_layer();
}

const boost::asio::ip::tcp::socket& BoostWebsocketStream::next_layer() const {
  if (std::holds_alternative<PlainStream>(stream_)) {
    return std::get<PlainStream>(stream_).next_layer();
  }
  return std::get<SslStream>(stream_).next_layer().next_layer();
}

void BoostWebsocketStream::handshake(std::string_view host,
                                     std::string_view target,
                                     boost::system::error_code& ec) {
  std::visit([&](auto& s) { s.handshake(host, target, ec); }, stream_);
}

void BoostWebsocketStream::async_ssl_handshake(
    boost::asio::ssl::stream_base::handshake_type type,
    std::function<void(boost::system::error_code)> cb) {
  if (std::holds_alternative<SslStream>(stream_)) {
    std::get<SslStream>(stream_).next_layer().async_handshake(type,
                                                              std::move(cb));
  } else {
    // should not happen if logic is correct
    cb(boost::asio::error::no_protocol_option);
  }
}

BoostWebsocketStream::SslStream* absl_nullable
BoostWebsocketStream::GetSslStreamOrNull() {
  if (auto* ssl_stream = std::get_if<SslStream>(&stream_)) {
    return ssl_stream;
  }
  return nullptr;
}

absl::Status PrepareClientStream(BoostWebsocketStream* stream) {
  stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine 0.1.7 "
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
                "Action Engine 0.1.7 "
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

absl::StatusOr<WsUrl> WsUrl::FromString(std::string_view url_str) {
  bool use_ssl = false;
  bool explicit_no_ssl = false;

  if (url_str.substr(0, 6) == "wss://") {
    use_ssl = true;
    url_str = url_str.substr(6);
  } else if (url_str.substr(0, 5) == "ws://") {
    url_str = url_str.substr(5);
    explicit_no_ssl = true;
  }

  std::string_view address;
  std::string_view target = "/";
  uint16_t port;
  if (const size_t colon_pos = url_str.find(':');
      colon_pos == std::string_view::npos) {
    address = url_str;
    port = use_ssl ? 443 : 80;
  } else {
    address = url_str.substr(0, colon_pos);
    const size_t port_start = colon_pos + 1;
    const size_t slash_pos = url_str.find('/', port_start);

    std::string_view port_str;
    if (slash_pos == std::string_view::npos) {
      port_str = url_str.substr(port_start);
    } else {
      port_str = url_str.substr(port_start, slash_pos - port_start);
    }

    if (!absl::SimpleAtoi(port_str, &port)) {
      LOG(ERROR)
          << "Failed to parse port from signalling server URL: address is "
          << address << " port string is '" << port_str << "'";
      return absl::InvalidArgumentError(absl::StrCat(
          "Signalling server URL contains an invalid port: '", port_str, "'"));
    }

    if (slash_pos != std::string_view::npos) {
      target = url_str.substr(slash_pos);
    }
  }

  if (address.empty()) {
    return absl::InvalidArgumentError(
        "Signalling server URL must contain a hostname");
  }

  if (target.empty() || target[0] != '/') {
    return absl::InvalidArgumentError(
        "Signalling server URL target must start with '/'");
  }

  if (port == 0) {
    return absl::InvalidArgumentError(
        "Signalling server URL port must be greater than 0");
  }

  if (port == 443 && !explicit_no_ssl) {
    use_ssl = true;
  }

  return WsUrl{.scheme = use_ssl ? "wss" : "ws",
               .host = std::string(address),
               .port = port,
               .target = std::string(target)};
}

WsUrl WsUrl::FromStringOrDie(std::string_view url_str) {
  auto result = FromString(url_str);
  CHECK(result.ok()) << "Failed to parse WsUrl from string: "
                     << result.status();
  return *std::move(result);
}

FiberAwareWebsocketStream::FiberAwareWebsocketStream(
    std::unique_ptr<BoostWebsocketStream> stream,
    PerformHandshakeFn handshake_fn)
    : stream_(std::move(stream)), handshake_fn_(std::move(handshake_fn)) {}

FiberAwareWebsocketStream::FiberAwareWebsocketStream(
    FiberAwareWebsocketStream&& other) noexcept {
  act::MutexLock lock(&other.mu_);

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
  ssl_ctx_ = std::move(other.ssl_ctx_);
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
  ssl_ctx_ = std::move(other.ssl_ctx_);
  handshake_fn_ = std::move(other.handshake_fn_);
  other.stream_ = nullptr;  // Ensure the moved-from object is empty
  other.ssl_ctx_ = nullptr;

  return *this;
}

FiberAwareWebsocketStream::~FiberAwareWebsocketStream() {
  act::MutexLock lock(&mu_);

  cancel_signal_.emit(boost::asio::cancellation_type::total);

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
    PrepareStreamFn prepare_stream_fn, bool use_ssl) {
  return Connect(*util::GetDefaultAsioExecutionContext(), address, port, target,
                 std::move(prepare_stream_fn), use_ssl);
}

BoostWebsocketStream& FiberAwareWebsocketStream::GetStream() const {
  return *stream_;
}

absl::Status FiberAwareWebsocketStream::Write(
    const std::vector<uint8_t>& message_bytes) noexcept {
  act::MutexLock lock(&mu_);

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

  mu_.unlock();
  thread::Select({write_done.OnEvent()});
  mu_.lock();
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
  act::MutexLock lock(&mu_);

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
  stream_->text(true);
  stream_->async_write(
      boost::asio::buffer(message),
      [&error, &write_done](const boost::system::error_code& ec, std::size_t) {
        error = ec;
        write_done.Notify();
      });

  mu_.unlock();
  thread::Select({write_done.OnEvent()});
  mu_.lock();
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
  act::MutexLock lock(&mu_);
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

  mu_.unlock();
  thread::Select({close_done.OnEvent()});
  mu_.lock();

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
  stream->handshake(host, target, error);

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
  act::MutexLock lock(&mu_);

  stream_->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::response_type& res) {
        res.set(boost::beast::http::field::server,
                "Action Engine 0.1.7 "
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

  mu_.unlock();
  thread::Select({accept_done.OnEvent(), thread::OnCancel()});
  mu_.lock();

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
    absl::Duration timeout, std::vector<uint8_t>* absl_nonnull buffer,
    bool* absl_nullable got_text) noexcept {
  const absl::Time deadline = absl::Now() + timeout;

  act::MutexLock lock(&mu_);

  while (read_pending_) {
    if (!cv_.WaitWithDeadline(&mu_, deadline)) {
      return absl::DeadlineExceededError(
          "FiberAwareWebsocketStream Read operation timed out waiting for "
          "another read to complete.");
    }
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
      boost::asio::bind_cancellation_slot(
          cancel_signal_.slot(),
          [&error, &read_done](const boost::system::error_code& ec,
                               std::size_t) {
            error = ec;
            read_done.Notify();
          }));

  mu_.unlock();
  const int selected = thread::SelectUntil(deadline, {read_done.OnEvent()});
  mu_.lock();

  if (selected == -1) {
    cancel_signal_.emit(boost::asio::cancellation_type::total);
    mu_.unlock();
    // We still need to wait for the read_done event to be processed because
    // it is a local variable, and we need to ensure that the callback has
    // completed before we return to avoid memory corruption.
    thread::Select({read_done.OnEvent()});
    mu_.lock();
  }

  // Only here we can safely let other read operations proceed.
  read_pending_ = false;
  cv_.SignalAll();

  // Only after we have notified other threads that the read is done,
  // we can return if the read was cancelled or timed out.
  if (selected == -1) {
    // Timed out.
    return absl::DeadlineExceededError(
        "FiberAwareWebsocketStream Read operation timed out.");
  }

  // Finally, we can move the received data to the output buffer.
  *buffer = std::move(temp_buffer);

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
    absl::Duration timeout, std::string* absl_nonnull buffer) noexcept {
  bool got_text = false;
  std::vector<uint8_t> temp_buffer;

  RETURN_IF_ERROR(
      Read(timeout, &temp_buffer, &got_text));  // Reuse the Read method.

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

}  // namespace act::net