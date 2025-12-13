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

#ifndef ACTIONENGINE_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_
#define ACTIONENGINE_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <variant>
#include <vector>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/string_view.h>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <openssl/ssl.h>

#include "actionengine/concurrency/concurrency.h"

namespace act::net {

// Forward declaration of the wrapper class.
class BoostWebsocketStream;

using PrepareStreamFn =
    std::function<absl::Status(BoostWebsocketStream* absl_nonnull)>;

using PerformHandshakeFn =
    std::function<absl::Status(BoostWebsocketStream* absl_nonnull)>;

absl::Status PrepareClientStream(BoostWebsocketStream* absl_nonnull stream);
absl::Status PrepareServerStream(BoostWebsocketStream* absl_nonnull stream);

class BoostWebsocketStream {
 public:
  using PlainStream =
      boost::beast::websocket::stream<boost::asio::ip::tcp::socket>;
  using SslStream = boost::beast::websocket::stream<
      boost::asio::ssl::stream<boost::asio::ip::tcp::socket>>;

  // Constructs a wrapper holding a Plain stream.
  explicit BoostWebsocketStream(boost::asio::ip::tcp::socket&& socket);

  // Constructs a wrapper holding an SSL stream.
  explicit BoostWebsocketStream(
      boost::asio::ssl::stream<boost::asio::ip::tcp::socket>&& stream);

  template <typename ExecutionContext>
  explicit BoostWebsocketStream(ExecutionContext& context)
      : stream_(PlainStream(boost::asio::make_strand(context))) {}

  template <typename ExecutionContext>
  explicit BoostWebsocketStream(ExecutionContext& context,
                                boost::asio::ssl::context& ssl_ctx)
      : stream_(SslStream(boost::asio::make_strand(context), ssl_ctx)) {}

  BoostWebsocketStream(BoostWebsocketStream&&) = default;

  template <class Option>
  void set_option(Option&& opt) {
    std::visit([&](auto& s) { s.set_option(std::forward<Option>(opt)); },
               stream_);
  }

  void binary(bool value);
  void text(bool value);
  [[nodiscard]] bool got_text() const;
  [[nodiscard]] bool is_open() const;
  void write_buffer_bytes(std::size_t bytes);

  boost::asio::ip::tcp::socket& next_layer();
  [[nodiscard]] const boost::asio::ip::tcp::socket& next_layer() const;

  // wrappers for async operations

  template <typename Buffer, typename Callback>
  void async_write(const Buffer& buffer, Callback&& cb) {
    std::visit(
        [&](auto& s) { s.async_write(buffer, std::forward<Callback>(cb)); },
        stream_);
  }

  template <typename DynamicBuffer, typename Callback>
  void async_read(DynamicBuffer& buffer, Callback&& cb) {
    std::visit(
        [&](auto& s) { s.async_read(buffer, std::forward<Callback>(cb)); },
        stream_);
  }

  template <typename Callback>
  void async_close(boost::beast::websocket::close_code code, Callback&& cb) {
    std::visit(
        [&](auto& s) { s.async_close(code, std::forward<Callback>(cb)); },
        stream_);
  }

  template <typename Callback>
  void async_accept(Callback&& cb) {
    std::visit([&](auto& s) { s.async_accept(std::forward<Callback>(cb)); },
               stream_);
  }

  void handshake(std::string_view host, std::string_view target,
                 boost::system::error_code& ec);

  // returns error if called on a plain stream (shouldn't happen)
  void async_ssl_handshake(boost::asio::ssl::stream_base::handshake_type type,
                           std::function<void(boost::system::error_code)> cb);

  SslStream* absl_nullable GetSslStreamOrNull();

 private:
  std::variant<PlainStream, SslStream> stream_;
};

struct WsUrl {
  std::string scheme;
  std::string host;
  uint16_t port;
  std::string target;

  static absl::StatusOr<WsUrl> FromString(std::string_view url_str);

  static WsUrl FromStringOrDie(std::string_view url_str);
};

class FiberAwareWebsocketStream {
 public:
  explicit FiberAwareWebsocketStream(
      std::unique_ptr<BoostWebsocketStream> stream = nullptr,
      PerformHandshakeFn handshake_fn = {});

  FiberAwareWebsocketStream(const FiberAwareWebsocketStream&) = delete;
  FiberAwareWebsocketStream& operator=(const FiberAwareWebsocketStream&) =
      delete;

  FiberAwareWebsocketStream(FiberAwareWebsocketStream&&) noexcept;
  FiberAwareWebsocketStream& operator=(FiberAwareWebsocketStream&&) noexcept;

  ~FiberAwareWebsocketStream();

  template <typename ExecutionContext>
  static absl::StatusOr<FiberAwareWebsocketStream> Connect(
      ExecutionContext& context, std::string_view address, uint16_t port,
      std::string_view target = "/",
      PrepareStreamFn prepare_stream_fn = PrepareClientStream,
      bool use_ssl = false);

  static absl::StatusOr<FiberAwareWebsocketStream> Connect(
      std::string_view address, uint16_t port, std::string_view target = "/",
      PrepareStreamFn prepare_stream_fn = PrepareClientStream,
      bool use_ssl = false);

  BoostWebsocketStream& GetStream() const;

  absl::Status Accept() const noexcept;
  absl::Status Close() const noexcept;
  absl::Status Read(absl::Duration timeout,
                    std::vector<uint8_t>* absl_nonnull buffer,
                    bool* absl_nullable got_text = nullptr) noexcept;
  absl::Status ReadText(absl::Duration timeout,
                        std::string* absl_nonnull buffer) noexcept;
  absl::Status Start() const noexcept;
  absl::Status Write(const std::vector<uint8_t>& message_bytes) noexcept;
  absl::Status WriteText(const std::string& message) noexcept;

  void CancelRead() noexcept {
    act::MutexLock lock(&mu_);
    cancel_signal_.emit(boost::asio::cancellation_type::total);
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink,
                            const FiberAwareWebsocketStream& stream) {
    const auto endpoint = stream.stream_->next_layer().remote_endpoint();
    sink.Append(absl::StrFormat("FiberAwareWebsocketStream: %s:%d",
                                endpoint.address().to_string(),
                                endpoint.port()));
  }

 private:
  absl::Status CloseInternal() const noexcept
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::unique_ptr<BoostWebsocketStream> stream_;
  std::shared_ptr<boost::asio::ssl::context> ssl_ctx_;

  PerformHandshakeFn handshake_fn_;

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);
  bool write_pending_ ABSL_GUARDED_BY(mu_) = false;
  bool read_pending_ ABSL_GUARDED_BY(mu_) = false;
  boost::asio::cancellation_signal cancel_signal_ ABSL_GUARDED_BY(mu_);
};

template <typename ExecutionContext>
absl::Status ResolveAndConnect(ExecutionContext& context,
                               BoostWebsocketStream* absl_nonnull stream,
                               std::string_view address, uint16_t port) {
  boost::system::error_code error;
  boost::asio::ip::tcp::resolver resolver(context);

  const auto endpoints =
      resolver.resolve(address, std::to_string(port),
                       boost::asio::ip::resolver_query_base::flags(), error);

  if (error) {
    return absl::InternalError(error.message());
  }

  boost::asio::connect(stream->next_layer(), endpoints, error);

  if (thread::Cancelled()) {
    stream->next_layer().cancel();
    stream->next_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                                  error);
    return absl::CancelledError("FiberAwareWebsocketStream Connect cancelled");
  }

  if (error) {
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status ResolveAndConnect(BoostWebsocketStream* absl_nonnull stream,
                               std::string_view address, uint16_t port);

absl::Status DoHandshake(BoostWebsocketStream* absl_nonnull stream,
                         std::string_view host, std::string_view target = "/");

template <typename ExecutionContext>
absl::StatusOr<FiberAwareWebsocketStream> FiberAwareWebsocketStream::Connect(
    ExecutionContext& context, std::string_view address, uint16_t port,
    std::string_view target, PrepareStreamFn prepare_stream_fn, bool use_ssl) {
  std::unique_ptr<BoostWebsocketStream> ws_stream;
  std::shared_ptr<boost::asio::ssl::context> ssl_ctx;

  if (use_ssl) {
    ssl_ctx = std::make_shared<boost::asio::ssl::context>(
        boost::asio::ssl::context::tls_client);
    ssl_ctx->set_default_verify_paths();
    ws_stream = std::make_unique<BoostWebsocketStream>(context, *ssl_ctx);
  } else {
    ws_stream = std::make_unique<BoostWebsocketStream>(context);
  }

  if (absl::Status resolve_status =
          ResolveAndConnect(context, ws_stream.get(), address, port);
      !resolve_status.ok()) {
    return resolve_status;
  }

  if (use_ssl) {
    // Set SNI Hostname (Server Name Indication)
    BoostWebsocketStream::SslStream* ssl_stream =
        ws_stream->GetSslStreamOrNull();
    if (ssl_stream == nullptr) {
      return absl::InternalError("Expected SSL stream, but got Plain stream");
    }
    if (!SSL_set_tlsext_host_name(ssl_stream->next_layer().native_handle(),
                                  std::string(address).c_str())) {
      return absl::InternalError("Failed to set SNI hostname");
    }

    boost::system::error_code error;
    thread::PermanentEvent ssl_handshake_done;
    ws_stream->async_ssl_handshake(
        boost::asio::ssl::stream_base::client,
        [&error, &ssl_handshake_done](const boost::system::error_code& ec) {
          error = ec;
          ssl_handshake_done.Notify();
        });

    thread::Select({ssl_handshake_done.OnEvent()});

    if (thread::Cancelled()) {
      return absl::CancelledError("SSL Handshake cancelled");
    }
    if (error) {
      return absl::InternalError(
          absl::StrFormat("SSL Handshake failed: %s", error.message()));
    }
  }

  if (prepare_stream_fn) {
    if (absl::Status prepare_status =
            std::move(prepare_stream_fn)(ws_stream.get());
        !prepare_status.ok()) {
      return prepare_status;
    }
  }

  auto do_handshake = [host = absl::StrFormat("%s:%d", address, port),
                       target = std::string(target)](
                          BoostWebsocketStream* absl_nonnull stream) {
    return DoHandshake(stream, host, target);
  };

  FiberAwareWebsocketStream fa_stream(std::move(ws_stream),
                                      std::move(do_handshake));
  fa_stream.ssl_ctx_ = std::move(ssl_ctx);

  return std::move(fa_stream);
}

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_