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

#ifndef EGLT_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_
#define EGLT_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"

namespace eglt::net {

using BoostWebsocketStream =
    boost::beast::websocket::stream<boost::asio::ip::tcp::socket>;

using PrepareStreamFn = std::function<absl::Status(BoostWebsocketStream*)>;

absl::Status PrepareClientStream(BoostWebsocketStream* absl_nonnull stream);
absl::Status PrepareServerStream(BoostWebsocketStream* absl_nonnull stream);

using PerformHandshakeFn = std::function<absl::Status(BoostWebsocketStream*)>;

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
      PrepareStreamFn prepare_stream_fn = PrepareClientStream);

  static absl::StatusOr<FiberAwareWebsocketStream> Connect(
      std::string_view address, uint16_t port, std::string_view target = "/",
      PrepareStreamFn prepare_stream_fn = PrepareClientStream);

  BoostWebsocketStream& GetStream() const;

  absl::Status Accept() const noexcept;
  absl::Status Close() const noexcept;
  absl::Status Read(std::vector<uint8_t>* absl_nonnull buffer,
                    bool* absl_nullable got_text = nullptr) noexcept;
  absl::Status ReadText(std::string* buffer) noexcept;
  absl::Status Start() const noexcept;
  absl::Status Write(const std::vector<uint8_t>& message_bytes) noexcept;
  absl::Status WriteText(const std::string& message) noexcept;
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
  PerformHandshakeFn handshake_fn_;

  mutable concurrency::Mutex mu_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mu_);
  bool write_pending_ ABSL_GUARDED_BY(mu_) = false;
  bool read_pending_ ABSL_GUARDED_BY(mu_) = false;
};

template <typename ExecutionContext>
absl::Status ResolveAndConnect(ExecutionContext& context,
                               BoostWebsocketStream* absl_nonnull stream,
                               std::string_view address, uint16_t port) {
  boost::system::error_code error;
  boost::asio::ip::tcp::resolver resolver(context);

  thread::PermanentEvent resolve_done;
  boost::asio::ip::tcp::resolver::results_type endpoints;
  resolver.async_resolve(
      address, std::to_string(port),
      boost::asio::ip::resolver_query_base::flags(),
      [&error, &resolve_done, &endpoints](
          const boost::system::error_code& ec,
          boost::asio::ip::tcp::resolver::results_type async_results) {
        error = ec;
        endpoints = std::move(async_results);
        resolve_done.Notify();
      });
  thread::Select({resolve_done.OnEvent(), thread::OnCancel()});

  if (thread::Cancelled()) {
    resolver.cancel();
    stream->next_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                                  error);
    return absl::CancelledError("FiberAwareWebsocketStream Connect cancelled");
  }

  if (error) {
    return absl::InternalError(error.message());
  }

  boost::asio::ip::tcp::endpoint endpoint;
  thread::PermanentEvent connect_done;
  boost::asio::async_connect(
      stream->next_layer(), endpoints,
      [&error, &connect_done, &endpoint](
          const boost::system::error_code& ec,
          boost::asio::ip::tcp::endpoint async_endpoint) {
        error = ec;
        endpoint = std::move(async_endpoint);
        connect_done.Notify();
      });
  thread::Select({connect_done.OnEvent(), concurrency::OnCancel()});

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
    std::string_view target, PrepareStreamFn prepare_stream_fn) {
  auto ws_stream = std::make_unique<BoostWebsocketStream>(context);

  if (absl::Status resolve_status =
          ResolveAndConnect(context, ws_stream.get(), address, port);
      !resolve_status.ok()) {
    return resolve_status;
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

  return FiberAwareWebsocketStream(std::move(ws_stream),
                                   std::move(do_handshake));
}

}  // namespace eglt::net

#endif  // EGLT_NET_WEBSOCKETS_FIBER_AWARE_WEBSOCKET_STREAM_H_