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

#ifndef EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_
#define EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_

#include <memory>
#include <optional>

#define BOOST_ASIO_NO_DEPRECATED

#include <utility>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/net/websockets/fiber_aware_websocket_stream.h"
#include "eglt/service/service.h"
#include "eglt/util/boost_asio_utils.h"
#include "eglt/util/random.h"

namespace eglt::net {

/**
 * A class representing a WebSocket stream for sending and receiving Action
 * Engine SessionMessages.
 *
 * @headerfile eglt/net/websockets/websockets.h
 *
 * This class implements the `WireStream` interface and provides methods for
 * sending and receiving messages over a WebSocket connection. It is designed to
 * be used in both client and server contexts, allowing for flexible
 * communication patterns.
 */
class WebsocketWireStream final : public WireStream {
 public:
  explicit WebsocketWireStream(std::unique_ptr<BoostWebsocketStream> stream,
                               std::string_view id = "");

  explicit WebsocketWireStream(FiberAwareWebsocketStream stream,
                               std::string_view id = "");

  absl::Status Send(SessionMessage message) override;

  absl::StatusOr<std::optional<SessionMessage>> Receive(
      absl::Duration timeout) override;

  absl::Status Start() override;

  absl::Status Accept() override;

  void HalfClose() override;

  void Abort() override;

  absl::Status GetStatus() const override { return status_; }

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* absl_nonnull GetImpl() const override {
    return &stream_;
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const WebsocketWireStream& stream) {
    absl::Format(&sink, "WebsocketWireStream(id: %s, status: %v)", stream.id_,
                 stream.status_);
  }

 private:
  absl::Status SendInternal(SessionMessage message)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable eglt::Mutex mu_;

  FiberAwareWebsocketStream stream_;
  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  std::string id_;

  absl::Status status_;
};

class WebsocketServer {
 public:
  explicit WebsocketServer(eglt::Service* absl_nonnull service,
                           std::string_view address = "0.0.0.0",
                           uint16_t port = 20000);

  ~WebsocketServer();

  void Run();

  absl::Status Cancel();

  absl::Status Join();

 private:
  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  eglt::Service* absl_nonnull const service_;
  std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;

  mutable eglt::Mutex mu_;
  std::unique_ptr<thread::Fiber> main_loop_;
  bool cancelled_ ABSL_GUARDED_BY(mu_) = false;
  eglt::CondVar join_cv_ ABSL_GUARDED_BY(mu_);
  bool joining_ ABSL_GUARDED_BY(mu_) = false;
  absl::Status status_;
};

absl::StatusOr<std::unique_ptr<WebsocketWireStream>> MakeWebsocketWireStream(
    std::string_view address = "127.0.0.1", uint16_t port = 20000,
    std::string_view target = "/", std::string_view id = "",
    PrepareStreamFn prepare_stream = PrepareClientStream);

}  // namespace eglt::net

#endif  // EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_