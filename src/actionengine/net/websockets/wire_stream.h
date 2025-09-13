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

#ifndef ACTIONENGINE_NET_WEBSOCKETS_WIRE_STREAM_H_
#define ACTIONENGINE_NET_WEBSOCKETS_WIRE_STREAM_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#define BOOST_ASIO_NO_DEPRECATED

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_format.h>
#include <boost/asio/ip/tcp.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/net/websockets/fiber_aware_websocket_stream.h"
#include "actionengine/service/service.h"

namespace act::net {

/**
 * A class representing a WebSocket stream for sending and receiving Action
 * Engine WireMessages.
 *
 * @headerfile actionengine/net/websockets/server.h
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

  ~WebsocketWireStream() override;

  absl::Status Send(WireMessage message) override;

  absl::StatusOr<std::optional<WireMessage>> Receive(
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
  absl::Status SendInternal(WireMessage message)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_;

  FiberAwareWebsocketStream stream_;
  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  std::string id_;

  absl::Status status_;
};

absl::StatusOr<std::unique_ptr<WebsocketWireStream>> MakeWebsocketWireStream(
    std::string_view address = "127.0.0.1", uint16_t port = 20000,
    std::string_view target = "/", std::string_view id = "",
    PrepareStreamFn prepare_stream = PrepareClientStream);

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBSOCKETS_WIRE_STREAM_H_
