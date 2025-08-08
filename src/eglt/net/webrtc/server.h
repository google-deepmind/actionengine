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

#ifndef EGLT_NET_WEBRTC_SERVER_H_
#define EGLT_NET_WEBRTC_SERVER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/hash/hash.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <rtc/datachannel.hpp>
#include <rtc/peerconnection.hpp>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/net/webrtc/signalling.h"
#include "eglt/net/webrtc/wire_stream.h"
#include "eglt/service/service.h"
#include "eglt/stores/byte_chunking.h"

/**
 * @file
 * @brief Provides the WebRtcServer class for handling WebRTC data channel
 * connections.
 *
 * This file defines the `WebRtcServer` class, which is responsible for
 * accepting incoming WebRTC data channel connections and turning them into
 * `WebRtcWireStream` instances. The `WebRtcServer` class is designed to
 * work with an `eglt::Service` instance, which handles the processing of the
 * `WebRtcWireStream` instances, including application logic, session
 * management, and error handling.
 *
 */

namespace eglt::net {

/**
 * Represents an acceptor for WebRTC data channels, opening an eglt::Service
 * instance for incoming connections.
 *
 * This class is designed to handle incoming WebRTC data channel connections,
 * which it accepts through a signalling server, turning successfully accepted
 * connections into `WebRtcWireStream` instances. The WireStream instances are
 * then passed to the `eglt::Service` for further processing.
 *
 * The server abstraction in Action Engine is intentionally not standardised
 * across all protocols, and it is only expected from the server to
 * accept incoming connections and pass them to the service. The service is
 * expected to handle the connections, including sending and receiving
 * session messages, managing the lifecycle of the connections, and
 * handling any logical errors that may occur during the communication.
 *
 * Protocol-specific lifecycle management is better handled by the WireStream
 * implementation and not by the server itself. Server implementations
 * may take care of other considerations, such as security, load balancing,
 * access and flow control, and are unrestricted in doing so.
 *
 * @headerfile eglt/net/webrtc/server.h
 */
class WebRtcServer {
 public:
  /**
   * Constructs a WebRtcServer instance.
   *
   * @param service
   *   The `eglt::Service` instance which will handle the WireStream instances
   *   created by this server from incoming WebRTC data channel connections.
   * @param address
   *   The address to bind the server to. Defaults to `0.0.0.0`, which
   *   means the server will accept connections on all available interfaces.
   * @param port
   *   The port to bind the server to. Defaults to `20000`.
   * @param signalling_address
   *   The address of the signalling server to connect to. Defaults to
   *   `localhost`.
   * @param signalling_port
   *   The port of the signalling server to connect to. Defaults to `80`.
   * @param signalling_identity
   *   The identity of the server in the signalling server. Defaults to
   *   `server`. This identity is used to identify the server in the signalling
   *   server and to establish connections with clients.
   * @note
   *   Notice that no special measures are taken to protect the server's
   *   identity, so in production setups, signalling servers MUST make sure
   *   that the identity cannot be impersonated by a malicious client.
   * @param rtc_config
   *   Optional configuration for the WebRTC connections. If not provided,
   *   a default configuration will be used. The configuration can include
   *   settings such as STUN/TURN servers, port ranges, and other WebRTC
   *   parameters.
   */
  explicit WebRtcServer(eglt::Service* absl_nonnull service,
                        std::string_view address = "0.0.0.0",
                        uint16_t port = 20000,
                        std::string_view signalling_address = "localhost",
                        uint16_t signalling_port = 80,
                        std::string_view signalling_identity = "server",
                        std::optional<RtcConfig> rtc_config = std::nullopt);

  ~WebRtcServer();

  /**
   * Starts the WebRTC server, accepting incoming connections and processing
   * them in a loop.
   *
   * Accepted connections are turned into `WebRtcWireStream` instances,
   * which are then passed to the `eglt::Service` to perform application
   * logic, session management, and error handling.
   *
   * This method does not block the caller, but instead runs in a separate
   * fiber. That fiber will continue to run until the server is cancelled by
   * calling `Cancel()`, and can be joined by calling `Join()`.
   */
  void Run();

  /**
   * Cancels the WebRTC server, stopping loop that accepts incoming connections.
   *
   * Note that by itself, this does not close any existing WireStream
   * instances or connections. It simply stops accepting new connections.
   * The eglt::Service instance will continue working normally unless and
   * until it is also cancelled or goes out of scope.
   *
   * @return
   *   An OK status.
   */
  absl::Status Cancel();

  /**
   * Joins the WebRTC server, waiting for the main loop that accepts
   * incoming connections to finish running.
   *
   * @return
   *   An OK status if the main loop has finished successfully, or an error
   *   status if there was an issue at some point during the execution of
   *   the main loop.
   */
  absl::Status Join();

 private:
  using DataChannelConnectionMap =
      absl::flat_hash_map<std::string, WebRtcDataChannelConnection>;
  void RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::shared_ptr<SignallingClient> InitSignallingClient(
      std::string_view signalling_address, uint16_t signalling_port,
      DataChannelConnectionMap* absl_nonnull connections);

  eglt::Service* absl_nonnull const service_;

  const std::string address_;
  const uint16_t port_;
  const std::string signalling_address_;
  const uint16_t signalling_port_;
  const std::string signalling_identity_;
  const std::optional<RtcConfig> rtc_config_;

  thread::Channel<WebRtcDataChannelConnection> ready_data_connections_;
  eglt::Mutex mu_;
  std::unique_ptr<thread::Fiber> main_loop_;
};

}  // namespace eglt::net

#endif  // EGLT_NET_WEBRTC_SERVER_H_