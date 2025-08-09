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

#ifndef ACTIONENGINE_NET_STREAM_H_
#define ACTIONENGINE_NET_STREAM_H_

#include <functional>
#include <optional>
#include <string>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/serialization.h"
#include "actionengine/data/types.h"

namespace act {

using SendBytesT = std::function<absl::Status(Bytes bytes)>;
using ReceiveBytesT = std::function<std::optional<Bytes>()>;

}  // namespace act

namespace act {

/**
 * An abstract base class for an ActionEngine stream.
 *
 * This class provides an interface for sending and receiving messages over a
 * stream. It is intended to be used as a base class for specific stream
 * implementations, such as WebSocket or gRPC streams.
 *
 * @headerfile actionengine/net/stream.h
 */
class WireStream {
 public:
  virtual ~WireStream() = default;

  /**
   * Sends a SessionMessage over the stream.
   *
   * May or may not block, depending on the implementation. For example,
   * WebsocketWireStream will block until the message is sent, while
   * WebRtcWireStream will return immediately, allowing the message to be sent
   * asynchronously.
   *
   * With that in mind, the caller should not assume that the message has been
   * sent immediately after this function returns, or even that it has been
   * sent successfully. Failure modes of this method are more about the
   * invariants of the stream itself: for example, if the stream is closed, or
   * half-closed (no more messages can be sent), this method will return an
   * error.
   *
   * @param message
   *   The SessionMessage to send, containing action calls (in the form of
   *   multiple ActionMessage) and/or node fragments.
   * @return
   *   An OK status unless the stream is in a state where it cannot send
   *   messages, such as being half-closed or closed.
   * @note
   *   Implementations of WireStream should and will treat a SessionMessage
   *   with no actions or node fragments as an initiation or acknowledgment of a
   *   half-close.
   */
  virtual auto Send(SessionMessage message) -> absl::Status = 0;
  /**
   * Receives an ActionEngine session message from the stream.
   *
   * Blocks to wait for a message to be received, up to the specified timeout.
   *
   * @param timeout
   *   The maximum duration to wait for a message to be received.
   *   If the timeout is reached, the function will return an
   *   `absl::DeadlineExceededError`.
   * @return
   *   An optional SessionMessage if a message is received within the
   *   specified timeout. An empty optional indicates that no message will be
   *   received.
   * @note
   *   Correct implementations of this method should return an empty optional
   *   if the stream is half-closed, and no more messages can be received.
   *   If the stream is aborted (e.g., due to an error), the method should
   *   return an error status.
   * @note
   *   Implementations MUST ensure that this method reacts to an Abort() call,
   *   returning an error status and not blocking indefinitely. This is
   *   important in the inner workings of the ActionEngine, where
   *   `Receive()` is called in a loop, and the loop should be able to
   *   terminate gracefully when the stream is aborted.
   */
  virtual auto Receive(absl::Duration timeout)
      -> absl::StatusOr<std::optional<SessionMessage>> = 0;

  /**
   * Accepts the stream on the server side.
   *
   * Clients should not call this method. It is intended to be called by the
   * server to accept a stream that has been initiated by a client.
   * Implementations that differentiate between client and server streams
   * are allowed to return an error if this method is called on a client, or
   * even to perform a hard termination of the process, as such a call is
   * considered a misuse of the API.
   *
   * Some implementations may not require this method, in which case they
   * can return an OK status without doing anything.
   *
   * @return
   *   An OK status if the stream is successfully accepted, or an error status
   *   if the stream cannot be accepted (e.g., if it is already accepted, or
   *   some underlying negotiation failed).
   */
  virtual auto Accept() -> absl::Status = 0;
  /**
   * Starts the stream on the client side.
   *
   * Clients should call this method to initiate the stream after it has been
   * created. This method is intended to be called by the client to start the
   * stream, which may involve some underlying negotiation or setup.
   * Implementations that differentiate between client and server streams
   * are allowed to return an error if this method is called on a server, or
   * even to perform a hard termination of the process, as such a call is
   * considered a misuse of the API.
   *
   * Some implementations may not require this method, in which case they
   * can return an OK status without doing anything.
   *
   * @return
   *   An OK status if the stream is successfully started, or an error status
   *   if the stream cannot be started (e.g., if it is already started, or
   *   some underlying negotiation failed).
   */
  virtual auto Start() -> absl::Status = 0;
  /**
   * Communicates to the other end that no more messages will be sent
   * over this stream.
   *
   * @note
   *   This method does not close the stream, nor does it initiate a
   *   finalisation. It simply indicates that no more messages will be sent.
   *   The stream remains open for receiving messages, and the other end can
   *   still send messages until it also calls HalfClose() or closes the stream.
   *
   * @note
   *   Nor does this method guarantee that the other end will receive the
   *   half-close message immediately, or at all. However, if the stream is
   *   indeed broken, GetStatus() will return an error status, and Send() and
   *   Receive() will return errors as well if a stream is in a state where
   *   they cannot proceed.
   */
  virtual void HalfClose() = 0;

  /**
   * Aborts the stream.
   *
   * This is used to terminate the stream immediately with an "error" state. For
   * example, the result of Receive() following an Abort() call *by either side*
   * should be an error status, indicating that the stream is no longer usable,
   * and not an empty optional.
   */
  virtual void Abort() = 0;

  /**
   * Returns the status of the stream.
   *
   * This method is used to check the current status of the stream, which is
   * OK unless the stream has been aborted.
   *
   * @return
   *   An `absl::Status` indicating the current status of the stream.
   *   If the stream is in a valid state, it returns absl::OkStatus().
   *   If the stream is in an error state (e.g., due to an abort), it
   *   returns an appropriate error status.
   */
  virtual auto GetStatus() const -> absl::Status = 0;

  /**
   * Returns the unique (in this process) identifier of the stream.
   * @return
   *   A string representing the unique identifier of the stream.
   *   This identifier is used to distinguish between different streams in the
   *   same process, but not across a load balanced group, for example.
   */
  [[nodiscard]] virtual auto GetId() const -> std::string = 0;

  /**
   * Returns the underlying implementation of the stream.
   *
   * This function is intended to be overridden by derived classes to provide
   * access to the specific implementation of the stream, in case such access is
   * required. The default implementation returns a null pointer.
   *
   * @return
   *   A pointer to the underlying implementation of the stream.
   */
  [[nodiscard]] virtual auto GetImpl() const -> const void* { return nullptr; }

  /**
   * Returns the underlying implementation of the stream.
   *
   * This function is a template that allows the user to specify the type of
   * the underlying implementation. It uses static_cast to convert the void*
   * pointer returned by GetImpl() to the specified type.
   *
   * @tparam T The type of the underlying implementation.
   * @return A pointer to the underlying implementation of type \p T.
   */
  template <typename T>
  [[nodiscard]] auto GetImpl() const -> T* {
    return static_cast<T*>(GetImpl());
  }
};

}  // namespace act

#endif  // ACTIONENGINE_NET_STREAM_H_
