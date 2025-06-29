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

#ifndef EGLT_NET_STREAM_H_
#define EGLT_NET_STREAM_H_

#include <functional>
#include <optional>
#include <string>

#include "eglt/data/eg_structs.h"
#include "eglt/data/serialization.h"

namespace eglt {

using SendBytesT = std::function<absl::Status(Bytes bytes)>;
using ReceiveBytesT = std::function<std::optional<Bytes>()>;

}  // namespace eglt

namespace eglt {

//! An abstract base class for an Evergreen stream.
/*!
 * This class provides an interface for sending and receiving messages over a
 * stream. It is intended to be used as a base class for specific stream
 * implementations, such as WebSocket or gRPC streams.
 */
class WireStream {
 public:
  virtual ~WireStream() = default;

  //! Sends an Evergreen session message over the stream.
  virtual auto Send(SessionMessage message) -> absl::Status = 0;
  //! Receives an Evergreen session message from the stream.
  virtual auto Receive() -> std::optional<SessionMessage> = 0;

  //! Accept the stream on the server side. Clients should not call this.
  virtual auto Accept() -> absl::Status = 0;
  //! Start the stream on the client side. Servers should not call this.
  virtual auto Start() -> absl::Status = 0;
  //! Initiates a graceful shutdown of the stream. Should be called from the
  //! client first.
  virtual auto HalfClose() -> absl::Status = 0;

  //! Registers a callback to be called when the stream is half-closed by the
  //! other end. This callback will be invoked with a pointer to the WireStream
  //! that was half-closed. This callback must NOT call HalfClose() on the
  /// stream itself.
  virtual auto OnHalfClose(absl::AnyInvocable<void(WireStream*)> fn)
      -> void = 0;

  //! Returns the status of the last operation.
  virtual auto GetStatus() const -> absl::Status = 0;

  //! Returns the implementation-dependent identifier of the stream.
  [[nodiscard]] virtual auto GetId() const -> std::string_view = 0;

  //! Returns the underlying implementation of the stream.
  /*!
   * This function is intended to be overridden by derived classes to provide
   * access to the specific implementation of the stream. The default
   * implementation returns a null pointer.
   *
   * @return A pointer to the underlying implementation of the stream.
   */
  [[nodiscard]] virtual auto GetImpl() const -> const void* { return nullptr; }

  //! Returns the underlying implementation of the stream.
  /*!
   * This function is a template that allows the user to specify the type of
   * the underlying implementation. It uses static_cast to convert the void*
   * pointer returned by GetImpl() to the specified type.
   *
   * \tparam T The type of the underlying implementation.
   * @return A pointer to the underlying implementation of type T.
   */
  template <typename T>
  [[nodiscard]] auto GetImpl() const -> T* {
    return static_cast<T*>(GetImpl());
  }
};

}  // namespace eglt

#endif  // EGLT_NET_STREAM_H_
