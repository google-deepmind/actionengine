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

#include "actionengine/net/websockets/wire_stream.h"

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

WebsocketWireStream::~WebsocketWireStream() {
  act::MutexLock lock(&mu_);
  if (!closed_) {
    HalfCloseInternal().IgnoreError();
  }
  closed_ = true;
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
}  // namespace act::net