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

#include "eglt/net/websockets/websockets.h"

namespace eglt::net {

WebsocketEvergreenWireStream::WebsocketEvergreenWireStream(
    std::unique_ptr<BoostWebsocketStream> stream, std::string_view id)
    : stream_({std::move(stream)}),
      id_(id.empty() ? GenerateUUID4() : std::string(id)) {
  DLOG(INFO) << absl::StrFormat("WESt %s created", id_);
}

WebsocketEvergreenWireStream::WebsocketEvergreenWireStream(
    FiberAwareWebsocketStream stream, std::string_view id)
    : stream_(std::move(stream)),
      id_(id.empty() ? GenerateUUID4() : std::string(id)) {}

absl::Status WebsocketEvergreenWireStream::Send(SessionMessage message) {
  const auto message_bytes = cppack::Pack(std::move(message));
  return stream_.Write(message_bytes);
}

std::optional<SessionMessage> WebsocketEvergreenWireStream::Receive() {
  std::vector<uint8_t> buffer;

  // Receive from underlying websocket stream.
  if (const absl::Status status = stream_.Read(&buffer); !status.ok()) {
    DLOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_, status);
    return std::nullopt;
  }

  // Unpack the received data into a SessionMessage.
  if (auto unpacked = cppack::Unpack<SessionMessage>(buffer); unpacked.ok()) {
    return *std::move(unpacked);
  } else {
    LOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_,
                                  unpacked.status());
  }

  return std::nullopt;
}

absl::Status WebsocketEvergreenWireStream::Start() {
  // In this case, the client EG stream is not responsible for handshaking.
  return absl::OkStatus();
}

absl::Status WebsocketEvergreenWireStream::Accept() {
  DLOG(INFO) << absl::StrFormat("WESt %s Accept()", id_);
  return stream_.Accept();
}

void WebsocketEvergreenWireStream::HalfClose() {
  stream_.Close().IgnoreError();
}

}  // namespace eglt::net