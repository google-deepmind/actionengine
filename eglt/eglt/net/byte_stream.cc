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

#include "byte_stream.h"

#include <any>
#include <memory>
#include <optional>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/data/serialization.h"

namespace eglt {

EvergreenByteStream::EvergreenByteStream(
    SendBytesT send_bytes, ReceiveBytesT receive_bytes,
    const std::shared_ptr<Serializer>& serializer)
    : send_bytes_(std::move(send_bytes)),
      receive_bytes_(std::move(receive_bytes)),
      serializer_(serializer) {}

absl::Status EvergreenByteStream::SendBytes(Bytes bytes) const {
  return send_bytes_(std::move(bytes));
}

absl::Status EvergreenByteStream::Send(base::SessionMessage message) const {
  const auto data = serializer_->Serialize(std::move(message));
  return send_bytes_(data);
}

std::optional<Bytes> EvergreenByteStream::ReceiveBytes() const {
  std::optional<Bytes> data = receive_bytes_();
  if (!data.has_value()) {
    return std::nullopt;
  }

  return *data;
}

std::optional<base::SessionMessage> EvergreenByteStream::Receive() const {
  const std::optional<Bytes> data = receive_bytes_();
  if (!data.has_value()) {
    return std::nullopt;
  }

  auto message = serializer_->Deserialize(*data);
  if (!message.has_value()) {
    return std::nullopt;
  }

  return std::any_cast<base::SessionMessage>(*message);
}

}  // namespace eglt
