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

/**
 * @file
 * @brief
 *   A wrapper to make an EvergreenStream from send/recv-like functions.
 *
 * This file contains the definition of the EvergreenByteStream class, which is
 * used to create an EvergreenStream from raw byte streams. It provides methods
 * to send and receive messages, as well as to send and receive raw bytes.
 */

#ifndef EGLT_NET_BYTE_STREAM_H_
#define EGLT_NET_BYTE_STREAM_H_

#include <memory>
#include <optional>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/data/serialization.h"
#include "eglt/net/stream.h"

namespace eglt {

class EvergreenByteStream {
 public:
  explicit EvergreenByteStream(
      SendBytesT send_bytes = nullptr, ReceiveBytesT receive_bytes = nullptr,
      const std::shared_ptr<Serializer>& serializer = nullptr);

  absl::Status Send(base::SessionMessage message) const;
  [[nodiscard]] std::optional<base::SessionMessage> Receive() const;

  [[nodiscard]] std::optional<Bytes> ReceiveBytes() const;
  absl::Status SendBytes(Bytes bytes) const;

 protected:
  SendBytesT send_bytes_;
  ReceiveBytesT receive_bytes_;
  std::shared_ptr<Serializer> serializer_;
};

}  // namespace eglt

#endif  // EGLT_NET_BYTE_STREAM_H_
