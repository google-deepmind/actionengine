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

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/data/serialization.h"

namespace eglt {

using SendBytesT = std::function<absl::Status(Bytes bytes)>;
using ReceiveBytesT = std::function<std::optional<Bytes>()>;

}  // namespace eglt

namespace eglt::base {

class EvergreenStream {
 public:
  virtual ~EvergreenStream() = default;

  virtual auto Send(SessionMessage message) -> absl::Status = 0;
  virtual auto Receive() -> std::optional<SessionMessage> = 0;

  virtual auto Accept() -> void = 0;
  virtual auto Start() -> void = 0;
  virtual auto HalfClose() -> void = 0;

  virtual auto GetLastSendStatus() const -> absl::Status = 0;

  [[nodiscard]] virtual auto GetId() const -> std::string = 0;

  [[nodiscard]] virtual auto GetImpl() const -> void* { return nullptr; }

  template <typename T>
  auto GetImpl() const -> T* {
    return static_cast<T*>(GetImpl());
  }
};

}  // namespace eglt::base

#endif  // EGLT_NET_STREAM_H_
