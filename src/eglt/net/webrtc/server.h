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

namespace eglt::net {

class WebRtcServer {
 public:
  explicit WebRtcServer(eglt::Service* absl_nonnull service,
                        std::string_view address = "0.0.0.0",
                        uint16_t port = 20000,
                        std::string_view signalling_address = "localhost",
                        uint16_t signalling_port = 80,
                        std::string_view signalling_identity = "server",
                        std::optional<RtcConfig> rtc_config = std::nullopt);

  ~WebRtcServer();

  void Run();

  absl::Status Cancel() {
    eglt::MutexLock lock(&mu_);
    return CancelInternal();
  }

  absl::Status Join() {
    eglt::MutexLock lock(&mu_);
    absl::Status status = JoinInternal();
    DLOG(INFO) << "WebRtcServer Join finished with status: " << status;
    return status;
  }

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