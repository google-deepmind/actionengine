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

#ifndef ACTIONENGINE_DISTRIBUTED_PEERS_H_
#define ACTIONENGINE_DISTRIBUTED_PEERS_H_

#include <string>
#include <string_view>
#include <utility>

#include <absl/base/nullability.h>

#include "actionengine/distributed/sinks.h"

namespace act::distributed {

struct GetRequest {
  std::string group;
  std::string key;
};

struct GetResponse {
  std::optional<std::string> value;
  std::optional<double> minute_qps;
};

using ServiceGetter = absl::AnyInvocable<absl::Status(
    GetRequest* absl_nonnull request, GetResponse* absl_nonnull response)>;

class PeerPicker {
 public:
  virtual ~PeerPicker() = default;
  [[nodiscard]] virtual std::optional<ServiceGetter> PickPeer(
      std::string_view key) const = 0;
};

class NoPeers final : public PeerPicker {
 public:
  [[nodiscard]] std::optional<ServiceGetter> PickPeer(
      std::string_view /*key*/) const override {
    return std::nullopt;
  }
};

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_PEERS_H_
