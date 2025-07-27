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

#include <string_view>

#include <absl/debugging/failure_signal_handler.h>
#include <pybind11/detail/common.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include "eglt/actions/actions_pybind11.h"
#include "eglt/data/data_pybind11.h"
#include "eglt/net/webrtc/webrtc_pybind11.h"
#include "eglt/net/websockets/websockets_pybind11.h"
#include "eglt/nodes/nodes_pybind11.h"
#include "eglt/redis/chunk_store_pybind11.h"
#include "eglt/service/service_pybind11.h"
#include "eglt/stores/chunk_store_pybind11.h"

namespace eglt {

/// @private
PYBIND11_MODULE(evergreen_pybind11, m) {
  absl::InstallFailureSignalHandler({});

  py::module_ data = pybindings::MakeDataModule(m, "data");

  py::module_ chunk_store = pybindings::MakeChunkStoreModule(m, "chunk_store");
  pybindings::BindNodeMap(m, "NodeMap");
  pybindings::BindAsyncNode(m, "AsyncNode");

  py::module_ actions = pybindings::MakeActionsModule(m, "actions");
  py::module_ redis = pybindings::MakeRedisModule(m, "redis");
  py::module_ service = pybindings::MakeServiceModule(m, "service");
  py::module_ webrtc = pybindings::MakeWebRtcModule(m, "webrtc");
  py::module_ websockets = pybindings::MakeWebsocketsModule(m, "websockets");
}

}  // namespace eglt
