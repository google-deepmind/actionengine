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

#include <Python.h>
#include <absl/debugging/failure_signal_handler.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/check_status_module_imported.h>

#include "actionengine/actions/actions_pybind11.h"
#include "actionengine/data/data_pybind11.h"
#include "actionengine/net/webrtc/webrtc_pybind11.h"
#include "actionengine/net/websockets/websockets_pybind11.h"
#include "actionengine/nodes/nodes_pybind11.h"
#include "actionengine/redis/chunk_store_pybind11.h"
#include "actionengine/service/service_pybind11.h"
#include "actionengine/stores/chunk_store_pybind11.h"
#include "actionengine/util/utils_pybind11.h"

namespace act {

PYBIND11_MODULE(actionengine_pybind11, m) {
  absl::InstallFailureSignalHandler({});
  if (!pybind11::google::internal::IsStatusModuleImported()) {
    py::module_::import("actionengine.pybind11_abseil.status");
    // importing under a custom path/name, so just in case check that the
    // library understands our import.
    py::google::internal::CheckStatusModuleImported();
  }

  py::module_ data = pybindings::MakeDataModule(m, "data");

  py::module_ chunk_store = pybindings::MakeChunkStoreModule(m, "chunk_store");
  pybindings::BindNodeMap(m, "NodeMap");
  pybindings::BindAsyncNode(m, "AsyncNode");

  py::module_ actions = pybindings::MakeActionsModule(m, "actions");
  py::module_ redis = pybindings::MakeRedisModule(m, "redis");
  py::module_ service = pybindings::MakeServiceModule(m, "service");
  py::module_ webrtc = pybindings::MakeWebRtcModule(m, "webrtc");
  py::module_ websockets = pybindings::MakeWebsocketsModule(m, "websockets");

  m.def("run_threadsafe_if_coroutine", &pybindings::RunThreadsafeIfCoroutine,
        py::arg("function_call_result"), py::arg_v("loop", py::none()),
        py::arg_v("return_future", false), pybindings::keep_event_loop_memo());
}

}  // namespace act
