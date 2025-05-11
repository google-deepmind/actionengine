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

#include <string>

#include "eglt/actions/actions_pybind11.h"
#include "eglt/stores/chunk_store_pybind11.h"
#include "eglt/nodes/nodes_pybind11.h"
#include "eglt/pybind11_headers.h"
#include "eglt/service/service_pybind11.h"
#include "eglt/data/types_pybind11.h"
#include "eglt/sdk/serving/websockets_pybind11.h"

namespace eglt {

namespace py = ::pybind11;

/// @private
PYBIND11_MODULE(evergreen_pybind11, m) {
  // pybind11::google::ImportStatusModule();
  // pybind11_protobuf::ImportNativeProtoCasters();
  absl::InstallFailureSignalHandler({});

  py::module_ types = pybindings::MakeTypesModule(m, "types");
  py::module_ chunk_store = pybindings::MakeChunkStoreModule(m, "chunk_store");
  pybindings::BindNodeMap(m, "NodeMap");
  pybindings::BindAsyncNode(m, "AsyncNode");

  py::module_ actions = pybindings::MakeActionsModule(m, "actions");
  py::module_ service = pybindings::MakeServiceModule(m, "service");
  py::module_ websockets = pybindings::MakeWebsocketsModule(m, "websockets");

  m.def("say_hello",
        [](const std::string& name) { py::print("Hello, " + name); });
}

} // namespace eglt
