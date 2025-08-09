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

#ifndef EGLT_PYBIND11_EGLT_WEBSOCKETS_H_
#define EGLT_PYBIND11_EGLT_WEBSOCKETS_H_

#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindWebsocketWireStream(py::handle scope,
                             std::string_view name = "WebsocketWireStream");

void BindWebsocketServer(py::handle scope,
                         std::string_view name = "WebsocketActionEngineServer");

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name = "websockets");

}  // namespace eglt::pybindings

#endif  // EGLT_PYBIND11_EGLT_WEBSOCKETS_H_
