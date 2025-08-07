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

#ifndef EGLT_NET_WEBRTC_WEBRTC_PYBIND11_H_
#define EGLT_NET_WEBRTC_WEBRTC_PYBIND11_H_

#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindTurnServer(py::handle scope, std::string_view name = "TurnServer");

void BindRtcConfig(py::handle scope, std::string_view name = "RtcConfig");

void BindWebRtcWireStream(py::handle scope,
                          std::string_view name = "WebRtcWireStream");

void BindWebRtcActionEngineServer(
    py::handle scope, std::string_view name = "WebRtcActionEngineServer");

py::module_ MakeWebRtcModule(py::module_ scope,
                             std::string_view module_name = "webrtc");

}  // namespace eglt::pybindings

#endif  // EGLT_NET_WEBRTC_WEBRTC_PYBIND11_H_