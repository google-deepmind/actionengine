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

#include "eglt/sdk/webrtc_pybind11.h"
#include "eglt/util/utils_pybind11.h"

#include "eglt/sdk/webrtc.h"
#include "eglt/service/service.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindWebRtcEvergreenWireStream(py::handle scope, std::string_view name) {
  py::class_<sdk::WebRtcEvergreenWireStream, EvergreenWireStream,
             std::shared_ptr<sdk::WebRtcEvergreenWireStream>>(
      scope, std::string(name).c_str())
      .def("send", &sdk::WebRtcEvergreenWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &sdk::WebRtcEvergreenWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &sdk::WebRtcEvergreenWireStream::Accept)
      .def("start", &sdk::WebRtcEvergreenWireStream::Start)
      .def("close", &sdk::WebRtcEvergreenWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &sdk::WebRtcEvergreenWireStream::GetStatus)
      .def("get_id", &sdk::WebRtcEvergreenWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<sdk::WebRtcEvergreenWireStream>& self) {
             return absl::StrFormat("WebRtcEvergreenWireStream, id=%s",
                                    self->GetId());
           })
      .doc() = "A WebRtcEvergreenWireStream interface.";
}

void BindWebRtcEvergreenServer(py::handle scope, std::string_view name) {
  py::class_<sdk::WebRtcEvergreenServer,
             std::shared_ptr<sdk::WebRtcEvergreenServer>>(
      scope, std::string(name).c_str(), "A WebRtcEvergreenServer interface.")
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port, std::string_view signalling_address,
                       uint16_t signalling_port, std::string_view identity) {
             return std::make_shared<sdk::WebRtcEvergreenServer>(
                 service, address, port, signalling_address, signalling_port,
                 identity);
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000),
           py::arg_v("signalling_address", "localhost"),
           py::arg_v("signalling_port", 80), py::arg_v("identity", "server"),
           pybindings::keep_event_loop_memo())
      .def("run", &sdk::WebRtcEvergreenServer::Run,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "cancel",
          [](const std::shared_ptr<sdk::WebRtcEvergreenServer>& self) {
            if (const absl::Status status = self->Cancel(); !status.ok()) {
              throw std::runtime_error(status.ToString());
            }
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<sdk::WebRtcEvergreenServer>& self) {
            if (const absl::Status status = self->Join(); !status.ok()) {
              throw std::runtime_error(status.ToString());
            }
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebRtcEvergreenServer interface.";
}

py::module_ MakeWebRtcModule(py::module_ scope, std::string_view module_name) {
  pybind11::module_ webrtc = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen WebRTC interface.");

  BindWebRtcEvergreenWireStream(webrtc, "WebRtcEvergreenWireStream");
  BindWebRtcEvergreenServer(webrtc, "WebRtcEvergreenServer");

  webrtc.def(
      "make_webrtc_evergreen_stream",
      [](std::string_view identity, std::string_view peer_identity,
         std::string_view signalling_address, uint16_t port) {
        if (auto stream = sdk::StartStreamWithSignalling(
                identity, peer_identity, signalling_address, port);
            !stream.ok()) {
          throw std::runtime_error(stream.status().ToString());
        } else {
          return std::shared_ptr(*std::move(stream));
        }
      },
      py::arg_v("identity", "client"), py::arg_v("peer_identity", "server"),
      py::arg_v("signalling_address", "localhost"),
      py::arg_v("signalling_port", 80));

  return webrtc;
}

}  // namespace eglt::pybindings