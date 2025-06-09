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

#include "eglt/net/websockets/websockets_pybind11.h"
#include "eglt/net/websockets/websockets.h"
#include "eglt/service/service.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindWebsocketEvergreenWireStream(py::handle scope, std::string_view name) {
  py::class_<net::WebsocketEvergreenWireStream, EvergreenWireStream,
             std::shared_ptr<net::WebsocketEvergreenWireStream>>(
      scope, std::string(name).c_str())
      .def("send", &net::WebsocketEvergreenWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &net::WebsocketEvergreenWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &net::WebsocketEvergreenWireStream::Accept)
      .def("start", &net::WebsocketEvergreenWireStream::Start)
      .def("close", &net::WebsocketEvergreenWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::WebsocketEvergreenWireStream::GetStatus)
      .def("get_id", &net::WebsocketEvergreenWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<net::WebsocketEvergreenWireStream>& self) {
             return absl::StrFormat("%v", *self);
           })
      .doc() = "A WebsocketEvergreenWireStream interface.";
}

void BindWebsocketEvergreenServer(py::handle scope, std::string_view name) {
  py::class_<net::WebsocketEvergreenServer,
             std::shared_ptr<net::WebsocketEvergreenServer>>(
      scope, std::string(name).c_str(), "A WebsocketEvergreenServer interface.")
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port) {
             return std::make_shared<net::WebsocketEvergreenServer>(
                 service, address, port);
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000), pybindings::keep_event_loop_memo())
      .def("run", &net::WebsocketEvergreenServer::Run,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "cancel",
          [](const std::shared_ptr<net::WebsocketEvergreenServer>& self) {
            if (const absl::Status status = self->Cancel(); !status.ok()) {
              throw std::runtime_error(status.ToString());
            }
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<net::WebsocketEvergreenServer>& self) {
            if (const absl::Status status = self->Join(); !status.ok()) {
              throw std::runtime_error(status.ToString());
            }
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebsocketEvergreenServer interface.";
}

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name) {
  pybind11::module_ websockets = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen Websocket interface.");

  BindWebsocketEvergreenWireStream(websockets, "WebsocketEvergreenWireStream");
  BindWebsocketEvergreenServer(websockets, "WebsocketEvergreenServer");

  websockets.def(
      "make_websocket_evergreen_stream",
      [](std::string_view address, std::string_view target, int32_t port) {
        if (auto stream =
                net::MakeWebsocketEvergreenWireStream(address, port, target);
            !stream.ok()) {
          throw std::runtime_error(stream.status().ToString());
        } else {
          return std::shared_ptr(*std::move(stream));
        }
      },
      py::arg_v("address", "localhost"), py::arg_v("target", "/"),
      py::arg_v("port", 20000));

  return websockets;
}

}  // namespace eglt::pybindings