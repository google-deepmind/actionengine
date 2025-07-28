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

#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "eglt/net/websockets/websockets.h"
#include "eglt/service/service.h"
#include "eglt/util/status_macros.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindWebsocketWireStream(py::handle scope, std::string_view name) {
  py::class_<net::WebsocketWireStream, WireStream,
             std::shared_ptr<net::WebsocketWireStream>>(
      scope, std::string(name).c_str())
      .def("send", &net::WebsocketWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &net::WebsocketWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &net::WebsocketWireStream::Accept)
      .def("start", &net::WebsocketWireStream::Start)
      .def("close", &net::WebsocketWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::WebsocketWireStream::GetStatus)
      .def("get_id", &net::WebsocketWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<net::WebsocketWireStream>& self) {
             return absl::StrFormat("%v", *self);
           })
      .doc() = "A WebsocketWireStream interface.";
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
            return self->Cancel();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<net::WebsocketEvergreenServer>& self) {
            return self->Join();
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebsocketEvergreenServer interface.";
}

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name) {
  pybind11::module_ websockets = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen Websocket interface.");

  BindWebsocketWireStream(websockets, "WebsocketWireStream");
  BindWebsocketEvergreenServer(websockets, "WebsocketEvergreenServer");

  websockets.def(
      "make_websocket_evergreen_stream",
      [](std::string_view address, std::string_view target, int32_t port)
          -> absl::StatusOr<std::shared_ptr<net::WebsocketWireStream>> {
        ASSIGN_OR_RETURN(std::unique_ptr<net::WebsocketWireStream> stream,
                         net::MakeWebsocketWireStream(address, port, target));
        return stream;
      },
      py::arg_v("address", "localhost"), py::arg_v("target", "/"),
      py::arg_v("port", 20000));

  return websockets;
}

}  // namespace eglt::pybindings