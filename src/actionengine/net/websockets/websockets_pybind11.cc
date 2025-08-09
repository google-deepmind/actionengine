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

#include "actionengine/net/websockets/websockets_pybind11.h"

#include <cstdint>
#include <memory>
#include <string>

#include <Python.h>
#include <absl/base/nullability.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_format.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/net/stream.h"
#include "actionengine/net/websockets/websockets.h"
#include "actionengine/service/service.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindWebsocketWireStream(py::handle scope, std::string_view name) {
  py::classh<net::WebsocketWireStream, WireStream>(scope,
                                                   std::string(name).c_str())
      .def("send", &net::WebsocketWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &net::WebsocketWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &net::WebsocketWireStream::Accept,
           py::call_guard<py::gil_scoped_release>())
      .def("start", &net::WebsocketWireStream::Start,
           py::call_guard<py::gil_scoped_release>())
      .def("half_close", &net::WebsocketWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::WebsocketWireStream::GetStatus)
      .def("get_id", &net::WebsocketWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<net::WebsocketWireStream>& self) {
             return absl::StrFormat("%v", *self);
           })
      .doc() = "A WebsocketWireStream interface.";
}

void BindWebsocketServer(py::handle scope, std::string_view name) {
  py::classh<net::WebsocketServer>(
      scope, std::string(name).c_str(), "A WebsocketServer interface.")
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port) {
             return std::make_shared<net::WebsocketServer>(service, address,
                                                           port);
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000), pybindings::keep_event_loop_memo())
      .def("run", &net::WebsocketServer::Run,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "cancel",
          [](const std::shared_ptr<net::WebsocketServer>& self) {
            return self->Cancel();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<net::WebsocketServer>& self) {
            return self->Join();
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebsocketServer interface.";
}

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name) {
  pybind11::module_ websockets = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine Websocket interface.");

  BindWebsocketWireStream(websockets, "WebsocketWireStream");
  BindWebsocketServer(websockets, "WebsocketServer");

  websockets.def(
      "make_websocket_stream",
      [](std::string_view address, std::string_view target, int32_t port)
          -> absl::StatusOr<std::shared_ptr<net::WebsocketWireStream>> {
        ASSIGN_OR_RETURN(std::unique_ptr<net::WebsocketWireStream> stream,
                         net::MakeWebsocketWireStream(address, port, target));
        return stream;
      },
      py::arg_v("address", "localhost"), py::arg_v("target", "/"),
      py::arg_v("port", 20000), py::call_guard<py::gil_scoped_release>());

  return websockets;
}

}  // namespace act::pybindings