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

#include "eglt/pybind11/websockets.h"

#include "eglt/sdk/serving/websockets.h"
#include "eglt/service/service.h"
#include "eglt/service/session.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindWebsocketEvergreenStream(py::handle scope, std::string_view name) {
  py::class_<sdk::WebsocketEvergreenStream, base::EvergreenStream,
             std::shared_ptr<sdk::WebsocketEvergreenStream>>(
      scope, std::string(name).c_str())
      .def("send", &sdk::WebsocketEvergreenStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &sdk::WebsocketEvergreenStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &sdk::WebsocketEvergreenStream::Accept)
      .def("start", &sdk::WebsocketEvergreenStream::Start)
      .def("close", &sdk::WebsocketEvergreenStream::HalfClose)
      .def("get_last_send_status",
           &sdk::WebsocketEvergreenStream::GetLastSendStatus)
      .def("get_id", &sdk::WebsocketEvergreenStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<sdk::WebsocketEvergreenStream>& self) {
             return absl::StrFormat("%v", *self);
           })
      .doc() = "A WebsocketEvergreenStream interface.";
}

void BindWebsocketEvergreenServer(py::handle scope, std::string_view name) {
  py::class_<sdk::WebsocketEvergreenServer,
             std::shared_ptr<sdk::WebsocketEvergreenServer>>(
      scope, std::string(name).c_str(), "A WebsocketEvergreenServer interface.")
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port) {
             return std::make_shared<sdk::WebsocketEvergreenServer>(
                 service, address, port);
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000))
      .def("run", &sdk::WebsocketEvergreenServer::Run)
      .def("cancel",
           [](const std::shared_ptr<sdk::WebsocketEvergreenServer>& self) {
             if (const absl::Status status = self->Cancel(); !status.ok()) {
               throw std::runtime_error(status.ToString());
             }
           })
      .def("join",
           [](const std::shared_ptr<sdk::WebsocketEvergreenServer>& self) {
             if (const absl::Status status = self->Join(); !status.ok()) {
               throw std::runtime_error(status.ToString());
             }
           })
      .doc() = "A WebsocketEvergreenServer interface.";
}

void BindWebsocketEvergreenClient(py::handle scope, std::string_view name) {}

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name) {
  pybind11::module_ websockets = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen Websocket interface.");

  BindWebsocketEvergreenStream(websockets, "WebsocketEvergreenStream");
  BindWebsocketEvergreenServer(websockets, "WebsocketEvergreenServer");

  websockets.def(
      "make_websocket_evergreen_stream",
      [](std::string_view address, std::string_view target, int32_t port) {
        return std::shared_ptr(
            sdk::MakeWebsocketEvergreenStream(address, port, target));
      },
      py::arg_v("address", "localhost"), py::arg_v("target", "/"),
      py::arg_v("port", 20000));

  return websockets;
}

}  // namespace eglt::pybindings