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

#include "eglt/service/service_pybind11.h"

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <absl/log/check.h>
#include <absl/strings/str_cat.h>
#include <bytearrayobject.h>
#include <listobject.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>

#include "eglt/actions/action.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/async_node.h"
#include "eglt/nodes/node_map.h"
#include "eglt/pybind11_headers.h"
#include "eglt/service/service.h"
#include "eglt/service/session.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

/// @private
void BindStream(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<WireStream, std::shared_ptr<WireStream>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("send", &WireStream::Send, py::call_guard<py::gil_scoped_release>())
      .def("receive", &WireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &WireStream::Accept)
      .def("start", &WireStream::Start)
      .def("close", &WireStream::HalfClose)
      .def("get_status", &WireStream::GetStatus)
      .def("get_id", &WireStream::GetId);

  py::class_<PyWireStream, WireStream, std::shared_ptr<PyWireStream>>(
      scope, name_str.c_str())
      .def(py::init<>(), pybindings::keep_event_loop_memo())
      .def(MakeSameObjectRefConstructor<PyWireStream>())
      .def("send", &PyWireStream::Send, py::arg("message"),
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &PyWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &PyWireStream::Accept)
      .def("start", &PyWireStream::Start)
      .def("close", &PyWireStream::HalfClose)
      .def("get_last_send_status", &PyWireStream::GetStatus)
      .def("get_id", &PyWireStream::GetId);
}

/// @private
void BindSession(py::handle scope, std::string_view name) {
  py::class_<Session, std::shared_ptr<Session>>(scope,
                                                std::string(name).c_str())
      .def(py::init([](NodeMap* node_map = nullptr,
                       ActionRegistry* action_registry = nullptr) {
             return std::make_shared<Session>(node_map, action_registry);
           }),
           py::arg("node_map"), py::arg_v("action_registry", nullptr))
      .def(MakeSameObjectRefConstructor<Session>())
      .def(
          "get_node",
          [](const std::shared_ptr<Session>& self, const std::string_view id,
             const ChunkStoreFactory& chunk_store_factory = {}) {
            return ShareWithNoDeleter(self->GetNode(id, chunk_store_factory));
          },
          py::arg_v("id", ""), py::arg_v("chunk_store_factory", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "dispatch_from",
          [](const std::shared_ptr<Session>& self,
             const std::shared_ptr<WireStream>& stream) {
            self->DispatchFrom(stream);
          },
          py::keep_alive<1, 2>(), py::call_guard<py::gil_scoped_release>())
      .def(
          "stop_dispatching_from",
          [](const std::shared_ptr<Session>& self, WireStream* stream) {
            self->StopDispatchingFrom(stream);
          },
          py::call_guard<py::gil_scoped_release>())
      .def("dispatch_message", &Session::DispatchMessage,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "get_node_map",
          [](const std::shared_ptr<Session>& self) {
            return ShareWithNoDeleter(self->GetNodeMap());
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_action_registry",
          [](const std::shared_ptr<Session>& self) {
            return ShareWithNoDeleter(self->GetActionRegistry());
          },
          py::call_guard<py::gil_scoped_release>())
      .def("set_action_registry", &Session::SetActionRegistry,
           py::arg("action_registry"),
           py::call_guard<py::gil_scoped_release>());
}

/// @private
void BindService(py::handle scope, std::string_view name) {
  py::class_<Service, std::shared_ptr<Service>>(scope,
                                                std::string(name).c_str())
      .def(py::init([](ActionRegistry* action_registry = nullptr,
                       ConnectionHandler connection_handler =
                           RunSimpleSession) {
             if (connection_handler == nullptr) {
               connection_handler = RunSimpleSession;
             }
             return std::make_shared<Service>(action_registry,
                                              std::move(connection_handler));
           }),
           py::arg("action_registry"),
           py::arg_v("connection_handler", py::none()))
      .def(
          "get_stream",
          [](const std::shared_ptr<Service>& self,
             const std::string& stream_id) {
            return ShareWithNoDeleter(self->GetStream(stream_id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_session",
          [](const std::shared_ptr<Service>& self,
             const std::string& session_id) {
            return ShareWithNoDeleter(self->GetSession(session_id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_session_keys", &Service::GetSessionKeys,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "establish_connection",
          [](const std::shared_ptr<Service>& self,
             const std::shared_ptr<PyWireStream>& stream) {
            return self->EstablishConnection(stream);
          },
          py::call_guard<py::gil_scoped_release>())
      .def("join_connection", &Service::JoinConnection,
           py::call_guard<py::gil_scoped_release>())
      .def("set_action_registry", &Service::SetActionRegistry,
           py::arg("action_registry"));
}

/// @private
void BindStreamToSessionConnection(py::handle scope, std::string_view name) {
  py::class_<StreamToSessionConnection,
             std::shared_ptr<StreamToSessionConnection>>(
      scope, std::string(name).c_str())
      .def(py::init(
               [](const std::shared_ptr<WireStream>& stream, Session* session) {
                 return std::make_shared<StreamToSessionConnection>(stream,
                                                                    session);
               }),
           py::arg("stream"), py::arg("session"))
      .def(MakeSameObjectRefConstructor<StreamToSessionConnection>())
      .def("get_stream",
           [](const StreamToSessionConnection& self) {
             return ShareWithNoDeleter(
                 dynamic_cast<PyWireStream*>(self.stream.get()));
           })
      .def("get_session",
           [](const StreamToSessionConnection& self) {
             return ShareWithNoDeleter(self.session);
           })
      .def_readwrite("stream_id", &StreamToSessionConnection::stream_id)
      .def_readwrite("session_id", &StreamToSessionConnection::session_id);
}

/// @private
pybind11::module_ MakeServiceModule(pybind11::module_ scope,
                                    std::string_view module_name) {
  pybind11::module_ service = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine Service interface.");

  BindStream(service, "WireStream");
  BindSession(service, "Session");
  BindService(service, "Service");
  BindStreamToSessionConnection(service, "StreamToSessionConnection");

  return service;
}

}  // namespace eglt::pybindings
