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

#include <memory>
#include <string>
#include <string_view>

#include <pybind11/attr.h>
#include <pybind11/pybind11.h>

#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/node_map.h"
#include "eglt/pybind11_headers.h"
#include "eglt/service/service.h"
#include "eglt/service/session.h"
#include "eglt/stores/chunk_store_pybind11.h"  // IWYU pragma: keep
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

/// @private
void BindStream(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<EvergreenWireStream, std::shared_ptr<EvergreenWireStream>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("send", &EvergreenWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &EvergreenWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &EvergreenWireStream::Accept)
      .def("start", &EvergreenWireStream::Start)
      .def("close", &EvergreenWireStream::HalfClose)
      .def("get_status", &EvergreenWireStream::GetStatus)
      .def("get_id", &EvergreenWireStream::GetId);

  py::class_<PyEvergreenWireStream, EvergreenWireStream,
             std::shared_ptr<PyEvergreenWireStream>>(scope, name_str.c_str())
      .def(py::init<>(), pybindings::keep_event_loop_memo())
      .def(MakeSameObjectRefConstructor<PyEvergreenWireStream>())
      .def("send", &PyEvergreenWireStream::Send, py::arg("message"),
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &PyEvergreenWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &PyEvergreenWireStream::Accept)
      .def("start", &PyEvergreenWireStream::Start)
      .def("close", &PyEvergreenWireStream::HalfClose)
      .def("get_last_send_status", &PyEvergreenWireStream::GetStatus)
      .def("get_id", &PyEvergreenWireStream::GetId);
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
          py::arg_v("id", ""), py::arg_v("chunk_store_factory", py::none()))
      .def(
          "dispatch_from",
          [](const std::shared_ptr<Session>& self,
             const std::shared_ptr<EvergreenWireStream>& stream) {
            self->DispatchFrom(stream);
          },
          py::keep_alive<1, 2>())
      .def("stop_dispatching_from",
           [](const std::shared_ptr<Session>& self,
              EvergreenWireStream* stream) {
             self->StopDispatchingFrom(stream);
           })
      .def("dispatch_message", &Session::DispatchMessage,
           py::call_guard<py::gil_scoped_release>())
      .def("get_node_map",
           [](const std::shared_ptr<Session>& self) {
             return ShareWithNoDeleter(self->GetNodeMap());
           })
      .def("get_action_registry",
           [](const std::shared_ptr<Session>& self) {
             return ShareWithNoDeleter(self->GetActionRegistry());
           })
      .def("set_action_registry", &Session::SetActionRegistry,
           py::arg("action_registry"));
}

/// @private
void BindService(py::handle scope, std::string_view name) {
  py::class_<Service, std::shared_ptr<Service>>(scope,
                                                std::string(name).c_str())
      .def(py::init([](ActionRegistry* action_registry = nullptr,
                       EvergreenConnectionHandler connection_handler =
                           RunSimpleEvergreenSession) {
             if (connection_handler == nullptr) {
               connection_handler = RunSimpleEvergreenSession;
             }
             return std::make_shared<Service>(action_registry,
                                              std::move(connection_handler));
           }),
           py::arg("action_registry"),
           py::arg_v("connection_handler", py::none()))
      .def("get_stream",
           [](const std::shared_ptr<Service>& self,
              const std::string& stream_id) {
             return ShareWithNoDeleter(self->GetStream(stream_id));
           })
      .def("get_session",
           [](const std::shared_ptr<Service>& self,
              const std::string& session_id) {
             return ShareWithNoDeleter(self->GetSession(session_id));
           })
      .def("get_session_keys", &Service::GetSessionKeys,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "establish_connection",
          [](const std::shared_ptr<Service>& self,
             const std::shared_ptr<PyEvergreenWireStream>& stream) {
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
      .def(py::init([](const std::shared_ptr<EvergreenWireStream>& stream,
                       Session* session) {
             return std::make_shared<StreamToSessionConnection>(stream,
                                                                session);
           }),
           py::arg("stream"), py::arg("session"))
      .def(MakeSameObjectRefConstructor<StreamToSessionConnection>())
      .def("get_stream",
           [](const StreamToSessionConnection& self) {
             return ShareWithNoDeleter(
                 dynamic_cast<PyEvergreenWireStream*>(self.stream.get()));
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
      std::string(module_name).c_str(), "Evergreen Service interface.");

  BindStream(service, "EvergreenWireStream");
  BindSession(service, "Session");
  BindService(service, "Service");
  BindStreamToSessionConnection(service, "StreamToSessionConnection");

  return service;
}

}  // namespace eglt::pybindings
