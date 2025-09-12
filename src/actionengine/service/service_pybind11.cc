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

#include "actionengine/service/service_pybind11.h"

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <Python.h>
#include <absl/log/check.h>
#include <absl/strings/str_cat.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/actions/registry.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/service.h"
#include "actionengine/service/session.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindStream(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::classh<WireStream>(scope, absl::StrCat(name, "VirtualBase").c_str(),
                         py::release_gil_before_calling_cpp_dtor())
      .def("send", &WireStream::Send, py::call_guard<py::gil_scoped_release>())
      .def(
          "receive",
          [](const std::shared_ptr<WireStream>& self,
             double timeout) -> absl::StatusOr<std::optional<WireMessage>> {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);
            return self->Receive(timeout_duration);
          },
          py::arg_v("timeout", -1.0), py::call_guard<py::gil_scoped_release>())
      .def("accept", &WireStream::Accept)
      .def("start", &WireStream::Start)
      .def("half_close", &WireStream::HalfClose)
      .def("abort", &WireStream::Abort,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &WireStream::GetStatus)
      .def("get_id", &WireStream::GetId);

  py::classh<PyWireStream, WireStream>(scope, name_str.c_str())
      .def(py::init<>(), pybindings::keep_event_loop_memo())
      .def(MakeSameObjectRefConstructor<PyWireStream>())
      .def("send", &PyWireStream::Send, py::arg("message"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "receive",
          [](const std::shared_ptr<PyWireStream>& self,
             double timeout) -> absl::StatusOr<std::optional<WireMessage>> {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);
            return self->Receive(timeout_duration);
          },
          py::arg_v("timeout", -1.0), py::call_guard<py::gil_scoped_release>())
      .def(
          "accept",
          [](const std::shared_ptr<PyWireStream>& self) {
            return self->Accept();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "start",
          [](const std::shared_ptr<PyWireStream>& self) {
            return self->Start();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("half_close", &PyWireStream::HalfClose)
      .def("abort", &PyWireStream::Abort,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &PyWireStream::GetStatus)
      .def("get_id", &PyWireStream::GetId);
}

void BindSession(py::handle scope, std::string_view name) {
  py::classh<Session>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
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
             const std::shared_ptr<WireStream>& stream,
             std::function<void()> on_done = {}) {
            self->DispatchFrom(stream, std::move(on_done));
          },
          py::keep_alive<1, 2>(), py::arg("stream"),
          py::arg_v("on_done", py::none()),
          py::call_guard<py::gil_scoped_release>())
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

void BindService(py::handle scope, std::string_view name) {
  py::classh<Service>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
      .def(
          py::init([](ActionRegistry* action_registry = nullptr,
                      ConnectionHandler connection_handler = RunSimpleSession) {
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

void BindStreamToSessionConnection(py::handle scope, std::string_view name) {
  py::classh<StreamToSessionConnection>(scope, std::string(name).c_str())
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

}  // namespace act::pybindings
