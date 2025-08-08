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

#include "eglt/net/webrtc/webrtc_pybind11.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <absl/base/nullability.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_format.h>
#include <bytearrayobject.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "eglt/net/stream.h"
#include "eglt/net/webrtc/server.h"
#include "eglt/net/webrtc/wire_stream.h"
#include "eglt/service/service.h"
#include "eglt/util/status_macros.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindTurnServer(py::handle scope, std::string_view name) {
  py::class_<net::TurnServer, std::shared_ptr<net::TurnServer>>(
      scope, std::string(name).c_str(), "A TURN server configuration.")
      .def_static(
          "from_string",
          [](std::string_view server) {
            return net::TurnServer::FromString(server);
          },
          py::arg("server"))
      .def_readwrite("hostname", &net::TurnServer::hostname)
      .def_readwrite("port", &net::TurnServer::port)
      .def_readwrite("username", &net::TurnServer::username)
      .def_readwrite("password", &net::TurnServer::password)
      .def("__repr__",
           [](const net::TurnServer& self) {
             return absl::StrFormat("TurnServer(%s:%d)", self.hostname,
                                    self.port);
           })
      .doc() = "A TURN server configuration.";
}

void BindRtcConfig(py::handle scope, std::string_view name) {
  py::class_<net::RtcConfig, std::shared_ptr<net::RtcConfig>>(
      scope, std::string(name).c_str(), "A WebRTC configuration.")
      .def(py::init([]() { return net::RtcConfig{}; }))
      .def_readwrite("max_message_size", &net::RtcConfig::max_message_size,
                     "The maximum message size for WebRTC data channels.")
      .def_readwrite("port_range_begin", &net::RtcConfig::port_range_begin,
                     "The beginning of the port range for WebRTC connections.")
      .def_readwrite("port_range_end", &net::RtcConfig::port_range_end,
                     "The end of the port range for WebRTC connections.")
      .def_readwrite("enable_ice_udp_mux", &net::RtcConfig::enable_ice_udp_mux,
                     "Whether to enable ICE UDP multiplexing.")
      .def_readwrite("stun_servers", &net::RtcConfig::stun_servers,
                     "A list of STUN servers to use for WebRTC connections.")
      .def_readwrite("turn_servers", &net::RtcConfig::turn_servers,
                     "A list of TURN servers to use for WebRTC connections.")
      .def("__repr__",
           [](const net::RtcConfig& self) {
             return absl::StrFormat("RtcConfig(turn_servers=%zu)",
                                    self.turn_servers.size());
           })
      .doc() = "A WebRTC configuration.";
}

void BindWebRtcWireStream(py::handle scope, std::string_view name) {
  py::class_<net::WebRtcWireStream, WireStream,
             std::shared_ptr<net::WebRtcWireStream>>(scope,
                                                     std::string(name).c_str())
      .def("send", &net::WebRtcWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def("receive", &net::WebRtcWireStream::Receive,
           py::call_guard<py::gil_scoped_release>())
      .def("accept", &net::WebRtcWireStream::Accept,
           py::call_guard<py::gil_scoped_release>())
      .def("start", &net::WebRtcWireStream::Start,
           py::call_guard<py::gil_scoped_release>())
      .def("half_close", &net::WebRtcWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("abort", &net::WebRtcWireStream::Abort,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::WebRtcWireStream::GetStatus)
      .def("get_id", &net::WebRtcWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<net::WebRtcWireStream>& self) {
             return absl::StrFormat("WebRtcWireStream, id=%s", self->GetId());
           })
      .doc() = "A WebRtcWireStream interface.";
}

void BindWebRtcServer(py::handle scope, std::string_view name) {
  py::class_<net::WebRtcServer, std::shared_ptr<net::WebRtcServer>>(
      scope, std::string(name).c_str(), "A WebRtcServer interface.")
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port, std::string_view signalling_address,
                       uint16_t signalling_port, std::string_view identity,
                       net::RtcConfig rtc_config) {
             return std::make_shared<net::WebRtcServer>(
                 service, address, port, signalling_address, signalling_port,
                 identity, std::move(rtc_config));
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000),
           py::arg_v("signalling_address", "localhost"),
           py::arg_v("signalling_port", 80), py::arg_v("identity", "server"),
           py::arg_v("rtc_config", net::RtcConfig{}),
           pybindings::keep_event_loop_memo())
      .def("run", &net::WebRtcServer::Run,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "cancel",
          [](const std::shared_ptr<net::WebRtcServer>& self) {
            return self->Cancel();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<net::WebRtcServer>& self) {
            return self->Join();
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebRtcServer interface.";
}

py::module_ MakeWebRtcModule(py::module_ scope, std::string_view module_name) {
  pybind11::module_ webrtc = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine WebRTC interface.");

  BindTurnServer(webrtc, "TurnServer");
  BindRtcConfig(webrtc, "RtcConfig");

  BindWebRtcWireStream(webrtc, "WebRtcWireStream");
  BindWebRtcServer(webrtc, "WebRtcServer");

  webrtc.def(
      "make_webrtc_stream",
      [](std::string_view identity, std::string_view peer_identity,
         std::string_view signalling_address, uint16_t port)
          -> absl::StatusOr<std::shared_ptr<net::WebRtcWireStream>> {
        ASSIGN_OR_RETURN(
            std::unique_ptr<net::WebRtcWireStream> stream,
            net::StartStreamWithSignalling(identity, peer_identity,
                                           signalling_address, port));
        return stream;
      },
      py::arg_v("identity", "client"), py::arg_v("peer_identity", "server"),
      py::arg_v("signalling_address", "localhost"),
      py::arg_v("signalling_port", 80),
      py::call_guard<py::gil_scoped_release>());

  return webrtc;
}

}  // namespace eglt::pybindings