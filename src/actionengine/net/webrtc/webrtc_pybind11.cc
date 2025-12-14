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

#include "actionengine/net/webrtc/webrtc_pybind11.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <Python.h>
#include <absl/base/nullability.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_format.h>
#include <abstract.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>  // IWYU pragma: keep
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/net/stream.h"
#include "actionengine/net/webrtc/server.h"
#include "actionengine/net/webrtc/wire_stream.h"
#include "actionengine/service/service.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_abseil/status_caster.h"
#include "pybind11_abseil/statusor_caster.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindTurnServer(py::handle scope, std::string_view name) {
  py::classh<net::TurnServer>(scope, std::string(name).c_str(),
                              "A TURN server configuration.")
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
  py::classh<net::RtcConfig>(scope, std::string(name).c_str(),
                             "A WebRTC configuration.")
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
  py::classh<net::WebRtcWireStream, WireStream>(
      scope, std::string(name).c_str(),
      py::release_gil_before_calling_cpp_dtor())
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
  py::classh<net::WebRtcServer>(scope, std::string(name).c_str(),
                                "A WebRtcServer interface.",
                                py::release_gil_before_calling_cpp_dtor())
      .def_static(
          "create",
          [](Service* absl_nonnull service, std::string_view address,
             uint16_t port, std::string_view identity,
             std::string_view signalling_url, net::RtcConfig rtc_config)
              -> absl::StatusOr<std::shared_ptr<net::WebRtcServer>> {
            ASSIGN_OR_RETURN(auto ws_signalling_url,
                             net::WsUrl::FromString(signalling_url));
            return std::make_shared<net::WebRtcServer>(
                service, address, port, identity, ws_signalling_url,
                std::move(rtc_config));
          },
          py::arg("service"), py::arg_v("address", "0.0.0.0"),
          py::arg_v("port", 20000), py::arg_v("identity", "server"),
          py::arg_v("signalling_url", "wss://actionengine.dev:19001"),
          py::arg_v("rtc_config", net::RtcConfig{}),
          pybindings::keep_event_loop_memo())
      .def("set_signalling_header", &net::WebRtcServer::SetSignallingHeader,
           py::arg("key"), py::arg("value"))
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
         std::string_view signalling_address,
         const absl::flat_hash_map<std::string, std::string>& headers,
         std::optional<uint16_t> port)
          -> absl::StatusOr<std::shared_ptr<net::WebRtcWireStream>> {
        std::string signalling_url = absl::StrCat(signalling_address);
        if (port.has_value()) {
          absl::StrAppend(&signalling_url, ":", *port);
        }
        if (!signalling_url.starts_with("ws://") &&
            !signalling_url.starts_with("wss://")) {
          if (port == 443) {
            signalling_url = absl::StrCat("wss://", signalling_url);
          } else {
            signalling_url = absl::StrCat("ws://", signalling_url);
          }
        }
        ASSIGN_OR_RETURN(std::unique_ptr<net::WebRtcWireStream> stream,
                         net::StartStreamWithSignalling(
                             identity, peer_identity, signalling_url, headers));
        return stream;
      },
      py::arg_v("identity", "client"), py::arg_v("peer_identity", "server"),
      py::arg_v("signalling_address", "wss://actionengine.dev:19001"),
      py::arg_v("headers", absl::flat_hash_map<std::string, std::string>{}),
      py::arg_v("port", std::nullopt),
      py::call_guard<py::gil_scoped_release>());

  return webrtc;
}

}  // namespace act::pybindings