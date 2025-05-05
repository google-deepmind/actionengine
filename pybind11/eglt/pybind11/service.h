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

#ifndef EGLT_PYBIND11_EGLT_SERVICE_H_
#define EGLT_PYBIND11_EGLT_SERVICE_H_

#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/pybind11/pybind11_headers.h"
#include "eglt/pybind11/utils.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindStream(py::handle scope, std::string_view name = "EvergreenStream");
void BindSession(py::handle scope, std::string_view name = "Session");
void BindService(py::handle scope, std::string_view name = "Service");
void BindStreamToSessionConnection(
    py::handle scope, std::string_view name = "StreamToSessionConnection");

class PyEvergreenStream final : public EvergreenStream {
 public:
  using EvergreenStream::EvergreenStream;

  PyEvergreenStream() : EvergreenStream() {}

  absl::Status Send(SessionMessage message) override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "send");

    if (!function) {
      return absl::UnimplementedError(
          "send is not implemented in the Python subclass of "
          "EvergreenStream.");
    }
    const py::object py_result = function(message);

    const absl::StatusOr<py::object> result =
        pybindings::RunThreadsafeIfCoroutine(py_result);

    if (!result.ok()) {
      return result.status();
    }
    return absl::OkStatus();
  }

  std::optional<SessionMessage> Receive() override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "receive");

    if (!function) {
      LOG(FATAL) << "receive is not implemented in the Python subclass of "
                    "EvergreenStream.";
    }
    const py::object py_result = function();

    absl::StatusOr<py::object> result =
        pybindings::RunThreadsafeIfCoroutine(py_result);

    if (!result.ok() || result->is_none()) {
      return std::nullopt;
    }
    return std::move(result)->cast<SessionMessage>();
  }

  absl::Status Accept() override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "accept");

    if (!function) {
      return absl::UnimplementedError(
          "accept is not implemented in the Python subclass of "
          "EvergreenStream.");
    }
    try {
      const py::object py_result = function();
      const absl::StatusOr<py::object> result =
          pybindings::RunThreadsafeIfCoroutine(py_result);

      if (!result.ok()) {
        return result.status();
      }
    } catch (const py::error_already_set& e) {
      return absl::InternalError(e.what());
    }

    return absl::OkStatus();
  }

  absl::Status Start() override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "start");

    if (!function) {
      return absl::UnimplementedError(
          "start is not implemented in the Python subclass of "
          "EvergreenStream.");
    }
    try {
      const py::object py_result = function();
      const absl::StatusOr<py::object> result =
          pybindings::RunThreadsafeIfCoroutine(py_result);

      if (!result.ok()) {
        return result.status();
      }
    } catch (const py::error_already_set& e) {
      return absl::InternalError(e.what());
    }

    return absl::OkStatus();
  }

  void HalfClose() override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyEvergreenStream, "close", Close, );
  }

  absl::Status GetStatus() const override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyEvergreenStream, "get_status",
                                GetStatus, );
  }

  [[nodiscard]] py::object GetLoop() const {
    PYBIND11_OVERRIDE_PURE_NAME(py::object, PyEvergreenStream, "get_loop",
                                GetLoop, );
  }

  [[nodiscard]] std::string GetId() const override {
    PYBIND11_OVERRIDE_PURE_NAME(std::string, PyEvergreenStream, "get_id",
                                GetId, );
  }
};

/// @private
py::module_ MakeServiceModule(py::module_ scope,
                              std::string_view module_name = "service");
}  // namespace eglt::pybindings

#endif  // EGLT_PYBIND11_EGLT_SERVICE_H_
