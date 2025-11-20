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

#ifndef ACTIONENGINE_PYBIND11_ACTIONENGINE_SERVICE_H_
#define ACTIONENGINE_PYBIND11_ACTIONENGINE_SERVICE_H_

#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <absl/base/optimization.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>
#include <pybind11/cast.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/stores/chunk_store.h"           // IWYU pragma: keep
#include "actionengine/stores/chunk_store_pybind11.h"  // IWYU pragma: keep
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindStream(py::handle scope, std::string_view name = "WireStream");
void BindSession(py::handle scope, std::string_view name = "Session");
void BindService(py::handle scope, std::string_view name = "Service");
void BindStreamToSessionConnection(
    py::handle scope, std::string_view name = "StreamToSessionConnection");

/**
 * A Python subclass of `WireStream` that allows for custom implementations of
 * the stream methods in Python.
 *
 * This class is intended to be used as a base class for Python implementations
 * of `WireStream`. It provides default implementations that call the corresponding
 * Python methods, allowing for easy customization. This is a so-called
 * trampoline class in the sense implied <a href="https://pybind11.readthedocs.io/en/stable/advanced/classes.html#overriding-virtual-functions-in-python">by PyBind11</a>.
 *
 * Action Engine uses `pybind11_abseil`'s `Status` bindings, so any absl::Status
 * returned is automatically converted to a Python exception. If the Python
 * method returns a coroutine, the best effort is made to run it in a
 * threadsafe manner, using `pybindings::RunThreadsafeIfCoroutine`. However,
 * this is not guaranteed to work in all cases, so it is recommended to
 * take extra care when implementing the methods in Python and be aware of
 * potential issues with coroutines.
 */
class PyWireStream final : public WireStream {
 public:
  using WireStream::WireStream;

  PyWireStream() : WireStream() {}

  absl::Status Send(WireMessage message) override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "send");

    if (!function) {
      return absl::UnimplementedError(
          "send is not implemented in the Python subclass of "
          "WireStream.");
    }
    const py::object py_result = function(message);

    const absl::StatusOr<py::object> result =
        pybindings::RunThreadsafeIfCoroutine(py_result);

    if (!result.ok()) {
      return result.status();
    }
    return absl::OkStatus();
  }

  absl::StatusOr<std::optional<WireMessage>> Receive(
      absl::Duration timeout) override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "receive");

    if (!function) {
      return absl::UnimplementedError(
          "receive is not implemented in the Python subclass of "
          "WireStream.");
    }
    const py::object py_result = function(absl::ToDoubleSeconds(timeout));

    absl::StatusOr<py::object> result =
        pybindings::RunThreadsafeIfCoroutine(py_result);

    if (!result.ok()) {
      return result.status();
    }

    if (result->is_none()) {

      return std::nullopt;
    }
    return std::move(result)->cast<WireMessage>();
  }

  absl::Status Accept() override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "accept");

    if (!function) {
      return absl::UnimplementedError(
          "accept is not implemented in the Python subclass of "
          "WireStream.");
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
          "WireStream.");
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
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "half_close");

    if (!function) {
      LOG(FATAL) << "half_close is not implemented in the Python subclass of "
                    "WireStream.";
      ABSL_ASSUME(false);
    }
    try {
      const py::object py_result = function();
      const absl::StatusOr<py::object> result =
          pybindings::RunThreadsafeIfCoroutine(py_result);

      if (!result.ok()) {
        LOG(ERROR) << "Error in half_close: " << result.status();
      }
    } catch (const py::error_already_set& e) {
      LOG(ERROR) << "Error in half_close: " << e.what();
    }
  }

  void Abort() override {
    py::gil_scoped_acquire gil;
    const py::function function = py::get_override(this, "abort");

    if (!function) {
      LOG(FATAL) << "abort is not implemented in the Python subclass of "
                    "WireStream.";
      ABSL_ASSUME(false);
    }
    try {
      const py::object py_result = function();
      const absl::StatusOr<py::object> result =
          pybindings::RunThreadsafeIfCoroutine(py_result);

      if (!result.ok()) {
        LOG(ERROR) << "Error in abort: " << result.status();
      }
    } catch (const py::error_already_set& e) {
      LOG(ERROR) << "Error in abort: " << e.what();
    }
  }

  absl::Status GetStatus() const override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyWireStream, "get_status",
                                GetStatus, );
  }

  [[nodiscard]] py::object GetLoop() const {
    PYBIND11_OVERRIDE_PURE_NAME(py::object, PyWireStream, "get_loop",
                                GetLoop, );
  }

  [[nodiscard]] std::string GetId() const override {
    PYBIND11_OVERRIDE_PURE_NAME(std::string, PyWireStream, "get_id", GetId, );
  }
};

py::module_ MakeServiceModule(py::module_ scope,
                              std::string_view module_name = "service");
}  // namespace act::pybindings

#endif  // ACTIONENGINE_PYBIND11_ACTIONENGINE_SERVICE_H_
