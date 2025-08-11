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

#include "actionengine/util/utils_pybind11.h"

#include <string_view>
#include <utility>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <pybind11/cast.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

namespace act::pybindings {
namespace py = ::pybind11;

py::object& GetGloballySavedEventLoop() {
  static py::object* global_event_loop_object = new py::none();
  return *global_event_loop_object;
}

void SaveEventLoopGlobally(const py::object& loop) {
  GetGloballySavedEventLoop() = loop;
}

void SaveFirstEncounteredEventLoop() {
  if (py::object& global_event_loop_object = GetGloballySavedEventLoop();
      global_event_loop_object.is_none()) {
    try {
      global_event_loop_object =
          py::module_::import("asyncio").attr("get_running_loop")();
    } catch (py::error_already_set&) {
      // No event loop was found.
    }
  }
}

absl::StatusOr<py::object> RunThreadsafeIfCoroutine(
    py::object function_call_result, py::object loop, bool return_future) {
  if (const py::function iscoroutine =
          py::module_::import("inspect").attr("iscoroutine");
      !iscoroutine(function_call_result)) {
    return function_call_result;
  }

  py::object resolved_loop = std::move(loop);

  if (resolved_loop.is_none()) {
    resolved_loop = GetGloballySavedEventLoop();
  }

  if (resolved_loop.is_none()) {
    return absl::FailedPreconditionError(
        "No asyncio loop was explicitly provided or could be deduced from "
        "previous library calls. Please provide an asyncio loop explicitly.");
  }

  py::object running_loop = py::none();
  try {
    running_loop = py::module_::import("asyncio").attr("get_running_loop")();
    if (const py::function equals = py::module_::import("operator").attr("eq");
        equals(running_loop, resolved_loop)) {
      return absl::FailedPreconditionError(
          "Target event loop resolves to the current thread's loop, which is "
          "not an intended use for asyncio.run_coroutine_threadsafe. Please "
          "provide a different event loop if you intend to use this "
          "function from an async context, or use it from a sync context.");
    }
  } catch (py::error_already_set&) {}

  const py::function run_coroutine_threadsafe =
      py::module_::import("asyncio").attr("run_coroutine_threadsafe");
  const py::object future =
      run_coroutine_threadsafe(function_call_result, resolved_loop);
  if (return_future) {
    return future;
  }

  return future.attr("result")();
}
}  // namespace act::pybindings
