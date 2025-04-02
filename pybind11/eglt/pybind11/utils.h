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

#ifndef EGLT_PYBIND11_EGLT_UTILS_H_
#define EGLT_PYBIND11_EGLT_UTILS_H_

#include <memory>

#include <pybind11/pybind11.h>

namespace eglt::pybindings {

namespace py = ::pybind11;

template <typename T>
auto MakeSameObjectRefConstructor() {
  return py::init([](const std::shared_ptr<T>& other) { return other; });
}

template <typename T>
std::shared_ptr<T> ShareWithNoDeleter(T* ptr) {
  return std::shared_ptr<T>(ptr, [](T*) {});
}

py::object& GetGloballySavedEventLoop();

void SaveFirstEncounteredEventLoop();

// this annotation is used to make bound functions or methods save a reference
// to the first encountered (on their calls) event loop. This is primarily
// useful to resolve the event loop when an async overload is called from a sync
// context (which is ideally always the case for bound functions).
struct keep_event_loop_memo {};

absl::StatusOr<py::object> RunThreadsafeIfCoroutine(
    py::object function_call_result, py::object loop = py::none());

}  // namespace eglt::pybindings

// implementation of PyBind11's machinery for the keep_event_loop_memo
// annotation.
template <>
struct pybind11::detail::process_attribute<
    eglt::pybindings::keep_event_loop_memo>
    : process_attribute_default<eglt::pybindings::keep_event_loop_memo> {
  static void precall(function_call&) {
    eglt::pybindings::SaveFirstEncounteredEventLoop();
  }
};

#endif  // EGLT_PYBIND11_EGLT_UTILS_H_
