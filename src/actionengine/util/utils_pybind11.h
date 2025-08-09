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

#ifndef ACTIONENGINE_PYBIND11_ACTIONENGINE_UTILS_H_
#define ACTIONENGINE_PYBIND11_ACTIONENGINE_UTILS_H_

#include <memory>

#include <absl/status/statusor.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

namespace act::pybindings {

namespace py = ::pybind11;

/**
 * \brief
 *   Creates a constructor that returns a Python object with the same content,
 *   but of a different [derived] type.
 *
 * This function is used to create a constructor that takes a shared_ptr<T>
 * and returns a shared_ptr<T> without copying the object.
 * This is useful to wrap objects of bound classes into Python descendants.
 */
template <typename T>
auto MakeSameObjectRefConstructor() {
  return py::init([](const std::shared_ptr<T>& other) { return other; });
}

/**
 * \brief
 *   Creates a shared_ptr<T> that does not delete the object when it goes out of
 *   scope.
 *
 * This is useful to guarantee that Python will never try to delete the object
 * passed to it from C++.
 *
 * \tparam T
 *   The type of the object to be exposed.
 * \param ptr
 *   The pointer to the object to be exposed.
 */
template <typename T>
std::shared_ptr<T> ShareWithNoDeleter(T* ptr) {
  return std::shared_ptr<T>(ptr, [](T*) {});
}

/**
 * @brief
 *   Returns the globally saved event loop.
 *
 * This function is used to retrieve the event loop that was saved by
 * SaveEventLoopGlobally() or tracked by calls to bindings annotated with
 * keep_event_loop_memo.
 *
 * @return
 *   The globally saved event loop, or a py::none if no event loop was saved.
 */
py::object& GetGloballySavedEventLoop();

/**
 * @brief
 *   Saves the given event loop as a global variable further available to
 *   GetGloballySavedEventLoop().
 *
 * No validation is performed on the event loop, so it is up to the caller to
 * ensure that the event loop is valid and usable.
 *
 * @param loop
 *   The event loop to be saved.
 */
void SaveEventLoopGlobally(const py::object& loop);

void SaveFirstEncounteredEventLoop();

/**
 * \brief
 *   Annotation for PyBind11 functions to indicate that the event loop should be
 *   tracked and saved as a global variable.
 *
 * This is primarily useful to resolve the event loop when an async overload
 * is called from a sync context (which is ideally always the case for bound
 * functions).
 *
 * Example:
 * \code
 *   .def(py::init<>(), pybindings::keep_event_loop_memo())
 * \endcode
 *
 * \headerfile actionengine/util/utils_pybind11.h
 */
struct keep_event_loop_memo {};

/**
 * @brief
 *   Runs a coroutine in a threadsafe manner in the given event loop.
 *
 * Calls `asyncio.run_coroutine_threadsafe()` with the given
 * \p function_call_result by directly using the Python interpreter. If no loop
 * is provided, the function will try to deduce the event loop from the
 * global variable first explicitly saved by SaveFirstEncounteredEventLoop(),
 * or tracked by the keep_event_loop_memo PyBind11 annotation. If
 * \p function_call_result is not a coroutine, it will be returned as is before
 * deducing the event loop.
 *
 * This function should not be called from an async context, and will return an
 * error if the event loop resolves to the current thread's loop.
 *
 * @param function_call_result
 *  The coroutine to be run in the event loop, or a non-coroutine object to be
 *  returned as is.
 * @param loop
 *  The event loop to run the coroutine in. If not provided or globally set, the
 *  function will try to deduce it from previous library calls.
 * @return
 *  The result of the coroutine, or the non-coroutine object.
 */
absl::StatusOr<py::object> RunThreadsafeIfCoroutine(
    py::object function_call_result, py::object loop = py::none());

}  // namespace act::pybindings

// implementation of PyBind11's machinery for the keep_event_loop_memo
// annotation.
template <>
struct pybind11::detail::process_attribute<
    act::pybindings::keep_event_loop_memo>
    : process_attribute_default<act::pybindings::keep_event_loop_memo> {
  static void precall(function_call&) {
    act::pybindings::SaveFirstEncounteredEventLoop();
  }
};

#endif  // ACTIONENGINE_PYBIND11_ACTIONENGINE_UTILS_H_
