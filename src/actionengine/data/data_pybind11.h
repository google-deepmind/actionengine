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

#ifndef ACTIONENGINE_PYBIND11_ACTIONENGINE_DATA_H_
#define ACTIONENGINE_PYBIND11_ACTIONENGINE_DATA_H_

#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/data/serialization.h"
#include "actionengine/util/status_macros.h"

namespace act {
namespace py = ::pybind11;

namespace pybindings {

void EnsureMimetypeAssociations(SerializerRegistry* registry);

py::dict GetMimetypeToTypeDict(SerializerRegistry* registry);

py::dict GetTypeToMimetypeDict(SerializerRegistry* registry);

py::dict GetGlobalTypeToMimetype();

struct PySerializationArgs {
  py::handle object;
  std::string_view mimetype;
};

template <typename T>
concept PyObjectEgltConvertsTo = requires(py::handle obj) {
  {
    EgltAssignInto(std::move(obj), std::declval<T*>())
  } -> std::same_as<absl::Status>;
};

using PyObjectToStdAnyCaster =
    std::function<absl::StatusOr<std::any>(py::handle)>;

template <typename T>
PyObjectToStdAnyCaster MakeDefaultPyObjectToStdAnyCaster() {
  return [](py::handle obj) -> absl::StatusOr<std::any> {
    if (obj.is_none()) {
      return absl::InvalidArgumentError(
          "Cannot convert None to a C++ type. Please provide a valid object.");
    }

    try {
      auto pybind11_conversion = py::cast<T>(obj);
      return std::any(std::move(pybind11_conversion));
    } catch (const py::cast_error& e) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Failed to cast object to ", typeid(T).name(), ": ", e.what()));
    }
  };
}

template <typename T>
PyObjectToStdAnyCaster MakeDefaultPyObjectToStdAnyCaster()
    requires(PyObjectEgltConvertsTo<T>) {
  return [](py::handle obj) -> absl::StatusOr<std::any> {
    if (obj.is_none()) {
      return absl::InvalidArgumentError(
          "Cannot convert None to a C++ type. Please provide a valid object.");
    }

    auto act_provided_conversion = act::ConvertTo<T>(obj);
    if (act_provided_conversion.ok()) {
      return std::any(std::move(*act_provided_conversion));
    }

    try {
      auto pybind11_conversion = py::cast<T>(std::move(obj));
      return std::any(std::move(pybind11_conversion));
    } catch (const py::cast_error& e) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Failed to cast object to ", typeid(T).name(), ": ", e.what()));
    }
  };
}

absl::flat_hash_map<std::string, PyObjectToStdAnyCaster>&
GetCastersForMimetypes();

absl::StatusOr<std::any> CastPyObjectToAny(py::handle obj,
                                           std::string_view mimetype = "");

absl::Status EgltAssignInto(PySerializationArgs args, std::any* dest);

}  // namespace pybindings
}  // namespace act

namespace act {
absl::Status EgltAssignInto(const py::handle& obj, Chunk* chunk);
}  // namespace act

namespace pybind11 {
absl::Status EgltAssignInto(pybind11::handle obj, std::string* dest);
}  // namespace pybind11

namespace act::pybindings {

absl::StatusOr<Chunk> PyToChunk(py::handle obj, std::string_view mimetype = "",
                                SerializerRegistry* registry = nullptr);

absl::StatusOr<py::object> PyFromChunk(
    Chunk chunk, std::string_view mimetype = "",
    const SerializerRegistry* registry = nullptr);

namespace py = pybind11;

void BindChunkMetadata(py::handle scope,
                       std::string_view name = "ChunkMetadata");

void BindChunk(py::handle scope, std::string_view name = "Chunk");

void BindNodeFragment(py::handle scope, std::string_view name = "NodeFragment");

void BindPort(py::handle scope, std::string_view name = "Port");

void BindActionMessage(py::handle scope,
                       std::string_view name = "ActionMessage");

void BindSessionMessage(py::handle scope,
                        std::string_view name = "SessionMessage");

void BindSerializerRegistry(py::handle scope,
                            std::string_view name = "SerializerRegistry");

py::module_ MakeDataModule(py::module_ scope,
                           std::string_view module_name = "data");
}  // namespace act::pybindings

#endif  // ACTIONENGINE_PYBIND11_ACTIONENGINE_DATA_H_
