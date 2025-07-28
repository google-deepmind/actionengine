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

#ifndef EGLT_PYBIND11_EGLT_DATA_H_
#define EGLT_PYBIND11_EGLT_DATA_H_

#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "eglt/data/serialization.h"
#include "eglt/util/status_macros.h"

namespace eglt {
namespace py = ::pybind11;

namespace pybindings {

inline void EnsureMimetypeAssociations(SerializerRegistry* registry) {
  if (registry->GetUserData() != nullptr) {
    return;  // Already initialized.
  }
  auto data = std::shared_ptr<py::tuple>(new py::tuple(2), [](py::object*) {});
  (*data)[0] = py::dict();  // mimetype to type
  (*data)[1] = py::dict();  // type to mimetype
  // *data = py::make_tuple(py::dict(), py::dict());
  registry->SetUserData(std::move(data));
}

inline py::dict GetMimetypeToTypeDict(SerializerRegistry* registry) {
  EnsureMimetypeAssociations(registry);
  const auto* data = static_cast<py::tuple*>(registry->GetUserData());
  return (*data)[0].cast<py::dict>();
}

inline py::dict GetTypeToMimetypeDict(SerializerRegistry* registry) {
  EnsureMimetypeAssociations(registry);
  const auto* data = static_cast<py::tuple*>(registry->GetUserData());
  return (*data)[1].cast<py::dict>();
}

inline py::dict GetGlobalTypeToMimetype() {
  SerializerRegistry* registry = GetGlobalSerializerRegistryPtr();
  EnsureMimetypeAssociations(registry);
  return GetTypeToMimetypeDict(registry);
}

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

    auto eglt_provided_conversion = eglt::ConvertTo<T>(obj);
    if (eglt_provided_conversion.ok()) {
      return std::any(std::move(*eglt_provided_conversion));
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

inline absl::flat_hash_map<std::string, PyObjectToStdAnyCaster>&
GetCastersForMimetypes() {
  static absl::flat_hash_map<std::string, PyObjectToStdAnyCaster> casters = {
      {"application/octet-stream",
       MakeDefaultPyObjectToStdAnyCaster<std::string>()},
      {"text/plain", MakeDefaultPyObjectToStdAnyCaster<std::string>()},
  };
  return casters;
}

inline absl::StatusOr<std::any> CastPyObjectToAny(
    py::handle obj, std::string_view mimetype = "") {
  if (obj.is_none()) {
    return absl::InvalidArgumentError("Cannot convert None to a C++ type.");
  }

  auto& casters = GetCastersForMimetypes();
  const auto it = casters.find(std::string(mimetype));
  if (it == casters.end()) {
    return std::any(std::move(obj));
  }

  // py::gil_scoped_acquire gil;  // Ensure GIL is held for Python calls.
  return it->second(std::move(obj));
}

inline absl::Status EgltAssignInto(PySerializationArgs args, std::any* dest) {
  auto mimetype_str = std::string(args.mimetype);
  {
    py::gil_scoped_acquire gil;  // Ensure GIL is held for Python calls.
    const py::handle global_type_to_mimetype = GetGlobalTypeToMimetype();

    if (mimetype_str.empty()) {
      mimetype_str =
          global_type_to_mimetype.attr("get")(args.object.get_type(), py::str())
              .cast<std::string>();
    }

    if (mimetype_str.empty()) {
      return absl::InvalidArgumentError(
          "Mimetype must be specified or globally associated with the "
          "object type.");
    }
  }

  absl::StatusOr<std::any> cpp_object =
      CastPyObjectToAny(std::move(args.object), mimetype_str);
  if (!cpp_object.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to convert object: ", cpp_object.status().message()));
  }

  *dest = std::move(*cpp_object);
  return absl::OkStatus();
}

}  // namespace pybindings
}  // namespace eglt

namespace eglt::base {
inline absl::Status EgltAssignInto(const py::handle& obj, Chunk* chunk) {
  const py::dict& global_type_to_mimetype =
      pybindings::GetGlobalTypeToMimetype();

  const auto mimetype =
      global_type_to_mimetype.attr("get")(obj.get_type(), py::str())
          .cast<std::string>();

  if (mimetype.empty()) {
    return absl::InvalidArgumentError(
        "Mimetype must be specified or globally associated with the "
        "object type.");
  }

  absl::StatusOr<Chunk> serialized_chunk =
      ToChunk(std::any(obj), mimetype, &GetGlobalSerializerRegistry());
  if (serialized_chunk.ok()) {
    // There was a serializer for that mimetype that could handle the object in
    // its py::object form (i.e. the serializer was set from Python), so we can
    // just return the serialized chunk.
    *chunk = std::move(*serialized_chunk);
    return absl::OkStatus();
  }

  absl::StatusOr<std::any> cpp_object =
      pybindings::CastPyObjectToAny(obj, mimetype);
  if (!cpp_object.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to convert object: ", cpp_object.status().message()));
  }

  py::gil_scoped_release release;  // Release GIL for serialization.
  serialized_chunk =
      ToChunk(*cpp_object, mimetype, &GetGlobalSerializerRegistry());

  if (!serialized_chunk.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to serialize object to Chunk: ",
                     serialized_chunk.status().message()));
  }

  *chunk = std::move(*serialized_chunk);
  return absl::OkStatus();
}
}  // namespace eglt::base

namespace pybind11 {

inline absl::Status EgltAssignInto(pybind11::handle obj, std::string* dest) {
  if (!pybind11::isinstance<pybind11::str>(obj)) {
    return absl::InvalidArgumentError(
        "Object is not a string. Cannot assign to std::string.");
  }

  try {
    *dest = std::move(obj).cast<std::string>();
    return absl::OkStatus();
  } catch (const pybind11::cast_error& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to cast object to std::string: ", e.what()));
  }
}
}  // namespace pybind11

namespace eglt::pybindings {

inline absl::StatusOr<Chunk> PyToChunk(py::handle obj,
                                       std::string_view mimetype = "",
                                       SerializerRegistry* registry = nullptr) {
  auto mimetype_str = std::string(mimetype);

  if (registry == nullptr) {
    registry = &GetGlobalSerializerRegistry();
  }

  if (mimetype_str.empty()) {
    const auto* data = static_cast<py::tuple*>(registry->GetUserData());
    const auto type_to_mimetype = (*data)[1].cast<py::dict>();
    const auto mro = obj.get_type().attr("__mro__");
    for (const auto& type : mro) {
      mimetype_str =
          type_to_mimetype.attr("get")(type, py::str()).cast<std::string>();
      if (!mimetype_str.empty()) {
        break;  // Found a matching mimetype.
      }
    }
  }

  if (mimetype_str.empty()) {
    {
      py::gil_scoped_release release;  // Release GIL for serialization.
      return ConvertTo<Chunk>(obj);
    }
  }

  absl::StatusOr<Chunk> serialized_chunk;
  {
    py::gil_scoped_release release;
    serialized_chunk = ToChunk(std::any(obj), mimetype_str, registry);
    if (!serialized_chunk.ok()) {
      serialized_chunk =
          ToChunk(ConvertToOrDie<std::any>(pybindings::PySerializationArgs{
                      .object = obj, .mimetype = mimetype_str}),
                  mimetype_str, registry);
    }
  }

  RETURN_IF_ERROR(serialized_chunk.status());
  return *std::move(serialized_chunk);
}

inline absl::StatusOr<py::object> PyFromChunk(
    Chunk chunk, std::string_view mimetype = "",
    const SerializerRegistry* registry = nullptr) {
  absl::StatusOr<std::any> obj;
  {
    py::gil_scoped_release release;  // Release GIL for deserialization.
    obj = FromChunk(std::move(chunk), mimetype, registry);
  }
  RETURN_IF_ERROR(obj.status());

  if (std::any_cast<py::object>(&*obj) == nullptr) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Deserialized object is not a py::object, but a ", obj->type().name(),
        ". Cannot convert to py::object because it's not "
        "implemented yet."));
  }

  return std::any_cast<py::object>(*std::move(obj));
}

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
}  // namespace eglt::pybindings

#endif  // EGLT_PYBIND11_EGLT_DATA_H_
