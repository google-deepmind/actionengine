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

#include "eglt/data/serialization.h"

namespace eglt {
namespace py = ::pybind11;

namespace pybindings {
inline py::dict& GetGlobalMimetypeAssociations() {
  static auto associations = new py::dict();
  return *associations;
}

struct PySerializationArgs {
  py::object object;
  std::string_view mimetype;
};

template <typename T>
concept PyObjectEgltConvertsTo = requires(py::object obj) {
  {
    EgltAssignInto(std::move(obj), std::declval<T*>())
  } -> std::same_as<absl::Status>;
};

using PyObjectToStdAnyCaster =
    std::function<absl::StatusOr<std::any>(py::object)>;

template <typename T>
PyObjectToStdAnyCaster MakeDefaultPyObjectToStdAnyCaster() {
  return [](py::object obj) -> absl::StatusOr<std::any> {
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
  return [](py::object obj) -> absl::StatusOr<std::any> {
    if (obj.is_none()) {
      return absl::InvalidArgumentError(
          "Cannot convert None to a C++ type. Please provide a valid object.");
    }

    auto eglt_provided_conversion = eglt::StatusOrConvertTo<T>(obj);
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
    py::object obj, std::string_view mimetype = "") {
  if (obj.is_none()) {
    return absl::InvalidArgumentError("Cannot convert None to a C++ type.");
  }

  auto& casters = GetCastersForMimetypes();
  const auto it = casters.find(std::string(mimetype));
  if (it == casters.end()) {
    return std::any(std::move(obj));
  }

  return it->second(std::move(obj));
}

inline absl::Status EgltAssignInto(PySerializationArgs args, std::any* dest) {
  auto mimetype_str = std::string(args.mimetype);
  const auto& associations = GetGlobalMimetypeAssociations();

  if (mimetype_str.empty()) {
    if (auto obj_type = args.object.get_type();
        associations.contains(obj_type)) {
      mimetype_str = associations[obj_type].cast<std::string>();
    }
  }

  if (mimetype_str.empty()) {
    return absl::InvalidArgumentError(
        "Mimetype must be specified or globally associated with the "
        "object type.");
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
inline absl::Status EgltAssignInto(const py::object& obj, Chunk* chunk) {
  const auto& associations = pybindings::GetGlobalMimetypeAssociations();

  py::handle obj_type = obj.get_type();
  if (!associations.contains(obj_type)) {
    return absl::InvalidArgumentError(
        absl::StrCat("No mimetype association found for object type."));
  }

  const auto mimetype = associations[obj_type].cast<std::string>();
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

inline absl::Status EgltAssignInto(pybind11::object obj, std::string* dest) {
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

inline Chunk PyToChunk(py::object obj, std::string_view mimetype = "",
                       SerializerRegistry* registry = nullptr) {
  if (mimetype.empty()) {
    auto chunk = StatusOrConvertTo<Chunk>(std::move(obj));
    if (!chunk.ok()) {
      throw py::value_error(absl::StrCat("Failed to convert object to Chunk: ",
                                         chunk.status().message()));
    }
    return *std::move(chunk);
  }

  if (registry == nullptr) {
    registry = &GetGlobalSerializerRegistry();
  }
  absl::StatusOr<Chunk> serialized_chunk =
      ToChunk(std::any(obj), mimetype, registry);
  if (!serialized_chunk.ok()) {
    serialized_chunk =
        ToChunk(ConvertTo<std::any>(pybindings::PySerializationArgs{
                    .object = std::move(obj), .mimetype = mimetype}),
                mimetype, registry);
  }
  if (!serialized_chunk.ok()) {
    throw py::value_error(std::string(serialized_chunk.status().message()));
  }
  return *std::move(serialized_chunk);
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
