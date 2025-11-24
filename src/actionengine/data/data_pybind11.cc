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

#include "actionengine/data/data_pybind11.h"

#include <string>
#include <string_view>
#include <vector>

#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/eval.h>
#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/import_status_module.h>
#include <pybind11_abseil/no_throw_status.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/data/msgpack.h"
#include "actionengine/data/serialization.h"
#include "actionengine/data/types.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"
#include "cppack/msgpack.h"

namespace act::pybindings {

auto PySerializerToCppSerializer(py::function py_serializer) -> Serializer {
  {
    py::gil_scoped_acquire gil;
    py_serializer = py::cast<py::function>(py_serializer.inc_ref());
  }
  return [py_serializer = std::move(py_serializer)](
             std::any value) -> absl::StatusOr<Bytes> {
    py::gil_scoped_acquire gil;
    if (std::any_cast<py::handle>(&value) == nullptr) {
      return absl::InvalidArgumentError(
          "Value must be a py::object to serialize with a Python function.");
    }
    try {
      auto result = py_serializer(std::any_cast<py::handle>(std::move(value)));
      return py::cast<Bytes>(std::move(result));
    } catch (const py::error_already_set& e) {
      return absl::InvalidArgumentError(
          absl::StrCat("Python serialization failed: ", e.what()));
    }
  };
}

auto PyDeserializerToCppDeserializer(py::function py_deserializer)
    -> Deserializer {
  {
    py::gil_scoped_acquire gil;
    py_deserializer = py::cast<py::function>(py_deserializer.inc_ref());
  }
  return [py_deserializer = std::move(py_deserializer)](
             Bytes data) -> absl::StatusOr<std::any> {
    py::gil_scoped_acquire gil;
    py::object result = py_deserializer(py::bytes(std::move(data)));
    if (result.is_none()) {
      return absl::InvalidArgumentError("Deserialization returned None.");
    }
    return std::any(std::move(result));
  };
}

absl::StatusOr<Chunk> PyToChunk(py::handle obj, std::string_view mimetype,
                                SerializerRegistry* registry) {
  auto mimetype_str = std::string(mimetype);

  if (registry == nullptr) {
    registry = &GetGlobalSerializerRegistry();
  }

  if (mimetype_str.empty()) {
    const auto* data = static_cast<py::tuple*>(registry->GetUserData());
    const auto type_to_mimetype = (*data)[1].cast<py::dict>();
    for (const auto mro = py::type::of(obj).attr("__mro__");
         const auto& type : mro) {
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

absl::StatusOr<py::object> PyFromChunk(Chunk chunk, std::string_view mimetype,
                                       const SerializerRegistry* registry) {
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

void BindChunkMetadata(py::handle scope, std::string_view name) {
  py::class_<ChunkMetadata>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view mimetype) {
             return ChunkMetadata{.mimetype = std::string(mimetype)};
           }),
           py::kw_only(), py::arg_v("mimetype", "text/plain"))
      .def_readwrite("mimetype", &ChunkMetadata::mimetype)
      .def_readwrite("timestamp", &ChunkMetadata::timestamp)
      .def_property(
          "attributes",
          [](const ChunkMetadata& metadata) -> py::dict {
            py::dict dict;
            for (const auto& [key, value] : metadata.attributes) {
              dict[py::str(key)] = py::bytes(value);
            }
            return dict;
          },
          [](ChunkMetadata& metadata, const py::dict& dict) {
            metadata.attributes.clear();

            for (auto it = dict.begin(); it != dict.end(); ++it) {
              const auto key = it->first.cast<std::string>();
              const auto value = it->second.cast<py::bytes>();
              metadata.attributes[key] = std::string(value);
            }
          })
      .def("has_attribute",
           [](const ChunkMetadata& metadata, std::string_view key) {
             return metadata.attributes.contains(key);
           })
      .def(
          "get_attribute",
          [](const ChunkMetadata& metadata,
             std::string_view key) -> absl::StatusOr<py::bytes> {
            const auto it = metadata.attributes.find(std::string(key));
            if (it == metadata.attributes.end()) {
              return absl::NotFoundError(
                  absl::StrCat("Attribute not found: ", key));
            }
            return it->second;
          },
          py::arg("key"))
      .def(
          "set_attribute",
          [](ChunkMetadata& metadata, std::string_view key,
             const py::bytes& value) {
            metadata.attributes[std::string(key)] = std::string(value);
          },
          py::arg("key"), py::arg("value"))
      .def("set_attribute",
           [](ChunkMetadata& metadata, std::string_view key,
              const py::str& value) {
             metadata.attributes[std::string(key)] =
                 std::string(value.attr("encode")("utf-8").cast<std::string>());
           })
      .def("__repr__",
           [](const ChunkMetadata& metadata) { return absl::StrCat(metadata); })
      .doc() = "Metadata for an ActionEngine Chunk.";
}

void BindChunk(py::handle scope, std::string_view name) {
  py::class_<Chunk>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](ChunkMetadata metadata = ChunkMetadata(),
                       const py::bytes& data_bytes = "", std::string ref = "") {
             return Chunk{.metadata = std::move(metadata),
                          .ref = std::move(ref),
                          .data = std::string(data_bytes)};
           }),
           py::kw_only(), py::arg_v("metadata", ChunkMetadata()),
           py::arg_v("data", py::bytes()), py::arg_v("ref", ""))
      .def_readwrite("metadata", &Chunk::metadata)
      .def_readwrite("ref", &Chunk::ref)
      .def_property(
          "data", [](const Chunk& chunk) { return py::bytes(chunk.data); },
          [](Chunk& chunk, const py::bytes& data) {
            chunk.data = std::string(data);
          })
      .def("__repr__", [](const Chunk& chunk) { return absl::StrCat(chunk); })
      .doc() =
      "An ActionEngine Chunk containing metadata and either a reference to or "
      "the data themselves.";
}

void BindNodeFragment(py::handle scope, std::string_view name) {
  py::class_<NodeFragment>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string id, Chunk chunk, int seq, bool continued) {
             return NodeFragment{.id = std::move(id),
                                 .data = std::move(chunk),
                                 .seq = seq,
                                 .continued = continued};
           }),
           py::kw_only(), py::arg_v("id", ""), py::arg_v("chunk", Chunk()),
           py::arg_v("seq", 0), py::arg_v("continued", false))
      .def_readwrite("id", &NodeFragment::id)
      .def_property(
          "chunk",
          [](NodeFragment& fragment) -> Chunk& {
            return std::get<Chunk>(fragment.data);
          },
          [](NodeFragment& fragment, const Chunk& chunk) {
            fragment.data = chunk;
          })
      .def_readwrite("seq", &NodeFragment::seq)
      .def_readwrite("continued", &NodeFragment::continued)
      .def("__repr__",
           [](const NodeFragment& fragment) { return absl::StrCat(fragment); })
      .doc() = "An ActionEngine NodeFragment.";
}

void BindPort(py::handle scope, std::string_view name) {
  py::class_<Port>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view name, std::string_view id) {
             return Port{
                 .name = std::string(name),
                 .id = std::string(id),
             };
           }),
           py::kw_only(), py::arg_v("name", ""), py::arg_v("id", ""))
      .def_readwrite("name", &Port::name)
      .def_readwrite("id", &Port::id)
      .def("__repr__",
           [](const Port& parameter) { return absl::StrCat(parameter); })
      .doc() = "An ActionEngine Port for an Action.";
}

void BindActionMessage(py::handle scope, std::string_view name) {
  py::bind_vector<std::vector<Port>>(scope, "PortList");

  py::class_<ActionMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view action_name, std::vector<Port> inputs,
                       std::vector<Port> outputs) {
             return ActionMessage{.name = std::string(action_name),
                                  .inputs = std::move(inputs),
                                  .outputs = std::move(outputs)};
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<Port>()),
           py::arg_v("outputs", std::vector<Port>()))
      .def_readwrite("name", &ActionMessage::name)
      .def_readwrite("id", &ActionMessage::id)
      .def_readwrite("inputs", &ActionMessage::inputs)
      .def_readwrite("outputs", &ActionMessage::outputs)
      .def("__repr__",
           [](const ActionMessage& action) { return absl::StrCat(action); })
      .doc() = "An ActionEngine ActionMessage definition.";
}

void BindWireMessage(py::handle scope, std::string_view name) {
  py::bind_vector<std::vector<NodeFragment>>(scope, "NodeFragmentList");
  py::bind_vector<std::vector<ActionMessage>>(scope, "ActionMessageList");

  py::class_<WireMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::vector<NodeFragment> node_fragments,
                       std::vector<ActionMessage> actions) {
             return WireMessage{.node_fragments = std::move(node_fragments),
                                .actions = std::move(actions)};
           }),
           py::kw_only(),
           py::arg_v("node_fragments", std::vector<NodeFragment>()),
           py::arg_v("actions", std::vector<ActionMessage>()))
      .def_readwrite("node_fragments", &WireMessage::node_fragments,
                     py::return_value_policy::reference)
      .def_readwrite("actions", &WireMessage::actions,
                     py::return_value_policy::reference)
      .def("pack_msgpack",
           [](const WireMessage& message) {
             const std::vector<uint8_t> bytes = cppack::Pack(message);
             return py::bytes(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size());
           })
      .def_static(
          "from_msgpack",
          [](const py::bytes& data) -> absl::StatusOr<WireMessage> {
            const std::string data_str = static_cast<std::string>(data);
            const std::vector<uint8_t> data_bytes(data_str.begin(),
                                                  data_str.end());
            return cppack::Unpack<WireMessage>(data_bytes);
          },
          py::arg("data"))
      .def("__repr__",
           [](const WireMessage& message) { return absl::StrCat(message); })
      .doc() = "An ActionEngine WireMessage data structure.";
}

void BindSerializerRegistry(py::handle scope, std::string_view name) {
  auto registry =
      py::classh<SerializerRegistry>(scope, std::string(name).c_str());
  registry.def(MakeSameObjectRefConstructor<SerializerRegistry>());

  registry.def(
      py::init([]() { return std::make_shared<SerializerRegistry>(); }));

  registry.def_property_readonly(
      "_mimetype_to_type", [](const std::shared_ptr<SerializerRegistry>& self) {
        return GetMimetypeToTypeDict(self.get());
      });

  registry.def_property_readonly(
      "_type_to_mimetype", [](const std::shared_ptr<SerializerRegistry>& self) {
        return GetTypeToMimetypeDict(self.get());
      });

  registry.def(
      "serialize",
      [](const std::shared_ptr<SerializerRegistry>& self, py::handle value,
         std::string_view mimetype) -> absl::StatusOr<py::bytes> {
        auto mimetype_str = std::string(mimetype);
        if (mimetype_str.empty()) {
          const auto type_to_mimetype = GetTypeToMimetypeDict(self.get());
          for (const auto mro = py::type::of(value).attr("__mro__");
               const auto& type : mro) {
            if (auto mtype = type_to_mimetype.attr("get")(type, py::none());
                !mtype.is_none()) {
              mimetype_str = mtype.cast<std::string>();
              break;
            }
          }
        }
        absl::StatusOr<Bytes> serialized;
        {
          py::gil_scoped_release release_gil;
          serialized = self->Serialize(value, mimetype_str);
          if (!serialized.ok()) {
            ASSIGN_OR_RETURN(std::any cpp_any,
                             CastPyObjectToAny(std::move(value), mimetype_str));
            serialized = self->Serialize(std::move(cpp_any), mimetype_str);
          }
        }
        RETURN_IF_ERROR(serialized.status());
        return py::bytes(std::move(*serialized));
      },
      py::arg("value"), py::arg_v("mimetype", ""));

  registry.def(
      "deserialize",
      [](const std::shared_ptr<SerializerRegistry>& self, py::bytes data,
         std::string_view mimetype) -> absl::StatusOr<py::object> {
        absl::StatusOr<std::any> deserialized;
        {
          py::gil_scoped_release release_gil;
          deserialized = self->Deserialize(std::move(data), mimetype);
        }
        RETURN_IF_ERROR(deserialized.status());
        if (std::any_cast<py::object>(&*deserialized) == nullptr) {
          return absl::InvalidArgumentError(
              absl::StrCat("Deserialized object is not a py::object, but a ",
                           deserialized->type().name(),
                           ". Cannot convert to py::object because it's not "
                           "implemented yet."));
        }

        return std::any_cast<py::object>(*std::move(deserialized));
      },
      py::arg("data"), py::arg_v("mimetype", ""));

  registry.def(
      "register_serializer",
      [](const std::shared_ptr<SerializerRegistry>& self,
         std::string_view mimetype, const py::function& serializer,
         const py::object& obj_type = py::none()) {
        if (!obj_type.is_none()) {
          if (!py::isinstance<py::type>(obj_type)) {
            return absl::InvalidArgumentError(
                "obj_type must be a type, not an instance or other object.");
          }
          // Register the mimetype with the type.
          auto mimetype_str = std::string(mimetype);
          GetTypeToMimetypeDict(self.get())[obj_type] = mimetype_str;
          GetMimetypeToTypeDict(self.get())[mimetype_str.c_str()] = obj_type;
        }
        self->RegisterSerializer(std::string(mimetype),
                                 PySerializerToCppSerializer(serializer));
        return absl::OkStatus();
      },
      py::arg("mimetype"), py::arg("serializer"),
      py::arg_v("obj_type", py::none()), py::keep_alive<1, 3>());
  registry.def(
      "register_deserializer",
      [](const std::shared_ptr<SerializerRegistry>& self,
         std::string_view mimetype, const py::function& deserializer,
         const py::object& obj_type = py::none()) {
        if (!obj_type.is_none()) {
          if (!py::isinstance<py::type>(obj_type)) {
            return absl::InvalidArgumentError(
                "obj_type must be a type, not an instance or other object.");
          }
          // Register the mimetype with the type.
          auto mimetype_str = std::string(mimetype);
          GetTypeToMimetypeDict(self.get())[obj_type] = mimetype_str;
          GetMimetypeToTypeDict(self.get())[mimetype_str.c_str()] = obj_type;
        }
        self->RegisterDeserializer(
            std::string(mimetype),
            PyDeserializerToCppDeserializer(deserializer));
        return absl::OkStatus();
      },
      py::keep_alive<1, 3>());
  registry.def("__del__", [](const std::shared_ptr<SerializerRegistry>& self) {
    self->SetUserData(nullptr);
  });
  registry.doc() = "A registry for serialization functions.";
}

py::module_ MakeDataModule(py::module_ scope, std::string_view module_name) {
  py::module_ data =
      scope.def_submodule(std::string(module_name).c_str(),
                          "ActionEngine data structures, as PODs.");

  BindChunkMetadata(data, "ChunkMetadata");
  BindChunk(data, "Chunk");
  BindNodeFragment(data, "NodeFragment");
  BindPort(data, "Port");
  BindActionMessage(data, "ActionMessage");
  BindWireMessage(data, "WireMessage");
  BindSerializerRegistry(data, "SerializerRegistry");

  data.def("get_global_serializer_registry", []() {
    return ShareWithNoDeleter(GetGlobalSerializerRegistryPtr());
  });

  data.def(
      "to_bytes",
      [](py::handle obj, std::string_view mimetype = "",
         SerializerRegistry* registry = nullptr) -> absl::StatusOr<py::bytes> {
        ASSIGN_OR_RETURN(Chunk chunk,
                         pybindings::PyToChunk(obj, mimetype, registry));
        return std::move(chunk.data);
      },
      py::arg("obj"), py::arg_v("mimetype", ""),
      py::arg_v("registry", nullptr));

  data.def(
      "to_chunk",
      [](py::handle obj, std::string_view mimetype = "",
         SerializerRegistry* registry = nullptr) -> absl::StatusOr<Chunk> {
        return pybindings::PyToChunk(obj, mimetype, registry);
      },
      py::arg("obj"), py::arg_v("mimetype", ""),
      py::arg_v("registry", nullptr));

  data.def("to_chunk",
           [](const NodeFragment& fragment) -> absl::StatusOr<Chunk> {
             std::vector<uint8_t> bytes = cppack::Pack(fragment);
             Chunk result;
             result.metadata.emplace().mimetype = "__act:NodeFragment__";
             result.data = std::string(bytes.begin(), bytes.end());
             return result;
           });

  data.def("to_chunk", [](const absl::Status& status) -> absl::StatusOr<Chunk> {
    ASSIGN_OR_RETURN(auto chunk, ConvertTo<Chunk>(status));
    return std::move(chunk);
  });

  data.def(
      "from_chunk",
      [](Chunk chunk, std::string_view mimetype = "",
         const SerializerRegistry* registry =
             nullptr) -> absl::StatusOr<py::object> {
        std::string chunk_mimetype;
        if (chunk.metadata) {
          chunk_mimetype = chunk.metadata->mimetype;
        }
        if (chunk_mimetype == "__status__") {
          ASSIGN_OR_RETURN(absl::Status unpacked_status,
                           ConvertTo<absl::Status>(std::move(chunk)));
          return py::cast(
              pybind11::google::DoNotThrowStatus(std::move(unpacked_status)));
        }
        if (chunk_mimetype == "__act:NodeFragment__") {
          const std::vector<uint8_t> data_bytes(chunk.data.begin(),
                                                chunk.data.end());
          ASSIGN_OR_RETURN(NodeFragment fragment,
                           cppack::Unpack<NodeFragment>(data_bytes));
          return py::cast(fragment);
        }
        return pybindings::PyFromChunk(std::move(chunk), mimetype, registry);
      },
      py::arg("chunk"), py::arg_v("mimetype", ""),
      py::arg_v("registry", nullptr));

  return data;
}

void EnsureMimetypeAssociations(SerializerRegistry* registry) {
  if (registry->GetUserData() != nullptr) {
    return;  // Already initialized.
  }
  auto data = std::shared_ptr<py::tuple>(new py::tuple(2), [](py::object*) {});
  (*data)[0] = py::dict();  // mimetype to type
  (*data)[1] = py::dict();  // type to mimetype
  // *data = py::make_tuple(py::dict(), py::dict());
  registry->SetUserData(std::move(data));
}

py::dict GetMimetypeToTypeDict(SerializerRegistry* registry) {
  EnsureMimetypeAssociations(registry);
  const auto* data = static_cast<py::tuple*>(registry->GetUserData());
  return (*data)[0].cast<py::dict>();
}

py::dict GetTypeToMimetypeDict(SerializerRegistry* registry) {
  EnsureMimetypeAssociations(registry);
  const auto* data = static_cast<py::tuple*>(registry->GetUserData());
  return (*data)[1].cast<py::dict>();
}

py::dict GetGlobalTypeToMimetype() {
  SerializerRegistry* registry = GetGlobalSerializerRegistryPtr();
  EnsureMimetypeAssociations(registry);
  return GetTypeToMimetypeDict(registry);
}

absl::flat_hash_map<std::string, PyObjectToStdAnyCaster>&
GetCastersForMimetypes() {
  static absl::flat_hash_map<std::string, PyObjectToStdAnyCaster> casters = {
      {"application/octet-stream",
       MakeDefaultPyObjectToStdAnyCaster<std::string>()},
      {"text/plain", MakeDefaultPyObjectToStdAnyCaster<std::string>()},
  };
  return casters;
}

absl::StatusOr<std::any> CastPyObjectToAny(py::handle obj,
                                           std::string_view mimetype) {
  if (obj.is_none()) {
    return absl::InvalidArgumentError("Cannot convert None to a C++ type.");
  }

  auto& casters = GetCastersForMimetypes();
  const auto it = casters.find(std::string(mimetype));
  if (it == casters.end()) {
    return std::any(obj);
  }

  return it->second(obj);
}

absl::Status EgltAssignInto(PySerializationArgs args, std::any* dest) {
  auto mimetype_str = std::string(args.mimetype);
  {
    py::gil_scoped_acquire gil;  // Ensure GIL is held for Python calls.
    const py::handle global_type_to_mimetype = GetGlobalTypeToMimetype();

    if (mimetype_str.empty()) {
      mimetype_str = global_type_to_mimetype
                         .attr("get")(py::type::of(args.object), py::str())
                         .cast<std::string>();
    }

    if (mimetype_str.empty()) {
      return absl::InvalidArgumentError(
          "Mimetype must be specified or globally associated with the "
          "object type.");
    }
  }

  absl::StatusOr<std::any> cpp_object =
      CastPyObjectToAny(args.object, mimetype_str);
  if (!cpp_object.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to convert object: ", cpp_object.status().message()));
  }

  *dest = std::move(*cpp_object);
  return absl::OkStatus();
}
}  // namespace act::pybindings

absl::Status act::EgltAssignInto(const py::handle& obj, Chunk* chunk) {
  const py::dict& global_type_to_mimetype =
      pybindings::GetGlobalTypeToMimetype();

  const auto mimetype =
      global_type_to_mimetype.attr("get")(py::type::of(obj), py::str())
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

absl::Status pybind11::EgltAssignInto(pybind11::handle obj, std::string* dest) {
  if (!pybind11::isinstance<pybind11::str>(obj)) {
    return absl::InvalidArgumentError(
        "Object is not a string. Cannot assign to std::string.");
  }

  try {
    *dest = obj.cast<std::string>();
    return absl::OkStatus();
  } catch (const pybind11::cast_error& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to cast object to std::string: ", e.what()));
  }
}