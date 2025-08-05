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

#include "eglt/data/data_pybind11.h"

#include <string>
#include <string_view>
#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11_abseil/no_throw_status.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/data/serialization.h"
#include "eglt/pybind11_headers.h"
#include "eglt/util/status_macros.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

auto PySerializerToCppSerializer(py::function py_serializer) -> Serializer {
  {
    py::gil_scoped_acquire gil;
    py_serializer.inc_ref();
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
    py_deserializer.inc_ref();
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

/// @private
void BindChunkMetadata(py::handle scope, std::string_view name) {
  py::class_<ChunkMetadata>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view mimetype) {
             return ChunkMetadata{.mimetype = std::string(mimetype)};
           }),
           py::kw_only(), py::arg_v("mimetype", "text/plain"))
      .def_readwrite("mimetype", &ChunkMetadata::mimetype)
      .def("__repr__",
           [](const ChunkMetadata& metadata) { return absl::StrCat(metadata); })
      .doc() = "Metadata for an Evergreen Chunk.";
}

/// @private
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
      "An Evergreen Chunk containing metadata and either a reference to or "
      "the data themselves.";
}

/// @private
void BindNodeFragment(py::handle scope, std::string_view name) {
  py::class_<NodeFragment>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string id, Chunk chunk, int seq, bool continued) {
             return NodeFragment{.id = std::move(id),
                                 .chunk = std::move(chunk),
                                 .seq = seq,
                                 .continued = continued};
           }),
           py::kw_only(), py::arg_v("id", ""), py::arg_v("chunk", Chunk()),
           py::arg_v("seq", 0), py::arg_v("continued", false))
      .def_readwrite("id", &NodeFragment::id)
      .def_readwrite("chunk", &NodeFragment::chunk)
      .def_readwrite("seq", &NodeFragment::seq)
      .def_readwrite("continued", &NodeFragment::continued)
      .def("__repr__",
           [](const NodeFragment& fragment) { return absl::StrCat(fragment); })
      .doc() = "An Evergreen NodeFragment.";
}

/// @private
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
      .doc() = "An Evergreen Port for an Action.";
}

/// @private
void BindActionMessage(py::handle scope, std::string_view name) {
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
      .def_readwrite("inputs", &ActionMessage::inputs)
      .def_readwrite("outputs", &ActionMessage::outputs)
      .def("__repr__",
           [](const ActionMessage& action) { return absl::StrCat(action); })
      .doc() = "An Evergreen ActionMessage definition.";
}

/// @private
void BindSessionMessage(py::handle scope, std::string_view name) {
  py::class_<SessionMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::vector<NodeFragment> node_fragments,
                       std::vector<ActionMessage> actions) {
             return SessionMessage{.node_fragments = std::move(node_fragments),
                                   .actions = std::move(actions)};
           }),
           py::kw_only(),
           py::arg_v("node_fragments", std::vector<NodeFragment>()),
           py::arg_v("actions", std::vector<ActionMessage>()))
      .def_readwrite("node_fragments", &SessionMessage::node_fragments)
      .def_readwrite("actions", &SessionMessage::actions)
      .def("__repr__",
           [](const SessionMessage& message) { return absl::StrCat(message); })
      .doc() = "An Evergreen SessionMessage data structure.";
}

void BindSerializerRegistry(py::handle scope, std::string_view name) {
  auto registry =
      py::class_<SerializerRegistry, std::shared_ptr<SerializerRegistry>>(
          scope, std::string(name).c_str());
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
          auto type_to_mimetype = GetTypeToMimetypeDict(self.get());
          auto mro = value.get_type().attr("__mro__");
          for (const auto& type : mro) {
            auto mtype = type_to_mimetype.attr("get")(type, py::none());
            if (!mtype.is_none()) {
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

/// @private
py::module_ MakeDataModule(py::module_ scope, std::string_view module_name) {
  py::module_ data = scope.def_submodule(std::string(module_name).c_str(),
                                         "Evergreen data structures, as PODs.");

  BindChunkMetadata(data, "ChunkMetadata");
  BindChunk(data, "Chunk");
  BindNodeFragment(data, "NodeFragment");
  BindPort(data, "Port");
  BindActionMessage(data, "ActionMessage");
  BindSessionMessage(data, "SessionMessage");
  BindSerializerRegistry(data, "SerializerRegistry");

  data.def("get_global_serializer_registry", []() {
    return ShareWithNoDeleter(GetGlobalSerializerRegistryPtr());
  });

  data.def(
      "to_bytes",
      [](py::handle obj, std::string_view mimetype = "",
         SerializerRegistry* registry = nullptr) -> absl::StatusOr<py::bytes> {
        ASSIGN_OR_RETURN(Chunk chunk, pybindings::PyToChunk(
                                          std::move(obj), mimetype, registry));
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
             result.metadata.mimetype = "__eglt:NodeFragment__";
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
        if (mimetype.empty()) {
          mimetype = chunk.metadata.mimetype;
        }
        if (mimetype == "__status__") {
          ASSIGN_OR_RETURN(absl::Status unpacked_status,
                           ConvertTo<absl::Status>(std::move(chunk)));
          return py::cast(
              pybind11::google::DoNotThrowStatus(std::move(unpacked_status)));
        }
        if (mimetype == "__eglt:NodeFragment__") {
          ASSIGN_OR_RETURN(NodeFragment fragment,
                           cppack::Unpack<NodeFragment>(chunk.data));
          return py::cast(fragment);
        }
        return pybindings::PyFromChunk(std::move(chunk), mimetype, registry);
      },
      py::arg("chunk"), py::arg_v("mimetype", ""),
      py::arg_v("registry", nullptr));

  return data;
}

}  // namespace eglt::pybindings
