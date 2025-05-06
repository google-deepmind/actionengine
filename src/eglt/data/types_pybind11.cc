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

#include "eglt/data/types_pybind11.h"

#include <string>
#include <string_view>
#include <vector>

#include "eglt/data/eg_structs.h"
#include "eglt/pybind11_headers.h"

namespace eglt::pybindings {

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
void BindNamedParameter(py::handle scope, std::string_view name) {
  py::class_<NamedParameter>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view name, std::string_view id) {
             return NamedParameter{
                 .name = std::string(name),
                 .id = std::string(id),
             };
           }),
           py::kw_only(), py::arg_v("name", ""), py::arg_v("id", ""))
      .def_readwrite("name", &NamedParameter::name)
      .def_readwrite("id", &NamedParameter::id)
      .def("__repr__",
           [](const NamedParameter& parameter) {
             return absl::StrCat(parameter);
           })
      .doc() = "An Evergreen NamedParameter for an Action.";
}

/// @private
void BindActionMessage(py::handle scope, std::string_view name) {
  py::class_<ActionMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view action_name,
                       std::vector<NamedParameter> inputs,
                       std::vector<NamedParameter> outputs) {
             return ActionMessage{.name = std::string(action_name),
                                  .inputs = std::move(inputs),
                                  .outputs = std::move(outputs)};
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NamedParameter>()),
           py::arg_v("outputs", std::vector<NamedParameter>()))
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

/// @private
py::module_ MakeTypesModule(py::module_ scope, std::string_view module_name) {
  py::module_ types = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen data structures, as PODs.");

  BindChunkMetadata(types, "ChunkMetadata");
  BindChunk(types, "Chunk");
  BindNodeFragment(types, "NodeFragment");
  BindNamedParameter(types, "NamedParameter");
  BindActionMessage(types, "ActionMessage");
  BindSessionMessage(types, "SessionMessage");

  return types;
}

}  // namespace eglt::pybindings
