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

#include "eglt/pybind11/types.h"

#include <string>
#include <string_view>
#include <vector>

#include "eglt/data/eg_structs.h"
#include "eglt/pybind11/pybind11_headers.h"

namespace eglt::pybindings {

/// @private
void BindChunkMetadata(py::handle scope, std::string_view name) {
  py::class_<base::ChunkMetadata>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view mimetype) {
             return base::ChunkMetadata{.mimetype = std::string(mimetype)};
           }),
           py::kw_only(), py::arg_v("mimetype", "text/plain"))
      .def_readwrite("mimetype", &base::ChunkMetadata::mimetype)
      .def("__repr__",
           [](const base::ChunkMetadata& metadata) {
             return absl::StrCat(metadata);
           })
      .doc() = "Metadata for an Evergreen Chunk.";
}

/// @private
void BindChunk(py::handle scope, std::string_view name) {
  py::class_<base::Chunk>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](base::ChunkMetadata metadata = base::ChunkMetadata(),
                       const py::bytes& data_bytes = "", std::string ref = "") {
             return base::Chunk{.metadata = std::move(metadata),
                                .ref = std::move(ref),
                                .data = std::string(data_bytes)};
           }),
           py::kw_only(), py::arg_v("metadata", base::ChunkMetadata()),
           py::arg_v("data", py::bytes()), py::arg_v("ref", ""))
      .def_readwrite("metadata", &base::Chunk::metadata)
      .def_readwrite("ref", &base::Chunk::ref)
      .def_property(
          "data",
          [](const base::Chunk& chunk) { return py::bytes(chunk.data); },
          [](base::Chunk& chunk, const py::bytes& data) {
            chunk.data = std::string(data);
          })
      .def("__repr__",
           [](const base::Chunk& chunk) { return absl::StrCat(chunk); })
      .doc() =
      "An Evergreen Chunk containing metadata and either a reference to or "
      "the data themselves.";
}

/// @private
void BindNodeFragment(py::handle scope, std::string_view name) {
  py::class_<base::NodeFragment>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init(
               [](std::string id, base::Chunk chunk, int seq, bool continued) {
                 return base::NodeFragment{.id = std::move(id),
                                           .chunk = std::move(chunk),
                                           .seq = seq,
                                           .continued = continued};
               }),
           py::kw_only(), py::arg_v("id", ""),
           py::arg_v("chunk", base::Chunk()), py::arg_v("seq", 0),
           py::arg_v("continued", false))
      .def_readwrite("id", &base::NodeFragment::id)
      .def_readwrite("chunk", &base::NodeFragment::chunk)
      .def_readwrite("seq", &base::NodeFragment::seq)
      .def_readwrite("continued", &base::NodeFragment::continued)
      .def("__repr__",
           [](const base::NodeFragment& fragment) {
             return absl::StrCat(fragment);
           })
      .doc() = "An Evergreen NodeFragment.";
}

/// @private
void BindNamedParameter(py::handle scope, std::string_view name) {
  py::class_<base::NamedParameter>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view name, std::string_view id) {
             return base::NamedParameter{
                 .name = std::string(name),
                 .id = std::string(id),
             };
           }),
           py::kw_only(), py::arg_v("name", ""), py::arg_v("id", ""))
      .def_readwrite("name", &base::NamedParameter::name)
      .def_readwrite("id", &base::NamedParameter::id)
      .def("__repr__",
           [](const base::NamedParameter& parameter) {
             return absl::StrCat(parameter);
           })
      .doc() = "An Evergreen NamedParameter for an Action.";
}

/// @private
void BindActionMessage(py::handle scope, std::string_view name) {
  py::class_<base::ActionMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view action_name,
                       std::vector<base::NamedParameter> inputs,
                       std::vector<base::NamedParameter> outputs) {
             return base::ActionMessage{.name = std::string(action_name),
                                        .inputs = std::move(inputs),
                                        .outputs = std::move(outputs)};
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<base::NamedParameter>()),
           py::arg_v("outputs", std::vector<base::NamedParameter>()))
      .def_readwrite("name", &base::ActionMessage::name)
      .def_readwrite("inputs", &base::ActionMessage::inputs)
      .def_readwrite("outputs", &base::ActionMessage::outputs)
      .def("__repr__",
           [](const base::ActionMessage& action) {
             return absl::StrCat(action);
           })
      .doc() = "An Evergreen ActionMessage definition.";
}

/// @private
void BindSessionMessage(py::handle scope, std::string_view name) {
  py::class_<base::SessionMessage>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::vector<base::NodeFragment> node_fragments,
                       std::vector<base::ActionMessage> actions) {
             return base::SessionMessage{
                 .node_fragments = std::move(node_fragments),
                 .actions = std::move(actions)};
           }),
           py::kw_only(),
           py::arg_v("node_fragments", std::vector<base::NodeFragment>()),
           py::arg_v("actions", std::vector<base::ActionMessage>()))
      .def_readwrite("node_fragments", &base::SessionMessage::node_fragments)
      .def_readwrite("actions", &base::SessionMessage::actions)
      .def("__repr__",
           [](const base::SessionMessage& message) {
             return absl::StrCat(message);
           })
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
