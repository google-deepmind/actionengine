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

#include "eglt/nodes/nodes_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "eglt/nodes/node_map.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/chunk_store_pybind11.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

/// @private
void BindNodeMap(py::handle scope, std::string_view name) {
  py::class_<NodeMap, std::shared_ptr<NodeMap>>(scope,
                                                std::string(name).c_str())
      .def(MakeSameObjectRefConstructor<NodeMap>())
      .def(py::init([](const ChunkStoreFactory& factory = {}) {
             return std::make_shared<NodeMap>(factory);
           }),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "get",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return ShareWithNoDeleter(self->Get(id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "extract",
          [](const std::shared_ptr<NodeMap>& self,
             std::string_view id) -> std::optional<std::shared_ptr<AsyncNode>> {
            std::unique_ptr<AsyncNode> node = self->Extract(id);
            if (node == nullptr) {
              return std::nullopt;
            }
            return node;
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "contains",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return self->contains(id);
          },
          py::call_guard<py::gil_scoped_release>());
}

/// @private
void BindAsyncNode(py::handle scope, std::string_view name) {
  py::class_<AsyncNode, std::shared_ptr<AsyncNode>>(scope,
                                                    std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<AsyncNode>())
      // it is not possible to pass a std::unique_ptr to pybind11, so we pass
      // the factory function instead.
      .def(py::init([](const std::string& id, NodeMap* node_map,
                       const ChunkStoreFactory& chunk_store_factory = {}) {
             std::unique_ptr<ChunkStore> chunk_store(nullptr);
             if (chunk_store_factory) {
               chunk_store = chunk_store_factory(id);
             }
             return std::make_shared<AsyncNode>(id, node_map,
                                                std::move(chunk_store));
           }),
           py::arg_v("id", ""), py::arg_v("node_map", nullptr),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "put_fragment",
          [](const std::shared_ptr<AsyncNode>& self, NodeFragment fragment,
             int seq = -1) { return self->Put(std::move(fragment), seq); },
          py::arg_v("fragment", NodeFragment()), py::arg_v("seq", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "put_chunk",
          [](const std::shared_ptr<AsyncNode>& self, Chunk chunk, int seq = -1,
             bool final = false) {
            return self->Put(std::move(chunk), seq, final);
          },
          py::arg_v("chunk", Chunk()), py::arg_v("seq", -1),
          py::arg_v("final", false), py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_stream",
          [](const std::shared_ptr<AsyncNode>& self,
             const std::shared_ptr<WireStream>& stream) {
            absl::flat_hash_map<std::string, std::shared_ptr<WireStream>> peers;
            peers[stream->GetId()] = stream;
            self->BindPeers(std::move(peers));
          },
          py::arg("stream"), py::call_guard<py::gil_scoped_release>())
      .def(
          "next_chunk",
          [](const std::shared_ptr<AsyncNode>& self)
              -> absl::StatusOr<std::optional<Chunk>> {
            return self->Next<Chunk>();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_id",
           [](const std::shared_ptr<AsyncNode>& self) { return self->GetId(); })
      .def(
          "make_reader",
          [](const std::shared_ptr<AsyncNode>& self, bool ordered = false,
             bool remove_chunks = false, int n_chunks_to_buffer = -1) {
            return std::shared_ptr(
                self->MakeReader(ordered, remove_chunks, n_chunks_to_buffer));
          },
          py::arg_v("ordered", false), py::arg_v("remove_chunks", false),
          py::arg_v("n_chunks_to_buffer", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "set_reader_options",
          [](const std::shared_ptr<AsyncNode>& self, bool ordered = false,
             bool remove_chunks = false, int n_chunks_to_buffer = -1) {
            self->SetReaderOptions(ordered, remove_chunks, n_chunks_to_buffer);
            return self;
          },
          py::arg_v("ordered", false), py::arg_v("remove_chunks", false),
          py::arg_v("n_chunks_to_buffer", -1),
          py::call_guard<py::gil_scoped_release>());
}

}  // namespace eglt::pybindings
