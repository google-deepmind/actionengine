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

#include "eglt/redis/chunk_store_pybind11.h"

#include <pybind11/pybind11.h>

#include "eglt/redis/chunk_store.h"

namespace eglt::pybindings {

namespace py = pybind11;

py::module_ MakeRedisModule(py::module_ scope, std::string_view name) {
  py::module_ redis_module = scope.def_submodule(name.data(), "Redis module");
  redis_module.doc() =
      "Module for Redis chunk store and related functionality.";

  return redis_module;

  // // Bind the RedisChunkStore class.
  // py::class_<eglt::redis::ChunkStore>(redis_module, "RedisChunkStore")
  //     .def(py::init<std::string_view>(), py::arg("store_id"))
  //     .def("put", &eglt::redis::ChunkStore::Put, py::arg("chunk"))
  //     .def("get", &eglt::redis::ChunkStore::Get, py::arg("seq"),
  //          py::arg("timeout") = absl::InfiniteDuration())
  //     .def("contains", &eglt::redis::ChunkStore::Contains, py::arg("seq"))
  //     .def("set_id", &eglt::redis::ChunkStore::SetId, py::arg("id"))
  //     .def("get_id", &eglt::redis::ChunkStore::GetId)
  //     .def("get_seq_for_arrival_offset",
  //          &eglt::redis::ChunkStore::GetSeqForArrivalOffset,
  //          py::arg("arrival_offset"))
  //     .def("get_final_seq", &eglt::redis::ChunkStore::GetFinalSeq);
  //
  // return redis_module;
}

}  // namespace eglt::pybindings