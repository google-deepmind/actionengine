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

#include "eglt/nodes/chunk_store_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/chunk_store_local.h"
#include "eglt/pybind11_headers.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

/// @private
void BindChunkStore(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<ChunkStore, std::shared_ptr<ChunkStore>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("get", &ChunkStore::Get, py::arg("seq_id"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("pop", &ChunkStore::Pop, py::arg("seq_id"),
           py::call_guard<py::gil_scoped_release>())
      .def("put", &ChunkStore::Put)
      .def("__contains__", &ChunkStore::Contains)
      .def("__len__", &ChunkStore::Size);

  py::class_<PyChunkStore, ChunkStore, std::shared_ptr<PyChunkStore>>(
      scope, name_str.c_str())
      .def(py::init<>(), keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<PyChunkStore>& self, int seq_id,
             double timeout) -> const Chunk& {
            auto result =
                self->Get(seq_id, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value().get();
          },
          py::arg("seq_id"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<PyChunkStore>& self, int seq_id,
             double timeout) -> const Chunk& {
            auto result = self->GetByArrivalOrder(
                seq_id, timeout < 0 ? absl::InfiniteDuration()
                                    : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value().get();
          },
          py::arg("seq_id"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &PyChunkStore::Pop, py::arg("seq_id"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<PyChunkStore>& self, int seq_id,
             const Chunk& chunk, bool final) {
            if (const auto status = self->Put(seq_id, chunk, final);
                !status.ok()) {
              throw std::runtime_error(std::string(status.message()));
            }
          },
          py::arg("seq_id"), py::arg("chunk"), py::arg_v("final", false))
      .def("no_further_puts", &PyChunkStore::NoFurtherPuts)
      .def("size", &PyChunkStore::Size)
      .def("contains", &PyChunkStore::Contains)
      .def("set_id", &PyChunkStore::SetId)
      .def("get_id", &PyChunkStore::GetId)
      .def("get_final_seq_id", &PyChunkStore::GetFinalSeqId)
      .def("get_seq_id_for_arrival_offset",
           &PyChunkStore::GetSeqIdForArrivalOffset, py::arg("arrival_offset"),
           py::call_guard<py::gil_scoped_release>())
      .def("__len__", &PyChunkStore::Size)
      .def("__contains__", &PyChunkStore::Contains)

      .doc() = "An Evergreen ChunkStore interface.";
}

/// @private
void BindLocalChunkStore(py::handle scope, std::string_view name) {
  py::class_<LocalChunkStore, ChunkStore, std::shared_ptr<LocalChunkStore>>(
      scope, std::string(name).c_str())
      .def(py::init<>(), keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq_id,
             double timeout) -> const Chunk& {
            auto result =
                self->Get(seq_id, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value().get();
          },
          py::arg("seq_id"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq_id,
             double timeout) -> const Chunk& {
            auto result = self->GetByArrivalOrder(
                seq_id, timeout < 0 ? absl::InfiniteDuration()
                                    : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value().get();
          },
          py::arg("seq_id"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &LocalChunkStore::Pop, py::arg("seq_id"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq_id,
             const Chunk& chunk, bool final) {
            if (const auto status = self->Put(seq_id, chunk, final);
                !status.ok()) {
              throw std::runtime_error(std::string(status.message()));
            }
          },
          py::arg("seq_id"), py::arg("chunk"), py::arg_v("final", false))
      .def("no_further_puts", &LocalChunkStore::NoFurtherPuts)
      .def("size", &LocalChunkStore::Size)
      .def("contains", &LocalChunkStore::Contains)
      .def("set_id", &LocalChunkStore::SetId)
      .def("get_id", &LocalChunkStore::GetId)
      .def("get_final_seq_id", &LocalChunkStore::GetFinalSeqId)
      .def("get_seq_id_for_arrival_offset",
           &LocalChunkStore::GetSeqIdForArrivalOffset,
           py::arg("arrival_offset"), py::call_guard<py::gil_scoped_release>())
      .def("__len__", &LocalChunkStore::Size)
      .def("__contains__", &LocalChunkStore::Contains)
      .doc() = "Evergreen LocalChunkStore.";
}

/// @private
py::module_ MakeChunkStoreModule(py::module_ scope,
                                 std::string_view module_name) {
  py::module_ chunk_store = scope.def_submodule(
      std::string(module_name).c_str(), "Evergreen ChunkStore interface.");

  BindChunkStore(chunk_store, "ChunkStore");
  BindLocalChunkStore(chunk_store, "LocalChunkStore");

  return chunk_store;
}

}  // namespace eglt::pybindings
