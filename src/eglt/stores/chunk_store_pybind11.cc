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

#include "eglt/stores/chunk_store_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include "eglt/pybind11_headers.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/local_chunk_store.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

/// @private
void BindChunkStore(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<ChunkStore, std::shared_ptr<ChunkStore>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("get", &ChunkStore::Get, py::arg("seq"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("pop", &ChunkStore::PopOrDie, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def("put", &ChunkStore::Put)
      .def("__contains__", &ChunkStore::ContainsOrDie)
      .def("__len__", &ChunkStore::SizeOrDie);

  py::class_<PyChunkStore, ChunkStore, std::shared_ptr<PyChunkStore>>(
      scope, name_str.c_str())
      .def(py::init<>(), keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             double timeout) -> Chunk {
            auto result =
                self->Get(seq, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value();
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             double timeout) -> Chunk {
            auto result = self->GetByArrivalOrder(
                seq, timeout < 0 ? absl::InfiniteDuration()
                                    : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value();
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &PyChunkStore::Pop, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             const Chunk& chunk, bool final) {
            if (const auto status = self->Put(seq, chunk, final);
                !status.ok()) {
              throw std::runtime_error(std::string(status.message()));
            }
          },
          py::arg("seq"), py::arg("chunk"), py::arg_v("final", false))
      .def("no_further_puts", &PyChunkStore::CloseWritesWithStatus)
      .def("size", &PyChunkStore::SizeOrDie)
      .def("contains", &PyChunkStore::Contains)
      .def("set_id", &PyChunkStore::SetIdOrDie)
      .def("get_id", &PyChunkStore::GetId)
      .def("get_final_seq", &PyChunkStore::GetFinalSeq)
      .def("get_seq_for_arrival_offset",
           &PyChunkStore::GetSeqForArrivalOffset, py::arg("arrival_offset"),
           py::call_guard<py::gil_scoped_release>())
      .def("__len__", &PyChunkStore::SizeOrDie)
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
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             double timeout) -> Chunk {
            auto result =
                self->Get(seq, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value();
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             double timeout) -> Chunk {
            auto result = self->GetByArrivalOrder(
                seq, timeout < 0 ? absl::InfiniteDuration()
                                    : absl::Seconds(timeout));
            if (!result.ok()) {
              throw std::runtime_error(std::string(result.status().message()));
            }
            return result.value();
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &LocalChunkStore::PopOrDie, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             const Chunk& chunk, bool final) {
            if (const auto status = self->Put(seq, chunk, final);
                !status.ok()) {
              throw std::runtime_error(std::string(status.message()));
            }
          },
          py::arg("seq"), py::arg("chunk"), py::arg_v("final", false))
      .def("no_further_puts", &LocalChunkStore::CloseWritesWithStatus)
      .def("size", &LocalChunkStore::SizeOrDie)
      .def("contains", &LocalChunkStore::ContainsOrDie)
      .def("set_id", &LocalChunkStore::SetIdOrDie)
      .def("get_id", &LocalChunkStore::GetId)
      .def("get_final_seq", &LocalChunkStore::GetFinalSeq)
      .def("get_seq_for_arrival_offset",
           &LocalChunkStore::GetSeqForArrivalOffset, py::arg("arrival_offset"),
           py::call_guard<py::gil_scoped_release>())
      .def("__len__", &LocalChunkStore::SizeOrDie)
      .def("__contains__", &LocalChunkStore::ContainsOrDie)
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
