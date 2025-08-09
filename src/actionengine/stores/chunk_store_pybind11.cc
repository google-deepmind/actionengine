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

#include "actionengine/stores/chunk_store_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include <Python.h>
#include <absl/strings/str_cat.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/stores/chunk_store.h"
#include "actionengine/stores/chunk_store_reader.h"
#include "actionengine/stores/local_chunk_store.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

absl::StatusOr<Chunk> PyChunkStore::Get(int64_t seq, absl::Duration timeout) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<Chunk>, PyChunkStore, "get", Get,
                              seq, timeout);
}

absl::StatusOr<Chunk> PyChunkStore::GetByArrivalOrder(int64_t seq,
                                                      absl::Duration timeout) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<Chunk>, PyChunkStore,
                              "get_by_arrival_order", GetByArrivalOrder, seq,
                              timeout);
}

absl::StatusOr<std::reference_wrapper<const Chunk>> PyChunkStore::GetRef(
    int64_t seq, absl::Duration timeout) {
  PYBIND11_OVERRIDE_PURE_NAME(
      absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore, "get",
      Get, seq);
}

absl::StatusOr<std::reference_wrapper<const Chunk>>
PyChunkStore::GetRefByArrivalOrder(int64_t seq, absl::Duration timeout) {
  PYBIND11_OVERRIDE_PURE_NAME(
      absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore,
      "get_by_arrival_order", GetByArrivalOrder, seq);
}

absl::StatusOr<std::optional<Chunk>> PyChunkStore::Pop(int64_t seq) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<std::optional<Chunk>>,
                              PyChunkStore, "pop", Pop, seq);
}

absl::Status PyChunkStore::Put(int64_t seq, Chunk chunk, bool final) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "put", Put, seq,
                              chunk, final);
}

absl::Status PyChunkStore::CloseWritesWithStatus(absl::Status status) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "no_further_puts",
                              CloseWritesWithStatus, status);
}

absl::StatusOr<size_t> PyChunkStore::Size() {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<size_t>, PyChunkStore, "size",
                              Size, );
}

absl::StatusOr<bool> PyChunkStore::Contains(int64_t seq) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<bool>, PyChunkStore, "contains",
                              Contains, seq);
}

absl::Status PyChunkStore::SetId(std::string_view id) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "set_id", SetId, id);
}

std::string_view PyChunkStore::GetId() const {
  PYBIND11_OVERRIDE_PURE_NAME(std::string_view, PyChunkStore, "get_id",
                              GetId, );
}

absl::StatusOr<int64_t> PyChunkStore::GetSeqForArrivalOffset(
    int64_t arrival_offset) {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<int64_t>, PyChunkStore,
                              "get_seq_for_arrival_offset",
                              GetSeqForArrivalOffset, arrival_offset);
}

absl::StatusOr<int64_t> PyChunkStore::GetFinalSeq() {
  PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<int64_t>, PyChunkStore,
                              "get_final_seq", GetFinalSeq, );
}

void BindChunkStoreReaderOptions(py::handle scope, std::string_view name) {
  py::class_<ChunkStoreReaderOptions>(scope, name.data(), py::module_local())
      .def(py::init([]() { return ChunkStoreReaderOptions{}; }),
           keep_event_loop_memo())
      .def_readwrite("ordered", &ChunkStoreReaderOptions::ordered)
      .def_readwrite("remove_chunks", &ChunkStoreReaderOptions::remove_chunks)
      .def_readwrite("n_chunks_to_buffer",
                     &ChunkStoreReaderOptions::n_chunks_to_buffer)
      .def_readwrite("timeout", &ChunkStoreReaderOptions::timeout)
      .doc() = "Options for reading from a ChunkStore.";
}

void BindChunkStore(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<ChunkStore, std::shared_ptr<ChunkStore>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("get", &ChunkStore::Get, py::arg("seq"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("pop", &ChunkStore::Pop, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def("put", &ChunkStore::Put)
      .def("__contains__", &ChunkStore::Contains)
      .def("__len__", &ChunkStore::Size);

  py::class_<PyChunkStore, ChunkStore, std::shared_ptr<PyChunkStore>>(
      scope, name_str.c_str())
      .def(py::init<>(), keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             double timeout) {
            return self->Get(seq, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             double timeout) {
            return self->GetByArrivalOrder(seq, timeout < 0
                                                    ? absl::InfiniteDuration()
                                                    : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &PyChunkStore::Pop, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<PyChunkStore>& self, int seq,
             const Chunk& chunk,
             bool final) { return self->Put(seq, chunk, final); },
          py::arg("seq"), py::arg("chunk"), py::arg_v("final", false),
          py::call_guard<py::gil_scoped_release>())
      .def("no_further_puts", &PyChunkStore::CloseWritesWithStatus,
           py::call_guard<py::gil_scoped_release>())
      .def("size", &PyChunkStore::Size,
           py::call_guard<py::gil_scoped_release>())
      .def("contains", &PyChunkStore::Contains)
      .def("set_id", &PyChunkStore::SetId)
      .def("get_id", &PyChunkStore::GetId)
      .def("get_final_seq", &PyChunkStore::GetFinalSeq)
      .def("get_seq_for_arrival_offset", &PyChunkStore::GetSeqForArrivalOffset,
           py::arg("arrival_offset"), py::call_guard<py::gil_scoped_release>())
      .def("__len__", &PyChunkStore::Size)
      .def("__contains__", &PyChunkStore::Contains)

      .doc() = "An ActionEngine ChunkStore interface.";
}

void BindLocalChunkStore(py::handle scope, std::string_view name) {
  py::class_<LocalChunkStore, ChunkStore, std::shared_ptr<LocalChunkStore>>(
      scope, std::string(name).c_str())
      .def(py::init<>(), keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             double timeout) {
            return self->Get(seq, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             double timeout) {
            return self->GetByArrivalOrder(seq, timeout < 0
                                                    ? absl::InfiniteDuration()
                                                    : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &LocalChunkStore::Pop, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<LocalChunkStore>& self, int seq,
             const Chunk& chunk,
             bool final) { return self->Put(seq, chunk, final); },
          py::arg("seq"), py::arg("chunk"), py::arg_v("final", false),
          py::call_guard<py::gil_scoped_release>())
      .def("no_further_puts", &LocalChunkStore::CloseWritesWithStatus,
           py::call_guard<py::gil_scoped_release>())
      .def("size", &LocalChunkStore::Size)
      .def("contains", &LocalChunkStore::Contains)
      .def("set_id", &LocalChunkStore::SetId)
      .def("get_id", &LocalChunkStore::GetId)
      .def("get_final_seq", &LocalChunkStore::GetFinalSeq)
      .def("get_seq_for_arrival_offset",
           &LocalChunkStore::GetSeqForArrivalOffset, py::arg("arrival_offset"),
           py::call_guard<py::gil_scoped_release>())
      .def("__len__", &LocalChunkStore::Size)
      .def("__contains__", &LocalChunkStore::Contains)
      .doc() = "ActionEngine LocalChunkStore.";
}

py::module_ MakeChunkStoreModule(py::module_ scope,
                                 std::string_view module_name) {
  py::module_ chunk_store = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine ChunkStore interface.");

  BindChunkStoreReaderOptions(chunk_store, "ChunkStoreReaderOptions");
  BindChunkStore(chunk_store, "ChunkStore");
  BindLocalChunkStore(chunk_store, "LocalChunkStore");

  return chunk_store;
}

}  // namespace act::pybindings
