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

#include "eglt/pybind11/chunk_store.h"

#include <memory>
#include <string>
#include <string_view>

#include "eglt/nodes/async_node.h"
#include "eglt/nodes/chunk_store.h"
#include "eglt/nodes/chunk_store_local.h"
#include "eglt/pybind11/pybind11_headers.h"
#include "eglt/pybind11/utils.h"

namespace eglt::pybindings {

/// @private
void BindChunkStore(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::class_<ChunkStore, std::shared_ptr<ChunkStore>>(
      scope, absl::StrCat(name, "VirtualBase").c_str())
      .def("get", &ChunkStore::Get, py::arg("seq_id"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("pop", &ChunkStore::Pop, py::arg("seq_id"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("put", &ChunkStore::Put)
      .def("__contains__", &ChunkStore::Contains)
      .def("__len__", &ChunkStore::Size);

  py::class_<PyChunkStore, ChunkStore, std::shared_ptr<PyChunkStore>>(
      scope, name_str.c_str())
      .def(py::init<>(), keep_event_loop_memo())
      // base methods
      .def("get", &PyChunkStore::Get, py::arg("seq_id"),
           py::arg_v("timeout", -1), py::call_guard<py::gil_scoped_release>())
      .def("pop", &PyChunkStore::Pop, py::arg("seq_id"),
           py::arg_v("timeout", -1), py::call_guard<py::gil_scoped_release>())
      .def("put", &PyChunkStore::Put)
      .def("__contains__", &ChunkStore::Contains)
      .def("__len__", &ChunkStore::Size)
      // virtual methods to be overridden by subclasses
      .def("get_immediately", &PyChunkStore::GetImmediately)
      .def("pop_immediately", &PyChunkStore::PopImmediately)
      .def("size", &PyChunkStore::Size)
      .def("contains", &PyChunkStore::Contains)
      .def("notify_all_waiters", &PyChunkStore::NotifyAllWaiters)
      .def("get_final_seq_id", &PyChunkStore::GetFinalSeqId)
      .def("wait_for_seq_id", &PyChunkStore::WaitForSeqId, py::arg("seq_id"),
           py::arg_v("timeout", -1), py::call_guard<py::gil_scoped_release>())
      .def("wait_for_arrival_offset", &PyChunkStore::WaitForArrivalOffset,
           py::arg("arrival_offset"), py::arg_v("timeout", -1),
           py::call_guard<py::gil_scoped_release>())
      .def("write_to_immediate_store", &PyChunkStore::WriteToImmediateStore,
           py::arg("seq_id"), py::arg("fragment"))
      .def("notify_waiters", &PyChunkStore::NotifyWaiters,
           py::arg_v("seq_id", -1), py::arg_v("arrival_offset", -1))
      .def("get_seq_id_for_arrival_offset",
           &PyChunkStore::GetSeqIdForArrivalOffset, py::arg("arrival_offset"),
           py::call_guard<py::gil_scoped_release>())
      .def("set_final_seq_id", &PyChunkStore::SetFinalSeqId,
           py::arg("final_seq_id"))
      .doc() = "An Evergreen ChunkStore interface.";
}

/// @private
void BindLocalChunkStore(py::handle scope, std::string_view name) {
  py::class_<LocalChunkStore, ChunkStore, std::shared_ptr<LocalChunkStore>>(
      scope, std::string(name).c_str())
      .def(py::init<>())
      .def("get_immediately", &LocalChunkStore::GetImmediately)
      .def("try_pop_immediately", &LocalChunkStore::PopImmediately)
      .def("size", &LocalChunkStore::Size)
      .def("contains", &LocalChunkStore::Contains)
      .def("notify_all_waiters", &LocalChunkStore::NotifyAllWaiters)
      .def("get_final_seq_id", &LocalChunkStore::GetFinalSeqId)
      .def("wait_for_seq_id", &LocalChunkStore::WaitForSeqId, py::arg("seq_id"),
           py::arg_v("timeout", 0), py::call_guard<py::gil_scoped_release>())
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
