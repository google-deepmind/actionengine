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

#ifndef EGLT_PYBIND11_EGLT_CHUNK_STORE_H_
#define EGLT_PYBIND11_EGLT_CHUNK_STORE_H_

#include <cstddef>
#include <string_view>
#include <utility>

#include "eglt/nodes/async_node.h"
#include "eglt/nodes/chunk_store.h"
#include "eglt/pybind11/pybind11_headers.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

class PyChunkStore final : public ChunkStore {
 public:
  using ChunkStore::ChunkStore;

  PyChunkStore() : ChunkStore() {}

  absl::StatusOr<base::Chunk> GetImmediately(int seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(base::Chunk, PyChunkStore, "get_immediately",
                                TryGetImmediately, seq_id);
  }

  absl::StatusOr<base::Chunk> PopImmediately(int seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(base::Chunk, PyChunkStore, "pop_immediately",
                                PopImmediately, seq_id);
  }

  size_t Size() override {
    PYBIND11_OVERRIDE_PURE_NAME(size_t, PyChunkStore, "size", Size, );
  }

  bool Contains(int seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(bool, PyChunkStore, "contains", Contains,
                                seq_id);
  }

  void NotifyAllWaiters() override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyChunkStore, "notify_all_waiters",
                                NotifyAllWaiters, );
  }

  int GetFinalSeqId() override {
    PYBIND11_OVERRIDE_PURE_NAME(int, PyChunkStore, "get_final_seq_id",
                                GetFinalSeqId, );
  }

  absl::Status WaitForSeqId(int seq_id, float timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "wait_for_seq_id",
                                WaitForSeqId, seq_id, timeout);
  }

  absl::StatusOr<int> WriteToImmediateStore(int seq_id,
                                            base::Chunk chunk) override {
    py::gil_scoped_acquire gil;
    const py::function function =
        py::get_override(this, "write_to_immediate_store");
    if (!function) {
      PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<int>, PyChunkStore,
                                  "write_to_immediate_store",
                                  WriteToImmediateStore, seq_id, chunk);
    }
    const py::object result = function(seq_id, chunk);
    return result.cast<int>();
  }

  void NotifyWaiters(int seq_id, int arrival_offset) override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyChunkStore, "notify_waiters",
                                NotifyWaiters, seq_id, arrival_offset);
  }

  absl::Status WaitForArrivalOffset(int arrival_offset,
                                    float timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore,
                                "wait_for_arrival_offset", WaitForArrivalOffset,
                                arrival_offset, timeout);
  }

  int GetSeqIdForArrivalOffset(int arrival_offset) override {
    PYBIND11_OVERRIDE_PURE_NAME(int, PyChunkStore,
                                "get_seq_id_for_arrival_offset",
                                GetSeqIdForArrivalOffset, arrival_offset);
  }

  void SetFinalSeqId(int final_seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyChunkStore, "set_final_seq_id",
                                SetFinalSeqId, final_seq_id);
  }
};

void BindChunkStore(py::handle scope, std::string_view name = "ChunkStore");

void BindLocalChunkStore(py::handle scope,
                         std::string_view name = "LocalChunkStore");

py::module_ MakeChunkStoreModule(py::module_ scope,
                                 std::string_view module_name = "chunk_store");
}  // namespace eglt::pybindings

namespace pybind11::detail {
/// @private
template <>
class type_caster<std::unique_ptr<eglt::ChunkStore>>
    : public type_caster_base<std::unique_ptr<eglt::ChunkStore>> {};
}  // namespace pybind11::detail

#endif  // EGLT_PYBIND11_EGLT_CHUNK_STORE_H_
