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

#include "eglt/stores/chunk_store.h"
#include "eglt/pybind11_headers.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

class PyChunkStore final : public ChunkStore {
 public:
  using ChunkStore::ChunkStore;

  PyChunkStore() : ChunkStore() {}

  absl::StatusOr<std::reference_wrapper<const Chunk>> Get(
      int seq_id, absl::Duration timeout) const override {
    PYBIND11_OVERRIDE_PURE_NAME(
        absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore,
        "get", Get, seq_id);
  }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetByArrivalOrder(
      int seq_id, absl::Duration timeout) const override {
    PYBIND11_OVERRIDE_PURE_NAME(
        absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore,
        "get_by_arrival_order", GetByArrivalOrder, seq_id);
  }

  std::optional<Chunk> Pop(int seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(std::optional<Chunk>, PyChunkStore, "pop", Pop,
                                seq_id);
  }

  absl::Status Put(int seq_id, Chunk chunk, bool final) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "put", Put, seq_id,
                                chunk, final);
  }

  void NoFurtherPuts() override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyChunkStore, "no_further_puts",
                                NoFurtherPuts, );
  }

  size_t Size() override {
    PYBIND11_OVERRIDE_PURE_NAME(size_t, PyChunkStore, "size", Size, );
  }

  bool Contains(int seq_id) override {
    PYBIND11_OVERRIDE_PURE_NAME(bool, PyChunkStore, "contains", Contains,
                                seq_id);
  }

  void SetId(std::string_view id) override {
    PYBIND11_OVERRIDE_PURE_NAME(void, PyChunkStore, "set_id", SetId, id);
  }

  [[nodiscard]] std::string_view GetId() const override {
    PYBIND11_OVERRIDE_PURE_NAME(std::string_view, PyChunkStore, "get_id",
                                GetId, );
  }

  int GetSeqIdForArrivalOffset(int arrival_offset) override {
    PYBIND11_OVERRIDE_PURE_NAME(int, PyChunkStore,
                                "get_seq_id_for_arrival_offset",
                                GetSeqIdForArrivalOffset, arrival_offset);
  }

  int GetFinalSeqId() override {
    PYBIND11_OVERRIDE_PURE_NAME(int, PyChunkStore, "get_final_seq_id",
                                GetFinalSeqId, );
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
