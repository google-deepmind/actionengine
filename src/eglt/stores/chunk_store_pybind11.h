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

#include "eglt/pybind11_headers.h"
#include "eglt/stores/chunk_store.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

class PyChunkStore final : public ChunkStore {
  // For detailed documentation, see the base class, ChunkStore.
 public:
  using ChunkStore::ChunkStore;

  PyChunkStore() : ChunkStore() {}

  absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<Chunk>, PyChunkStore, "get", Get,
                                seq, timeout);
  }

  absl::StatusOr<Chunk> GetByArrivalOrder(int64_t seq,
                                          absl::Duration timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<Chunk>, PyChunkStore,
                                "get_by_arrival_order", GetByArrivalOrder, seq,
                                timeout);
  }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(
        absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore,
        "get", Get, seq);
  }

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRefByArrivalOrder(
      int64_t seq, absl::Duration timeout) override {
    PYBIND11_OVERRIDE_PURE_NAME(
        absl::StatusOr<std::reference_wrapper<const Chunk>>, PyChunkStore,
        "get_by_arrival_order", GetByArrivalOrder, seq);
  }

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<std::optional<Chunk>>,
                                PyChunkStore, "pop", Pop, seq);
  }

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "put", Put, seq,
                                chunk, final);
  }

  absl::Status CloseWritesWithStatus(absl::Status status) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "no_further_puts",
                                CloseWritesWithStatus, status);
  }

  absl::StatusOr<size_t> Size() override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<size_t>, PyChunkStore, "size",
                                Size, );
  }

  absl::StatusOr<bool> Contains(int64_t seq) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<bool>, PyChunkStore, "contains",
                                Contains, seq);
  }

  absl::Status SetId(std::string_view id) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::Status, PyChunkStore, "set_id", SetId,
                                id);
  }

  [[nodiscard]] std::string_view GetId() const override {
    PYBIND11_OVERRIDE_PURE_NAME(std::string_view, PyChunkStore, "get_id",
                                GetId, );
  }

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<int64_t>, PyChunkStore,
                                "get_seq_for_arrival_offset",
                                GetSeqForArrivalOffset, arrival_offset);
  }

  absl::StatusOr<int64_t> GetFinalSeq() override {
    PYBIND11_OVERRIDE_PURE_NAME(absl::StatusOr<int64_t>, PyChunkStore,
                                "get_final_seq", GetFinalSeq, );
  }
};

void BindChunkStoreReaderOptions(
    py::handle scope, std::string_view name = "ChunkStoreReaderOptions");

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
