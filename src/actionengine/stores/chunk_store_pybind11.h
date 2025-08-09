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

#ifndef ACTIONENGINE_PYBIND11_ACTIONENGINE_CHUNK_STORE_H_
#define ACTIONENGINE_PYBIND11_ACTIONENGINE_CHUNK_STORE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>
#include <pybind11/cast.h>
#include <pybind11/detail/type_caster_base.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/data/types.h"
#include "actionengine/stores/chunk_store.h"

namespace act::pybindings {

namespace py = ::pybind11;

class PyChunkStore final : public ChunkStore {
  // For detailed documentation, see the base class, ChunkStore.
 public:
  using ChunkStore::ChunkStore;

  PyChunkStore() : ChunkStore() {}

  absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout) override;

  absl::StatusOr<Chunk> GetByArrivalOrder(int64_t seq,
                                          absl::Duration timeout) override;

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRef(
      int64_t seq, absl::Duration timeout) override;

  absl::StatusOr<std::reference_wrapper<const Chunk>> GetRefByArrivalOrder(
      int64_t seq, absl::Duration timeout) override;

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override;

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override;

  absl::Status CloseWritesWithStatus(absl::Status status) override;

  absl::StatusOr<size_t> Size() override;

  absl::StatusOr<bool> Contains(int64_t seq) override;

  absl::Status SetId(std::string_view id) override;

  [[nodiscard]] std::string_view GetId() const override;

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override;

  absl::StatusOr<int64_t> GetFinalSeq() override;
};

void BindChunkStoreReaderOptions(
    py::handle scope, std::string_view name = "ChunkStoreReaderOptions");

void BindChunkStore(py::handle scope, std::string_view name = "ChunkStore");

void BindLocalChunkStore(py::handle scope,
                         std::string_view name = "LocalChunkStore");

py::module_ MakeChunkStoreModule(py::module_ scope,
                                 std::string_view module_name = "chunk_store");
}  // namespace act::pybindings

namespace pybind11::detail {
template <>
class type_caster<std::unique_ptr<act::ChunkStore>>
    : public type_caster_base<std::unique_ptr<act::ChunkStore>> {};
}  // namespace pybind11::detail

#endif  // ACTIONENGINE_PYBIND11_ACTIONENGINE_CHUNK_STORE_H_
