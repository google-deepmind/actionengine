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
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "eglt/redis/chunk_store.h"
#include "eglt/redis/redis.h"
#include "eglt/stores/chunk_store_pybind11.h"
#include "eglt/util/status_macros.h"
#include "eglt/util/utils_pybind11.h"

namespace eglt::pybindings {

namespace py = pybind11;

py::module_ MakeRedisModule(py::module_ scope, std::string_view name) {
  py::module_ redis_module = scope.def_submodule(name.data(), "Redis module");
  redis_module.doc() =
      "Module for Redis chunk store and related functionality.";

  py::class_<redis::Redis, std::shared_ptr<redis::Redis>>(redis_module, "Redis")
      .def_static(
          "connect",
          [](std::string_view host,
             uint16_t port) -> absl::StatusOr<std::shared_ptr<redis::Redis>> {
            ASSIGN_OR_RETURN(std::shared_ptr client,
                             redis::Redis::Connect(host, port));
            return client;
          },
          py::arg("host"), py::arg_v("port", 6379), keep_event_loop_memo(),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get",
          [](const std::shared_ptr<redis::Redis>& self,
             std::string_view key) -> absl::StatusOr<std::string> {
            ASSIGN_OR_RETURN(redis::Reply reply,
                             self->ExecuteCommand("GET", {key}));
            return ConvertTo<std::string>(std::move(reply));
          },
          py::arg("key"), py::call_guard<py::gil_scoped_release>())
      .def(
          "set",
          [](const std::shared_ptr<redis::Redis>& self, std::string_view key,
             py::handle value) -> absl::Status {
            if (!py::isinstance<py::str>(value) &&
                !py::isinstance<py::bytes>(value)) {
              return absl::InvalidArgumentError(
                  "Value must be a string or bytes for Redis SET command.");
            }
            ASSIGN_OR_RETURN(const redis::Reply reply,
                             self->ExecuteCommand(
                                 "SET", {key, py::cast<std::string>(value)}));
            return GetStatusOrErrorFrom(reply);
          },
          py::arg("key"), py::arg("value"),
          py::call_guard<py::gil_scoped_release>())
      .doc() = "Redis client for Evergreen.";

  py::class_<redis::ChunkStore, eglt::ChunkStore,
             std::shared_ptr<redis::ChunkStore>>(redis_module, "ChunkStore")
      .def(py::init([](redis::Redis* absl_nonnull redis, std::string_view id,
                       int64_t ttl = -1) {
             absl::Duration ttl_duration =
                 ttl < 0 ? absl::InfiniteDuration() : absl::Seconds(ttl);
             return std::make_shared<redis::ChunkStore>(redis, id,
                                                        ttl_duration);
           }),
           py::arg("redis"), py::arg("id"), py::arg("ttl"),
           keep_event_loop_memo())
      .def(
          "get",
          [](const std::shared_ptr<redis::ChunkStore>& self, int seq,
             double timeout) -> absl::StatusOr<Chunk> {
            return self->Get(seq, timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_by_arrival_order",
          [](const std::shared_ptr<redis::ChunkStore>& self, int seq,
             double timeout) -> absl::StatusOr<Chunk> {
            return self->GetByArrivalOrder(seq, timeout < 0
                                                    ? absl::InfiniteDuration()
                                                    : absl::Seconds(timeout));
          },
          py::arg("seq"), py::arg_v("timeout", -1),
          py::call_guard<py::gil_scoped_release>())
      .def("pop", &redis::ChunkStore::PopOrDie, py::arg("seq"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "put",
          [](const std::shared_ptr<redis::ChunkStore>& self, int seq,
             const Chunk& chunk,
             bool final) { return self->Put(seq, chunk, final); },
          py::arg("seq"), py::arg("chunk"), py::arg_v("final", false),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "no_further_puts",
          [](const std::shared_ptr<redis::ChunkStore>& self) {
            return self->CloseWritesWithStatus(absl::OkStatus());
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "size",
          [](const std::shared_ptr<redis::ChunkStore>& self) {
            return self->Size();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "contains",
          [](const std::shared_ptr<redis::ChunkStore>& self, int seq) {
            return self->Contains(seq);
          },
          py::arg("seq"), py::call_guard<py::gil_scoped_release>())
      .def("set_id", &redis::ChunkStore::SetId)
      .def("get_id", &redis::ChunkStore::GetId)
      .def("get_final_seq", &redis::ChunkStore::GetFinalSeq,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "get_seq_for_arrival_offset",
          [](const std::shared_ptr<redis::ChunkStore>& self,
             int64_t arrival_offset) {
            return self->GetSeqForArrivalOffset(arrival_offset);
          },
          py::arg("arrival_offset"), py::call_guard<py::gil_scoped_release>())
      .def(
          "__len__",
          [](const std::shared_ptr<redis::ChunkStore>& self)
              -> absl::StatusOr<int64_t> { return self->Size(); },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "__contains__",
          [](const std::shared_ptr<redis::ChunkStore>& self, int seq) {
            return self->Contains(seq);
          },
          py::arg("seq"), py::call_guard<py::gil_scoped_release>())
      .doc() = "Evergreen Redis ChunkStore.";

  return redis_module;
}

}  // namespace eglt::pybindings