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

#ifndef EGLT_REDIS_CHUNK_STORE_H_
#define EGLT_REDIS_CHUNK_STORE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/optimization.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include "eglt/concurrency/concurrency.h"
#include "eglt/data/conversion.h"
#include "eglt/data/eg_structs.h"
#include "eglt/redis/chunk_store_ops/close_writes.lua.h"
#include "eglt/redis/pubsub.h"
#include "eglt/redis/redis.h"
#include "eglt/redis/reply.h"
#include "eglt/redis/reply_converters.h"
#include "eglt/redis/streams.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/util/status_macros.h"

namespace eglt::redis {

struct ChunkStoreEvent {
  std::string type;
  int seq = -1;
  int arrival_offset = -1;
  std::string stream_message_id;

  static ChunkStoreEvent FromString(const std::string& message);
};

class ChunkStore final : public eglt::ChunkStore {
 public:
  explicit ChunkStore(Redis* absl_nonnull redis, std::string_view id,
                      absl::Duration ttl = absl::InfiniteDuration());

  // No copy or move semantics allowed.
  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore& other) = delete;

  ~ChunkStore() override;

  absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout) override;

  absl::StatusOr<Chunk> GetByArrivalOrder(int64_t arrival_offset,
                                          absl::Duration timeout) override;

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override;

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override;

  absl::Status CloseWritesWithStatus(absl::Status status) override;

  absl::StatusOr<size_t> Size() override;

  absl::StatusOr<bool> Contains(int64_t seq) override;

  absl::Status SetId(std::string_view id) override;

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override;

  absl::StatusOr<int64_t> GetFinalSeq() override;

 private:
  std::string GetKey(std::string_view key = "") const {
    if (key.empty()) {
      return redis_->GetKey(absl::StrCat("streams:", id_));
    }
    return redis_->GetKey(absl::StrCat("streams:", id_, ":", key));
  }

  absl::StatusOr<Chunk> GetInternal(int64_t seq, absl::Duration timeout)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<Chunk>> TryGet(int64_t seq)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable eglt::Mutex mu_;
  eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);

  bool allow_new_gets_ ABSL_GUARDED_BY(mu_) = true;
  bool writes_closed_ ABSL_GUARDED_BY(mu_) = false;
  size_t num_pending_gets_ ABSL_GUARDED_BY(mu_) = 0;
  absl::Duration ttl_ = absl::InfiniteDuration();

  absl::flat_hash_map<int, std::string> seq_to_stream_id_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, std::string> arrival_offset_to_stream_id_
      ABSL_GUARDED_BY(mu_);

  Redis* absl_nonnull redis_;
  const std::string id_;
  RedisStream stream_;
  std::shared_ptr<Subscription> subscription_;
};

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_H_