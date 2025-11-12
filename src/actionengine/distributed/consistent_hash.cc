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

#include "actionengine/distributed/consistent_hash.h"

#include <absl/crc/crc32c.h>
#include <zlib.h>

namespace act::distributed {
absl::crc32c_t DefaultHash(std::string_view data) {
  return absl::crc32c_t(
      crc32(0, reinterpret_cast<const Bytef*>(data.data()), data.size()));
}

ConsistentMap::ConsistentMap(int32_t replicas, Hash hash)
    : hash_(std::move(hash)), replicas_(replicas) {
  CHECK(replicas_ > 0) << "replicas must be positive";  // Crash OK: invariant
}

bool ConsistentMap::Empty() const {
  return keys_.empty();
}

void ConsistentMap::Add(std::initializer_list<std::string_view> keys) {
  for (const auto& key : keys) {
    for (int32_t i = 0; i < replicas_; ++i) {
      std::string replica_key = absl::StrFormat("%d%s", i, key);
      const auto hash = static_cast<uint32_t>(hash_(replica_key));
      keys_.push_back(hash);
      hash_map_[hash] = key;
    }
  }
  std::sort(keys_.begin(), keys_.end());
}

std::string ConsistentMap::Get(std::string_view key) const {
  if (keys_.empty()) {
    return "";
  }
  const auto hash = static_cast<uint32_t>(hash_(key));

  auto it = std::lower_bound(keys_.begin(), keys_.end(), hash, std::less());
  if (it == keys_.end()) {
    it = keys_.begin();
  }
  const auto found = hash_map_.find(*it);
  return found->second;
}
}  // namespace act::distributed