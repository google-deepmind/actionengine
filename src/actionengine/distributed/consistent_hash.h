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

#ifndef ACTIONENGINE_DISTRIBUTED_CONSISTENT_HASH_H_
#define ACTIONENGINE_DISTRIBUTED_CONSISTENT_HASH_H_

#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/crc/crc32c.h>
#include <absl/log/check.h>

namespace act::distributed {

absl::crc32c_t DefaultHash(std::string_view data);

using Hash = absl::AnyInvocable<absl::crc32c_t(std::string_view) const>;

class ConsistentMap {
 public:
  explicit ConsistentMap(int32_t replicas, Hash hash = &DefaultHash);

  [[nodiscard]] bool Empty() const;

  void Add(std::initializer_list<std::string_view> keys);

  [[nodiscard]] std::string Get(std::string_view key) const;

 private:
  Hash hash_;
  int32_t replicas_ = 1;
  std::vector<uint32_t> keys_;
  absl::flat_hash_map<uint32_t, std::string> hash_map_;
};

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_CONSISTENT_HASH_H_
