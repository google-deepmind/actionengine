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

#ifndef ACTIONENGINE_DISTRIBUTED_LRU_H_
#define ACTIONENGINE_DISTRIBUTED_LRU_H_

#include <any>
#include <list>
#include <string>
#include <string_view>
#include <utility>

#include <absl/container/flat_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "actionengine/util/status_macros.h"

namespace act::distributed {

using EvictCallback =
    absl::AnyInvocable<void(std::string_view key, std::any value) const>;

class LruCache {
 public:
  explicit LruCache(size_t max_entries = 0, EvictCallback on_evicted = nullptr);

  void Add(std::string_view key, std::any value);

  template <typename T>
  void Add(std::string_view key, T&& value) {
    Add(key, std::any(std::forward<T>(value)));
  }

  std::any* absl_nullable Get(std::string_view key);

  template <typename T>
  const T* absl_nullable Get(std::string_view key) {
    const std::any* value = Get(key);
    auto typed_ptr = std::any_cast<const T>(value);
    if (typed_ptr == nullptr && value != nullptr) {
      // TODO: revise if this is the best way to handle type mismatches
      LOG(FATAL) << "Type mismatch for key '" << key << "' in LRU cache.";
      ABSL_ASSUME(false);
    }
    return typed_ptr;
  }

  void Remove(std::string_view key);

  void RemoveOldest();

  [[nodiscard]] size_t Size() const { return map_.size(); }

 private:
  // Maximum number of entries in the cache, 0 means no limit.
  size_t max_entries_ = 0;

  // Optional callback function that is called when an entry is evicted.
  EvictCallback on_evicted_ = nullptr;

  // Doubly linked list of key-value pairs, most recently used at the front.
  // The list stores pairs of key and value.
  std::list<std::pair<std::string, std::any>> list_;
  absl::flat_hash_map<std::string,
                      std::list<std::pair<std::string, std::any>>::iterator>
      map_;
};

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_LRU_H_