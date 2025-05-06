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

#ifndef EGLT_UTIL_MAP_UTIL_H_
#define EGLT_UTIL_MAP_UTIL_H_

#include "eglt/absl_headers.h"

namespace eglt {
/// @private
template <typename RecordKey, typename Value, typename QueryKey>
Value& FindOrDie(absl::flat_hash_map<RecordKey, Value>& map,
                 const QueryKey& key) {
  auto it = map.find(key);
  CHECK(it != map.end());
  return it->second;
}

/// @private
template <typename RecordKey, typename Value, typename QueryKey>
const Value& FindOrDie(const absl::flat_hash_map<RecordKey, Value>& map,
                       const QueryKey& key) {
  const auto it = map.find(key);
  CHECK(it != map.end());
  return it->second;
}
}  // namespace eglt

#endif  // EGLT_UTIL_MAP_UTIL_H_
