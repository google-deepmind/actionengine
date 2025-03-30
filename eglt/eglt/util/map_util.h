#ifndef EGLT_UTIL_MAP_UTIL_H_
#define EGLT_UTIL_MAP_UTIL_H_

#include <utility>

#include "eglt/absl_headers.h"

namespace eglt {
template <typename RecordKey, typename Value, typename QueryKey>
Value& FindOrDie(absl::flat_hash_map<RecordKey, Value>& map,
                 const QueryKey& key) {
  auto it = map.find(key);
  CHECK(it != map.end());
  return it->second;
}

template <typename RecordKey, typename Value, typename QueryKey>
const Value& FindOrDie(const absl::flat_hash_map<RecordKey, Value>& map,
                       const QueryKey& key) {
  auto it = map.find(key);
  CHECK(it != map.end());
  return it->second;
}
} // namespace eglt

#endif  // EGLT_UTIL_MAP_UTIL_H_
