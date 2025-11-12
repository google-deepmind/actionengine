#include "actionengine/distributed/lru.h"

namespace act::distributed {

LruCache::LruCache(size_t max_entries, EvictCallback on_evicted)
    : max_entries_(max_entries), on_evicted_(std::move(on_evicted)) {}

void LruCache::Add(std::string_view key, std::any value) {
  if (const auto it = map_.find(key); it != map_.end()) {
    // Key exists, update value and move to front.
    it->second->second = std::move(value);
    list_.splice(list_.begin(), list_, it->second);
    return;
  }

  // Key does not exist, insert new entry at front.
  list_.emplace_front(key, std::move(value));
  map_[key] = list_.begin();

  // Evict least recently used entry if over capacity.
  if (max_entries_ > 0 && list_.size() > max_entries_) {
    auto last = list_.end();
    --last;
    if (on_evicted_) {
      on_evicted_(last->first, last->second);
    }
    map_.erase(last->first);
    list_.pop_back();
  }
}

std::any* absl_nullable LruCache::Get(std::string_view key) {
  const auto it = map_.find(key);
  if (it == map_.end()) {
    return nullptr;
  }
  // Move the accessed entry to the front of the list.
  list_.splice(list_.begin(), list_, it->second);
  return &it->second->second;
}

void LruCache::Remove(std::string_view key) {
  const auto it = map_.find(key);
  if (it == map_.end()) {
    return;
  }
  if (on_evicted_) {
    on_evicted_(it->second->first, it->second->second);
  }
  list_.erase(it->second);
  map_.erase(it);
}

void LruCache::RemoveOldest() {
  if (list_.empty()) {
    return;
  }
  auto last = list_.end();
  --last;
  if (on_evicted_) {
    on_evicted_(last->first, last->second);
  }
  map_.erase(last->first);
  list_.pop_back();
}

}  // namespace act::distributed