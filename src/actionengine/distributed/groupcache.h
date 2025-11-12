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

#ifndef ACTIONENGINE_DISTRIBUTED_GROUPCACHE_H_
#define ACTIONENGINE_DISTRIBUTED_GROUPCACHE_H_

#include <atomic>

#include <absl/base/nullability.h>
#include <absl/functional/any_invocable.h>
#include <absl/random/random.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/distributed/lru.h"
#include "actionengine/distributed/peers.h"
#include "actionengine/distributed/singleflight.h"
#include "actionengine/distributed/sinks.h"

namespace act::distributed {

using Getter = absl::AnyInvocable<absl::Status(std::string_view key,
                                               Sink* absl_nonnull sink)>;

struct Stats {
  std::atomic<uint64_t> gets{0};
  std::atomic<uint64_t> cache_hits{0};
  std::atomic<uint64_t> peer_loads{0};
  std::atomic<uint64_t> peer_errors{0};
  std::atomic<uint64_t> loads{0};
  std::atomic<uint64_t> loads_deduped{0};
  std::atomic<uint64_t> local_loads{0};
  std::atomic<uint64_t> local_load_errors{0};
  std::atomic<uint64_t> server_requests{0};
};

struct CacheStats {
  uint64_t bytes;
  uint64_t items;
  uint64_t gets;
  uint64_t hits;
  uint64_t evictions;
};

class Cache {
 public:
  explicit Cache(size_t max_entries = 0);

  void Add(std::string_view key, std::string_view value);

  const std::string* absl_nullable Get(std::string_view key);

  void Remove(std::string_view key);

  void RemoveOldest();

  CacheStats Stats() const {
    act::MutexLock l(&mu_);
    return CacheStats{bytes_, lru_.Size(), gets_, hits_, evictions_};
  }

  uint64_t Size() const {
    act::MutexLock l(&mu_);
    return bytes_;
  }

  uint64_t Items() const {
    act::MutexLock l(&mu_);
    return lru_.Size();
  }

 private:
  mutable act::Mutex mu_;
  LruCache lru_ ABSL_GUARDED_BY(mu_);

  uint64_t bytes_ ABSL_GUARDED_BY(mu_) = 0;
  uint64_t gets_ ABSL_GUARDED_BY(mu_) = 0;
  uint64_t hits_ ABSL_GUARDED_BY(mu_) = 0;
  uint64_t evictions_ ABSL_GUARDED_BY(mu_) = 0;
};

class Group {
 public:
  Group(std::string_view name, size_t cache_bytes, Getter getter);

  std::string_view Name() const { return name_; }

  absl::StatusOr<std::string_view> LookupCache(std::string_view key);

  absl::StatusOr<std::string> Load(std::string_view key,
                                   Sink* absl_nonnull dest,
                                   bool* absl_nonnull dest_populated);

 private:
  absl::Status PopulateCache(std::string_view key, std::string_view value,
                             Cache* absl_nonnull cache);

  absl::StatusOr<std::string_view> GetLocally(std::string_view key,
                                              Sink* absl_nonnull dest);

  absl::StatusOr<std::string> GetFromPeer(ServiceGetter* absl_nonnull peer,
                                          std::string_view key);

  std::string name_;

  Getter getter_;
  PeerPicker* absl_nullable peers_ = nullptr;

  Cache main_cache_;
  Cache hot_cache_;
  uint64_t cache_bytes_ = std::numeric_limits<
      uint64_t>::max();  // Total bytes allocated for both caches.

  FlightGroup load_group_;

  Stats stats_;

  absl::BitGen rng_;

  absl::once_flag peers_init_once_;
};

class Instance {
 public:
  Group* absl_nonnull NewGroup(std::string_view name, uint64_t cache_bytes,
                               Getter getter,
                               PeerPicker* absl_nullable peers = nullptr);

  Group* absl_nullable GetGroup(std::string_view name);

 private:
  mutable act::Mutex mu_;
  absl::flat_hash_map<std::string, std::unique_ptr<Group>> groups_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_GROUPCACHE_H_
