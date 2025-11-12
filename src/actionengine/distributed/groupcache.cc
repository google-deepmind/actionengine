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

#include "actionengine/distributed/groupcache.h"

#include <any>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <absl/base/call_once.h>
#include <absl/base/nullability.h>
#include <absl/container/flat_hash_map.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "actionengine/concurrency/concurrency.h"

namespace act::distributed {

Cache::Cache(size_t max_entries)
    : lru_(max_entries, [this](std::string_view key, const std::any& value)
                            ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
                              const auto& value_str =
                                  std::any_cast<std::string>(value);
                              bytes_ -= key.size() + value_str.size();
                              ++evictions_;
                            }) {}

void Cache::Add(std::string_view key, std::string_view value) {
  act::MutexLock lock(&mu_);
  lru_.Add(key, std::string(value));
  bytes_ += key.size() + value.size();
}

const std::string* absl_nullable Cache::Get(std::string_view key) {
  act::MutexLock lock(&mu_);
  ++gets_;
  const auto* value = lru_.Get<std::string>(key);
  if (value == nullptr) {
    return nullptr;
  }
  ++hits_;
  return value;
}

void Cache::Remove(std::string_view key) {
  act::MutexLock l(&mu_);
  lru_.Remove(key);
}

void Cache::RemoveOldest() {
  act::MutexLock l(&mu_);
  lru_.RemoveOldest();
}

Group::Group(std::string_view name, size_t cache_bytes, Getter getter)
    : name_(name), getter_(std::move(getter)), cache_bytes_(cache_bytes) {}

absl::StatusOr<std::string_view> Group::LookupCache(std::string_view key) {
  if (cache_bytes_ == 0) {
    return absl::InternalError("Cache not configured");
  }
  if (const std::string* value = main_cache_.Get(key); value != nullptr) {
    return *value;
  }
  if (const std::string* value = hot_cache_.Get(key); value != nullptr) {
    return *value;
  }
  return absl::NotFoundError("Key not found in cache");
}

absl::StatusOr<std::string> Group::Load(std::string_view key,
                                        Sink* absl_nonnull dest,
                                        bool* absl_nonnull dest_populated) {
  ++stats_.loads;
  return load_group_.Do<std::string>(
      key, [key, dest, dest_populated, this]() -> absl::StatusOr<std::string> {
        absl::StatusOr<std::string_view> value_in_cache = LookupCache(key);
        if (value_in_cache.ok()) {
          ++stats_.cache_hits;
          return std::string(*value_in_cache);
        }

        ++stats_.loads_deduped;
        if (peers_ == nullptr) {
          return absl::InternalError("Peers are not initialised.");
        }
        if (std::optional<ServiceGetter> peer = peers_->PickPeer(key); peer) {
          if (absl::StatusOr<std::string> value = GetFromPeer(&*peer, key);
              value.ok()) {
            ++stats_.peer_loads;
            return *value;
          }
          ++stats_.peer_errors;
        }

        absl::StatusOr<std::string_view> value = GetLocally(key, dest);
        if (!value.ok()) {
          ++stats_.local_load_errors;
          return value.status();
        }
        ++stats_.local_loads;
        *dest_populated = true;

        PopulateCache(key, *value, &main_cache_).IgnoreError();
        return std::string(*value);
      });
}

absl::Status Group::PopulateCache(std::string_view key, std::string_view value,
                                  Cache* absl_nonnull cache) {
  if (cache_bytes_ == 0) {
    return absl::InternalError("Cache not configured");
  }
  if (key.size() + value.size() > cache_bytes_) {
    // Don't cache items that are larger than the total cache size.
    return absl::OutOfRangeError("Item too large to cache");
  }
  cache->Add(key, value);

  // Crash OK: make sure the cache is one of the two we know about, it's UB
  // to pass in some other cache.
  CHECK(cache == &main_cache_ || cache == &hot_cache_)
      << "PopulateCache: unknown cache; this use case is not supported and "
         "is "
         "likely a bug.";

  // Evict items until we're under budget.
  while (true) {
    if (main_cache_.Size() + hot_cache_.Size() <= cache_bytes_) {
      break;
    }

    Cache* absl_nonnull evict_from =
        hot_cache_.Size() > main_cache_.Size() / 8 ? &hot_cache_ : &main_cache_;
    evict_from->RemoveOldest();
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string_view> Group::GetLocally(std::string_view key,
                                                   Sink* dest) {
  RETURN_IF_ERROR(getter_(key, dest));
  return dest->View();
}

absl::StatusOr<std::string> Group::GetFromPeer(ServiceGetter* absl_nonnull peer,
                                               std::string_view key) {
  GetRequest request{.group = name_, .key = std::string(key)};
  GetResponse response;
  RETURN_IF_ERROR((*peer)(&request, &response));
  if (!response.value) {
    return absl::NotFoundError("Key not found on peer");
  }

  if (absl::Uniform(rng_, 0, 10) < 1) {
    PopulateCache(key, *response.value, &hot_cache_).IgnoreError();
  }

  return *response.value;
}

Group* absl_nonnull Instance::NewGroup(std::string_view name,
                                       uint64_t cache_bytes, Getter getter,
                                       PeerPicker* absl_nullable peers) {}

Group* absl_nullable Instance::GetGroup(std::string_view name) {
  act::MutexLock lock(&mu_);
  const auto it = groups_.find(std::string(name));
  if (it == groups_.end()) {
    return nullptr;
  }
  return it->second.get();
}

static absl::once_flag init_peer_server_flag;

static void InitPeerServer() {}

}  // namespace act::distributed