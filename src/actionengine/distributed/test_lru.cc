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

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/crc/crc32c.h>
#include <absl/log/log.h>
#include <absl/random/random.h>
#include <absl/status/status_matchers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "actionengine/distributed/lru.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

struct SimpleStruct {
  int a;
  std::string b;

  bool operator==(const SimpleStruct& other) const {
    return a == other.a && b == other.b;
  }
};

struct ComplexStruct {
  int a;
  SimpleStruct b;

  bool operator==(const ComplexStruct& other) const {
    return a == other.a && b == other.b;
  }
};

TEST(LruTest, Remove) {
  const std::unique_ptr<act::distributed::LruCache> cache =
      std::make_unique<act::distributed::LruCache>(/*max_entries=*/0);

  EXPECT_EQ(cache->Get("myKey"), nullptr);

  cache->Add("myKey", 1234);
  const int* value_1 = cache->Get<int>("myKey");
  ASSERT_TRUE(value_1 != nullptr);
  EXPECT_EQ(*value_1, 1234);

  cache->Remove("myKey");
  const std::any* value_2 = cache->Get("myKey");
  EXPECT_EQ(value_2, nullptr);
}

TEST(LruTest, Get) {
  std::unique_ptr<act::distributed::LruCache> cache =
      std::make_unique<act::distributed::LruCache>(/*max_entries=*/0);
  cache->Add("myKey", 1234);
  const int* value_1 = cache->Get<int>("myKey");
  ASSERT_TRUE(value_1 != nullptr);
  EXPECT_EQ(*value_1, 1234);

  cache = std::make_unique<act::distributed::LruCache>(/*max_entries=*/0);
  cache->Add("myKey", 1234);
  const std::any* value_2 = cache->Get("nonsense");
  EXPECT_EQ(value_2, nullptr);
}

TEST(LruTest, EvictCallback) {
  int evict_count = 0;
  auto on_evicted = [&evict_count](std::string_view key, std::any value) {
    ++evict_count;
    if (std::any_cast<int>(&value) != nullptr) {
      EXPECT_EQ(*std::any_cast<int>(&value), 1234);
    } else if (std::any_cast<SimpleStruct>(&value) != nullptr) {
      const SimpleStruct expected{42, "hello"};
      EXPECT_EQ(*std::any_cast<SimpleStruct>(&value), expected);
    } else if (std::any_cast<ComplexStruct>(&value) != nullptr) {
      const ComplexStruct expected{7, {42, "hello"}};
      EXPECT_EQ(*std::any_cast<ComplexStruct>(&value), expected);
    } else {
      FAIL() << "Unexpected type in evict callback";
    }
  };

  // Test eviction of int
  {
    act::distributed::LruCache cache(/*max_entries=*/1, on_evicted);
    cache.Add("key1", 1234);
    cache.Add("key2", 1234);  // This should evict key1
    EXPECT_EQ(evict_count, 1);
  }

  // Test eviction of SimpleStruct
  {
    act::distributed::LruCache cache(/*max_entries=*/1, on_evicted);
    cache.Add("key1", SimpleStruct{42, "hello"});
    cache.Add("key2", SimpleStruct{42, "hello"});  // This should evict key1
    EXPECT_EQ(evict_count, 2);
  }

  // Test eviction of ComplexStruct
  {
    act::distributed::LruCache cache(/*max_entries=*/1, on_evicted);
    cache.Add("key1", ComplexStruct{7, {42, "hello"}});
    cache.Add("key2",
              ComplexStruct{7, {42, "hello"}});  // This should evict key1
    EXPECT_EQ(evict_count, 3);
  }
}

}  // namespace