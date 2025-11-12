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

#include "actionengine/distributed/consistent_hash.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

// Override the hash function to return easier to reason about values. Assumes
// the keys can be converted to an integer.
absl::crc32c_t SimpleHash(std::string_view data) {
  uint32_t value;
  CHECK(absl::SimpleAtoi(data, &value))  // Crash OK: should never happen,
      // the check is here simply not to misuse the helper
      << "Failed to convert " << data << " to integer";
  return absl::crc32c_t{value};
}

TEST(ConsistentHashTest, Basic) {
  act::distributed::ConsistentMap map(3, SimpleHash);

  EXPECT_TRUE(map.Empty());

  map.Add({"6", "4", "2"});

  EXPECT_FALSE(map.Empty());

  absl::flat_hash_map<std::string, std::string> expected = {
      {"2", "2"}, {"11", "2"}, {"23", "4"}, {"27", "2"}};

  for (const auto& [key, value] : expected) {
    EXPECT_EQ(map.Get(key), value);
  }

  map.Add({"8"});
  expected["27"] = "8";

  for (const auto& [key, value] : expected) {
    EXPECT_EQ(map.Get(key), value);
  }
}

TEST(ConsistentHashTest, Consistency) {
  act::distributed::ConsistentMap map1(1);
  act::distributed::ConsistentMap map2(1);

  map1.Add({"Bill", "Bob", "Bonny"});
  map2.Add({"Bob", "Bonny", "Bill"});

  EXPECT_EQ(map1.Get("Ben"), map2.Get("Ben"));

  map2.Add({"Becky", "Ben", "Bobby"});

  for (const auto& key : {"Ben", "Bob", "Bonny"}) {
    LOG(INFO) << "Key: " << key << " -> " << map1.Get(key) << " vs "
              << map2.Get(key);
    EXPECT_EQ(map1.Get(key), map2.Get(key));
  }
}

}  // namespace