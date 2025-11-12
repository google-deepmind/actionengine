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

#include <absl/crc/crc32c.h>
#include <absl/log/log.h>
#include <absl/random/random.h>
#include <absl/status/status_matchers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "actionengine/distributed/groupcache.h"

namespace {

TEST(Groupcache, Basic) {
  act::distributed::Cache cache(/*max_entries=*/2);

  cache.Add("key1", "value1");
  cache.Add("key2", "value2");
  LOG(INFO) << "Cache Size: " << cache.Size();
  LOG(INFO) << "Cache Items: " << cache.Items();
  EXPECT_EQ(cache.Size(), 20);  // "key1" + "value1" + "key2" + "value2"

  // EXPECT_EQ();

  cache.Add("key3", "-value3-");  // This should evict "key1"
  LOG(INFO) << "Cache Size after adding key3: " << cache.Size();
  LOG(INFO) << "Cache Items after adding key3: " << cache.Items();
  EXPECT_EQ(cache.Size(), 22);  // "key2" + "value2" + "key3" + "-value3-"
}

}  // namespace