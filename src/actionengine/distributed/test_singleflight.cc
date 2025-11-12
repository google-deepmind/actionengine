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
#include <absl/random/random.h>
#include <absl/status/status_matchers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "actionengine/distributed/singleflight.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

using ::absl_testing::IsOkAndHolds;

TEST(SingleFlightTest, Do) {
  act::distributed::FlightGroup group;

  const absl::StatusOr<std::string> result = group.Do<std::string>(
      "key", []() mutable { return std::string("hello"); });
  EXPECT_THAT(result, IsOkAndHolds("hello"));
}

TEST(SingleFlightTest, DoErr) {
  act::distributed::FlightGroup group;

  const absl::StatusOr<std::string> result = group.Do<std::string>(
      "key", []() mutable { return absl::InternalError("error"); });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInternal);
  EXPECT_EQ(result.status().message(), "error");
}

TEST(SingleFlightTest, DoDupSuppress) {
  act::distributed::FlightGroup group;
  std::atomic<int32_t> calls;

  thread::Channel<std::string> channel(0);
  auto fn = [&calls, &channel]() mutable -> absl::StatusOr<std::string> {
    calls.fetch_add(1);

    std::string value;
    bool ok;
    thread::Select({channel.reader()->OnRead(&value, &ok)});

    return value;
  };

  constexpr size_t n = 10;
  act::distributed::WaitGroup wg;
  for (size_t i = 0; i < n; ++i) {
    wg.Add(1);
    thread::Detach({}, [&wg, &group, &fn]() mutable {
      EXPECT_THAT(group.Do<std::string>("key", fn), IsOkAndHolds("hello"));
      wg.Done();
    });
  }

  act::SleepFor(absl::Milliseconds(100));
  channel.writer()->Write("hello");
  wg.Wait();
  EXPECT_EQ(calls.load(), 1);
}

}  // namespace