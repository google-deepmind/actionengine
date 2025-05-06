#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/status/status_matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "eglt/absl_headers.h"
#include "eglt/net/testing_utils.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::NotNull;

TEST(PairableInMemoryStreamTest, SendsAndReceivesMessages) {
  eglt::testing::PairableInMemoryStream stream1("stream1");
  eglt::testing::PairableInMemoryStream stream2("stream2");

  stream1.PairWith(&stream2);
  EXPECT_OK(stream1.GetStatus());
  EXPECT_OK(stream2.GetStatus());

  EXPECT_OK(stream1.Start());

  eglt::NodeFragment fragment1{
      .id = "test1",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Hello, World!",
          },
      .seq = 0,
      .continued = false,
  };

  EXPECT_OK(stream1.Send({
      .node_fragments = {fragment1},
  }));
  EXPECT_OK(stream1.GetStatus());

  auto message = stream2.Receive();
  EXPECT_OK(stream2.GetStatus());
  EXPECT_TRUE(message.has_value());
  EXPECT_THAT(message->node_fragments.size(), Eq(1));
  EXPECT_EQ(message->node_fragments[0], fragment1);

  eglt::NodeFragment fragment2{
      .id = "test2",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Goodbye, World!",
          },
      .seq = 0,
      .continued = false,
  };
  EXPECT_OK(stream2.Send({
      .node_fragments = {fragment2},
  }));
  EXPECT_OK(stream2.GetStatus());
  message = stream1.Receive();
  EXPECT_OK(stream1.GetStatus());
  EXPECT_TRUE(message.has_value());
  EXPECT_THAT(message->node_fragments.size(), Eq(1));
  EXPECT_EQ(message->node_fragments[0], fragment2);

  stream1.HalfClose();
}

}  // namespace