#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/status/status_matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "eglt/absl_headers.h"
#include "eglt/net/recoverable_stream.h"
#include "eglt/net/testing_utils.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::NotNull;

TEST(RecoverableStreamTest, InitializesWithNoImmediateSideEffects) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable",
      /*timeout=*/absl::InfiniteDuration());

  // Wrapping the stream in a recoverable stream should not change its status.
  EXPECT_OK(stream2->GetStatus());
  EXPECT_OK(stream2_recoverable.GetStatus());

  // The wrapper should not affect the stream's ID.
  EXPECT_THAT(stream2->GetId(), Eq("stream2"));
  EXPECT_THAT(stream2_recoverable.GetId(), Eq("stream2_recoverable"));

  stream2_recoverable.HalfClose();
}

TEST(RecoverableStreamTest, StatusReflectedCorrectlyWithNoPriorOperations) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable",
      /*timeout=*/absl::InfiniteDuration());

  // If get_stream fails to return a valid stream, the recoverable stream should
  // immediately reflect a non-ok status, even if no operations are
  // performed on it. Once the stream is recovered, the status should be ok again.
  PairableInMemoryStream* stream2_ejected = stream2.release();
  EXPECT_THAT(stream2_recoverable.GetStatus(),
              StatusIs(absl::StatusCode::kUnavailable));
  stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
  EXPECT_OK(stream2_recoverable.GetStatus());

  stream2_recoverable.HalfClose();
}

TEST(RecoverableStreamTest, SendsAndReceivesMessages) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable",
      /*timeout=*/absl::InfiniteDuration());

  eglt::NodeFragment fragment{
      .id = "test",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Hello, World!",
          },
      .seq = 0,
      .continued = false,
  };
  eglt::SessionMessage message{
      .node_fragments = {fragment},
  };

  // Wrapper should send messages correctly.
  EXPECT_OK(stream2_recoverable.Send(message));
  EXPECT_OK(stream2_recoverable.GetStatus());
  auto received_message = stream1->Receive();
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());
  EXPECT_TRUE(received_message.has_value());
  EXPECT_TRUE(*received_message == message);

  // Wrapper should receive messages correctly.
  EXPECT_OK(stream1->Send(message));
  EXPECT_OK(stream1->GetStatus());
  received_message = stream2_recoverable.Receive();
  EXPECT_OK(stream2_recoverable.GetStatus());
  EXPECT_OK(stream2->GetStatus());
  EXPECT_TRUE(received_message.has_value());
  EXPECT_THAT(*received_message, Eq(message));

  stream2_recoverable.HalfClose();
}

TEST(RecoverableStreamTest, SendingWaitsForRecovery) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable",
      /*timeout=*/absl::InfiniteDuration());

  eglt::NodeFragment fragment{
      .id = "test",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Hello, World!",
          },
      .seq = 0,
      .continued = false,
  };
  eglt::SessionMessage message{
      .node_fragments = {fragment},
  };

  // Make the stream unavailable by releasing it.
  PairableInMemoryStream* stream2_ejected = stream2.release();
  EXPECT_THAT(stream2_recoverable.GetStatus(),
              StatusIs(absl::StatusCode::kUnavailable));

  // Try to send a message while the stream is unavailable. This should
  // block until the stream is recovered.
  thread::PermanentEvent sent;
  thread::Fiber fiber([&stream2_recoverable, &message, &sent]() {
    EXPECT_OK(stream2_recoverable.Send(message));
    sent.Notify();
  });

  // Wait for a short time to check that the send operation is blocked.
  // Status should be unavailable and IsLost should be true.
  int selected =
      thread::SelectUntil(absl::Now() + absl::Seconds(0.01), {sent.OnEvent()});
  EXPECT_THAT(selected, Eq(-1));  // No event should be selected yet because
                                  // stream2 is unavailable.
  EXPECT_THAT(stream2_recoverable.GetStatus(),
              StatusIs(absl::StatusCode::kUnavailable));
  EXPECT_TRUE(stream2_recoverable.IsLost());

  // Recover the stream and check that the send operation completes.
  stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
  stream2_recoverable.RecoverAndNotify();
  EXPECT_OK(stream2_recoverable.GetStatus());
  EXPECT_FALSE(stream2_recoverable.IsLost());
  selected =
      thread::SelectUntil(absl::Now() + absl::Seconds(0.01), {sent.OnEvent()});
  EXPECT_TRUE(selected == 0);  // The send operation should complete now.
  fiber.Join();

  stream2_recoverable.HalfClose();
}

TEST(RecoverableStreamTest, SendingFailsOnTimeout) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  const absl::Duration timeout = absl::Seconds(0.005);

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable", timeout);

  eglt::NodeFragment fragment{
      .id = "test",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Hello, World!",
          },
      .seq = 0,
      .continued = false,
  };
  eglt::SessionMessage message{
      .node_fragments = {fragment},
  };

  // Make the stream unavailable by releasing it.
  PairableInMemoryStream* stream2_ejected = stream2.release();
  EXPECT_THAT(stream2_recoverable.GetStatus(),
              StatusIs(absl::StatusCode::kUnavailable));

  // Try to send a message while the stream is unavailable. This should
  // block until the stream is recovered.
  absl::Status send_status;
  auto fiber = std::make_unique<thread::Fiber>(
      [&stream2_recoverable, &message, &send_status]() {
        send_status = stream2_recoverable.Send(message);
      });

  // Wait for twice the timeout to check that the send operation is completed
  // with a timeout.
  int selected =
      thread::SelectUntil(absl::Now() + 2 * timeout, {fiber->OnJoinable()});
  EXPECT_THAT(selected, Eq(0));  // operation should be completed
  EXPECT_TRUE(stream2_recoverable.IsLost());
  EXPECT_THAT(send_status, StatusIs(absl::StatusCode::kDeadlineExceeded));
  fiber->Join();

  // Recover the stream and check that a send operation now completes.
  stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
  stream2_recoverable.RecoverAndNotify();
  EXPECT_OK(stream2_recoverable.GetStatus());
  EXPECT_FALSE(stream2_recoverable.IsLost());
  fiber = std::make_unique<thread::Fiber>(
      [&stream2_recoverable, &message, &send_status]() {
        send_status = stream2_recoverable.Send(message);
      });
  selected = thread::SelectUntil(absl::Now() + timeout, {fiber->OnJoinable()});
  EXPECT_TRUE(selected == 0);  // operation should be completed
  EXPECT_OK(send_status);
  EXPECT_OK(stream2_recoverable.GetStatus());
  EXPECT_FALSE(stream2_recoverable.IsLost());
  auto received_message = stream1->Receive();
  ASSERT_TRUE(received_message.has_value());
  EXPECT_TRUE(*received_message == message);
  fiber->Join();

  stream2_recoverable.HalfClose();
}

TEST(RecoverableStreamTest, TimeoutEventFires) {
  using namespace eglt::testing;

  auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
  auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");

  stream1->PairWith(stream2.get());
  EXPECT_OK(stream1->GetStatus());
  EXPECT_OK(stream2->GetStatus());

  const absl::Duration timeout = absl::Seconds(0.005);

  eglt::net::RecoverableStream stream2_recoverable(
      [&stream2]() { return stream2.get(); }, "stream2_recoverable", timeout);

  eglt::NodeFragment fragment{
      .id = "test",
      .chunk =
          eglt::Chunk{
              .metadata = {.mimetype = "text/plain"},
              .data = "Hello, World!",
          },
      .seq = 0,
      .continued = false,
  };
  eglt::SessionMessage message{
      .node_fragments = {fragment},
  };

  // Make the stream unavailable by releasing it.
  PairableInMemoryStream* stream2_ejected = stream2.release();
  EXPECT_THAT(stream2_recoverable.GetStatus(),
              StatusIs(absl::StatusCode::kUnavailable));

  // Try to send a message while the stream is unavailable. This should
  // block until the stream is recovered.
  absl::Status send_status;
  auto fiber = std::make_unique<thread::Fiber>(
      [&stream2_recoverable, &message, &send_status]() {
        send_status = stream2_recoverable.Send(message);
      });

  // Wait for twice the timeout to check that the send operation is completed
  // with a timeout.
  int selected = thread::SelectUntil(absl::Now() + 2 * timeout,
                                     {stream2_recoverable.OnTimeout()});

  EXPECT_THAT(selected, Eq(0));  // timeout event should be selected

  fiber->Join();
  EXPECT_TRUE(stream2_recoverable.IsLost());
  EXPECT_THAT(send_status, StatusIs(absl::StatusCode::kDeadlineExceeded));

  stream2_recoverable.CloseAndNotify(/*ignore_lost=*/true);

  // closing the underlying stream
  stream2_ejected->HalfClose();
}

}  // namespace
