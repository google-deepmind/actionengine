// // Copyright 2025 Google LLC
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
// #include <optional>
// #include <string>
// #include <utility>
// #include <vector>
//
// #include <absl/status/status_matchers.h>
// #include <gmock/gmock.h>
// #include <gtest/gtest.h>
//
// #include "actionengine/net/recoverable_stream.h"
// #include "actionengine/net/websockets/websockets.h"
// #include "actionengine/service/service.h"
//
// #define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())
//
// namespace {
//
// using ::absl_testing::StatusIs;
// using ::testing::ElementsAre;
// using ::testing::Eq;
// using ::testing::NotNull;
//
// TEST(RecoverableStreamTest, InitializesWithNoImmediateSideEffects) {
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable",
//       /*timeout=*/absl::InfiniteDuration());
//
//   // Wrapping the stream in a recoverable stream should not change its status.
//   EXPECT_OK(stream2->GetStatus());
//   EXPECT_OK(stream2_recoverable.GetStatus());
//
//   // The wrapper should not affect the stream's ID.
//   EXPECT_THAT(stream2->GetId(), Eq("stream2"));
//   EXPECT_THAT(stream2_recoverable.GetId(), Eq("stream2_recoverable"));
//
//   stream2_recoverable.HalfClose();
// }
//
// TEST(RecoverableStreamTest, StatusReflectedCorrectlyWithNoPriorOperations) {
//   using namespace act::testing;
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable",
//       /*timeout=*/absl::InfiniteDuration());
//
//   // If get_stream fails to return a valid stream, the recoverable stream should
//   // immediately reflect a non-ok status, even if no operations are
//   // performed on it. Once the stream is recovered, the status should be ok again.
//   PairableInMemoryStream* stream2_ejected = stream2.release();
//   EXPECT_THAT(stream2_recoverable.GetStatus(),
//               StatusIs(absl::StatusCode::kUnavailable));
//   stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
//   EXPECT_OK(stream2_recoverable.GetStatus());
//
//   stream2_recoverable.HalfClose();
// }
//
// TEST(RecoverableStreamTest, SendsAndReceivesMessages) {
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable",
//       /*timeout=*/absl::InfiniteDuration());
//
//   act::NodeFragment fragment{
//       .id = "test",
//       .chunk =
//           act::Chunk{
//               .metadata = {.mimetype = "text/plain"},
//               .data = "Hello, World!",
//           },
//       .seq = 0,
//       .continued = false,
//   };
//   act::WireMessage message{
//       .node_fragments = {fragment},
//   };
//
//   // Wrapper should send messages correctly.
//   EXPECT_OK(stream2_recoverable.Send(message));
//   EXPECT_OK(stream2_recoverable.GetStatus());
//   auto received_message = stream1->Receive();
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//   EXPECT_TRUE(received_message.has_value());
//   EXPECT_TRUE(*received_message == message);
//
//   // Wrapper should receive messages correctly.
//   EXPECT_OK(stream1->Send(message));
//   EXPECT_OK(stream1->GetStatus());
//   received_message = stream2_recoverable.Receive();
//   EXPECT_OK(stream2_recoverable.GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//   EXPECT_TRUE(received_message.has_value());
//   EXPECT_THAT(*received_message, Eq(message));
//
//   stream2_recoverable.HalfClose();
// }
//
// TEST(RecoverableStreamTest, SendingWaitsForRecovery) {
//   using namespace act::testing;
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable",
//       /*timeout=*/absl::InfiniteDuration());
//
//   act::NodeFragment fragment{
//       .id = "test",
//       .chunk =
//           act::Chunk{
//               .metadata = {.mimetype = "text/plain"},
//               .data = "Hello, World!",
//           },
//       .seq = 0,
//       .continued = false,
//   };
//   act::WireMessage message{
//       .node_fragments = {fragment},
//   };
//
//   // Make the stream unavailable by releasing it.
//   PairableInMemoryStream* stream2_ejected = stream2.release();
//   EXPECT_THAT(stream2_recoverable.GetStatus(),
//               StatusIs(absl::StatusCode::kUnavailable));
//
//   // Try to send a message while the stream is unavailable. This should
//   // block until the stream is recovered.
//   thread::PermanentEvent sent;
//   thread::Fiber fiber([&stream2_recoverable, &message, &sent]() {
//     EXPECT_OK(stream2_recoverable.Send(message));
//     sent.Notify();
//   });
//
//   // Wait for a short time to check that the send operation is blocked.
//   // Status should be unavailable and IsLost should be true.
//   int selected =
//       thread::SelectUntil(absl::Now() + absl::Seconds(0.01), {sent.OnEvent()});
//   EXPECT_THAT(selected, Eq(-1));  // No event should be selected yet because
//                                   // stream2 is unavailable.
//   EXPECT_THAT(stream2_recoverable.GetStatus(),
//               StatusIs(absl::StatusCode::kUnavailable));
//   EXPECT_TRUE(stream2_recoverable.IsLost());
//
//   // Recover the stream and check that the send operation completes.
//   stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
//   stream2_recoverable.RecoverAndNotify();
//   EXPECT_OK(stream2_recoverable.GetStatus());
//   EXPECT_FALSE(stream2_recoverable.IsLost());
//   selected =
//       thread::SelectUntil(absl::Now() + absl::Seconds(0.01), {sent.OnEvent()});
//   EXPECT_TRUE(selected == 0);  // The send operation should complete now.
//   fiber.Join();
//
//   stream2_recoverable.HalfClose();
// }
//
// TEST(RecoverableStreamTest, SendingFailsOnTimeout) {
//   using namespace act::testing;
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   const absl::Duration timeout = absl::Seconds(0.005);
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable", timeout);
//
//   act::NodeFragment fragment{
//       .id = "test",
//       .chunk =
//           act::Chunk{
//               .metadata = {.mimetype = "text/plain"},
//               .data = "Hello, World!",
//           },
//       .seq = 0,
//       .continued = false,
//   };
//   act::WireMessage message{
//       .node_fragments = {fragment},
//   };
//
//   // Make the stream unavailable by releasing it.
//   PairableInMemoryStream* stream2_ejected = stream2.release();
//   EXPECT_THAT(stream2_recoverable.GetStatus(),
//               StatusIs(absl::StatusCode::kUnavailable));
//
//   // Try to send a message while the stream is unavailable. This should
//   // block until the stream is recovered.
//   absl::Status send_status;
//   auto fiber = std::make_unique<thread::Fiber>(
//       [&stream2_recoverable, &message, &send_status]() {
//         send_status = stream2_recoverable.Send(message);
//       });
//
//   // Wait for twice the timeout to check that the send operation is completed
//   // with a timeout.
//   int selected =
//       thread::SelectUntil(absl::Now() + 2 * timeout, {fiber->OnJoinable()});
//   EXPECT_THAT(selected, Eq(0));  // operation should be completed
//   EXPECT_TRUE(stream2_recoverable.IsLost());
//   EXPECT_THAT(send_status, StatusIs(absl::StatusCode::kDeadlineExceeded));
//   fiber->Join();
//
//   // Recover the stream and check that a send operation now completes.
//   stream2 = std::unique_ptr<PairableInMemoryStream>(stream2_ejected);
//   stream2_recoverable.RecoverAndNotify();
//   EXPECT_OK(stream2_recoverable.GetStatus());
//   EXPECT_FALSE(stream2_recoverable.IsLost());
//   fiber = std::make_unique<thread::Fiber>(
//       [&stream2_recoverable, &message, &send_status]() {
//         send_status = stream2_recoverable.Send(message);
//       });
//   selected = thread::SelectUntil(absl::Now() + timeout, {fiber->OnJoinable()});
//   EXPECT_TRUE(selected == 0);  // operation should be completed
//   EXPECT_OK(send_status);
//   EXPECT_OK(stream2_recoverable.GetStatus());
//   EXPECT_FALSE(stream2_recoverable.IsLost());
//   auto received_message = stream1->Receive();
//   ASSERT_TRUE(received_message.has_value());
//   EXPECT_TRUE(*received_message == message);
//   fiber->Join();
//
//   stream2_recoverable.HalfClose();
// }
//
// TEST(RecoverableStreamTest, TimeoutEventFires) {
//   using namespace act::testing;
//
//   auto stream1 = std::make_unique<PairableInMemoryStream>("stream1");
//   auto stream2 = std::make_unique<PairableInMemoryStream>("stream2");
//
//   stream1->PairWith(stream2.get());
//   EXPECT_OK(stream1->GetStatus());
//   EXPECT_OK(stream2->GetStatus());
//
//   const absl::Duration timeout = absl::Seconds(0.005);
//
//   act::net::RecoverableStream stream2_recoverable(
//       [&stream2]() { return stream2.get(); }, "stream2_recoverable", timeout);
//
//   act::NodeFragment fragment{
//       .id = "test",
//       .chunk =
//           act::Chunk{
//               .metadata = {.mimetype = "text/plain"},
//               .data = "Hello, World!",
//           },
//       .seq = 0,
//       .continued = false,
//   };
//   act::WireMessage message{
//       .node_fragments = {fragment},
//   };
//
//   // Make the stream unavailable by releasing it.
//   PairableInMemoryStream* stream2_ejected = stream2.release();
//   EXPECT_THAT(stream2_recoverable.GetStatus(),
//               StatusIs(absl::StatusCode::kUnavailable));
//
//   // Try to send a message while the stream is unavailable. This should
//   // block until the stream is recovered.
//   absl::Status send_status;
//   auto fiber = std::make_unique<thread::Fiber>(
//       [&stream2_recoverable, &message, &send_status]() {
//         send_status = stream2_recoverable.Send(message);
//       });
//
//   // Wait for twice the timeout to check that the send operation is completed
//   // with a timeout.
//   int selected = thread::SelectUntil(absl::Now() + 2 * timeout,
//                                      {stream2_recoverable.OnTimeout()});
//
//   EXPECT_THAT(selected, Eq(0));  // timeout event should be selected
//
//   fiber->Join();
//   EXPECT_TRUE(stream2_recoverable.IsLost());
//   EXPECT_THAT(send_status, StatusIs(absl::StatusCode::kDeadlineExceeded));
//
//   stream2_recoverable.CloseAndNotify();
//
//   // closing the underlying stream
//   stream2_ejected->HalfClose();
// }
//
// }  // namespace
