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

#include <absl/debugging/failure_signal_handler.h>
#include <eglt/util/random.h>

#include "eglt/redis/chunk_store.h"
#include "eglt/redis/redis.h"
#include "eglt/redis/streams.h"

void GetFromChunkStore(eglt::redis::ChunkStore* absl_nonnull store, int64_t seq,
                       absl::Duration timeout = absl::InfiniteDuration()) {
  auto chunk_or_status = store->Get(seq, timeout);
  if (!chunk_or_status.ok()) {
    LOG(ERROR) << "Failed to get chunk for seq " << seq << ": "
               << chunk_or_status.status();
    return;
  }
  const auto& chunk = chunk_or_status.value();
  LOG(INFO) << "Got chunk for seq " << seq << ": " << chunk;
}

absl::Status RunAsyncPlayground() {
  using namespace eglt::redis;

  ASSIGN_OR_RETURN(std::unique_ptr<Redis> redis,
                   Redis::Connect("localhost", 6379));

  RETURN_IF_ERROR(redis->ExecuteCommand("PING").status());

  ASSIGN_OR_RETURN(auto reply,
                   redis->ExecuteCommand("PING", {"non-trivial PONG"}));
  DLOG(INFO) << "Reply to PING: " << eglt::ConvertToOrDie<std::string>(reply);

  std::string chunk_store_id = eglt::GenerateUUID4();
  auto store = std::make_unique<ChunkStore>(redis.get(), chunk_store_id,
                                            absl::Seconds(300));
  // Wait for subscription to become ready (TODO: use a more robust way to
  // ensure the store is ready).
  // eglt::SleepFor(absl::Seconds(1));
  RETURN_IF_ERROR(store->Put(0, {}, /*final=*/false));
  RETURN_IF_ERROR(store->Put(2, {}, /*final=*/true));
  LOG(INFO) << "Final seq: " << store->GetFinalSeqOrDie();
  LOG(INFO) << "Size: " << store->SizeOrDie();

  thread::Detach({}, [store = store.get()]() {
    LOG(INFO) << "Fiber started, waiting for chunk with seq 1";
    GetFromChunkStore(store, 1);
  });
  eglt::SleepFor(absl::Seconds(1));
  LOG(INFO) << "Putting chunk with seq 1";
  RETURN_IF_ERROR(store->Put(1, {}, /*final=*/false));
  LOG(INFO) << "Put chunk with seq 1.";
  LOG(INFO) << "Final seq: " << store->GetFinalSeqOrDie();
  LOG(INFO) << "Size: " << store->SizeOrDie();
  ASSIGN_OR_RETURN(auto chunk, store->Get(1, absl::Seconds(10)));
  LOG(INFO) << "Got chunk: " << chunk;

  RETURN_IF_ERROR(
      redis->ExecuteCommand("SET", {"test_key", "test_value"}).status());
  ASSIGN_OR_RETURN(auto value, redis->ExecuteCommand("GET", {"test_key"}));
  DLOG(INFO) << "Value for 'test_key': "
             << eglt::ConvertToOrDie<std::string>(value);

  ASSIGN_OR_RETURN(std::shared_ptr<Subscription> subscription,
                   redis->Subscribe("test_channel", [](Reply reply) {
                     DLOG(INFO) << "Received message: "
                                << reply.ConsumeStringContentOrDie();
                   }));
  RETURN_IF_ERROR(
      redis->ExecuteCommand("PUBLISH", {"test_channel", "Hello, Redis!"})
          .status());

  RETURN_IF_ERROR(redis->Set("int_value", 42));
  ASSIGN_OR_RETURN(int int_value, redis->Get<int64_t>("int_value"));
  DLOG(INFO) << "int_value: " << int_value;

  RedisStream stream(redis.get(), eglt::GenerateUUID4());
  ASSIGN_OR_RETURN(
      StreamMessageId id,
      stream.XAdd({{"seq", "0"}, {"data", "<data>"}, {"final", "1"}}));
  ASSIGN_OR_RETURN(
      std::vector<StreamMessage> messages,
      stream.XRead(StreamMessageId{}, 10, absl::Milliseconds(1000)));
  for (const auto& message : messages) {
    LOG(INFO) << message;
  }

  return absl::OkStatus();
}

int main() {
  absl::InstallFailureSignalHandler(absl::FailureSignalHandlerOptions());
  CHECK_OK(RunAsyncPlayground());
  return 0;
}