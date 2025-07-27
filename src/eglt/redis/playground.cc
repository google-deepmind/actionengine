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

absl::Status RunPlayground() {
  using namespace eglt::redis;

  ASSIGN_OR_RETURN(std::unique_ptr<Redis> redis,
                   Redis::Connect("localhost", 6379));

  ASSIGN_OR_RETURN(HelloReply reply, redis->Hello());

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

  RETURN_IF_ERROR(redis->Ping("PONG"));

  RETURN_IF_ERROR(redis->Set("int_value", 10));
  ASSIGN_OR_RETURN(const int64_t value, redis->Get<int64_t>("int_value"));
  LOG(INFO) << "int64_t value: " << value;

  RETURN_IF_ERROR(redis->Set("bool_value", true));
  ASSIGN_OR_RETURN(const bool bool_value, redis->Get<bool>("bool_value"));
  LOG(INFO) << "bool value: " << std::boolalpha << bool_value;

  RETURN_IF_ERROR(redis->Set("double_value", 3.14));
  ASSIGN_OR_RETURN(const double double_value,
                   redis->Get<double>("double_value"));
  LOG(INFO) << "double value: " << double_value;

  RETURN_IF_ERROR(redis->Set("string_value", "hello"));
  ASSIGN_OR_RETURN(const std::string string_value,
                   redis->Get<std::string>("string_value"));
  LOG(INFO) << "string value: " << string_value;

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
  CHECK_OK(RunPlayground());
  return 0;
}