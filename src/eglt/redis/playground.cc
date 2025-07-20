#include <eglt/util/random.h>

#include "eglt/absl_headers.h"
#include "eglt/redis/redis.h"
#include "eglt/redis/streams.h"

absl::Status RunPlayground() {
  using namespace eglt::redis;

  ASSIGN_OR_RETURN(std::unique_ptr<Redis> redis,
                   Redis::Connect("localhost", 6379));

  ASSIGN_OR_RETURN(HelloReply reply, redis->Hello());

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