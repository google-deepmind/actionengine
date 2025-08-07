// ------------------------------------------------------------------------------
// This example shows how to define custom types that can be serialized to
// ActionEngine chunks, completely at your own discretion and at application level.
//
// You can run this example with:
// blaze run //third_party/eglt/examples:chunks_from_custom_types
// ------------------------------------------------------------------------------

#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <absl/debugging/failure_signal_handler.h>
#include <eglt/data/eg_structs.h>
#include <eglt/nodes/async_node.h>
#include <eglt/redis/chunk_store.h>
#include <eglt/redis/redis.h>
#include <eglt/stores/local_chunk_store.h>
#include <eglt/util/random.h>

// Simply some type aliases to make the code more readable. These declarations
// are not included anywhere, so it is okay to use such shorthands to make the
// example easier to read.
using Chunk = eglt::Chunk;
using AsyncNode = eglt::AsyncNode;

// A custom data type: a user with a name and an email.
struct User {
  std::string name;
  std::string email;
};

absl::Status EgltAssignInto(const User& user, Chunk* chunk) {
  chunk->metadata = eglt::ChunkMetadata{
      .mimetype = "application/x-eglt;User",
      .timestamp = absl::Now(),
  };
  chunk->data = absl::StrCat(user.name, ":::", user.email);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(const Chunk& chunk, User* user) {
  if (chunk.metadata.mimetype != "application/x-eglt;User") {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid mimetype: %v", chunk.metadata.mimetype));
  }

  // this validation is application-level logic
  const std::vector<std::string> parts = absl::StrSplit(chunk.data, ":::");
  if (parts.size() != 2) {
    return absl::InvalidArgumentError("Invalid data.");
  }

  user->name = parts[0];
  user->email = parts[1];

  return absl::OkStatus();
}

std::unique_ptr<eglt::redis::Redis> GetRedisOrDie() {
  absl::StatusOr<std::unique_ptr<eglt::redis::Redis>> redis_or =
      eglt::redis::Redis::Connect("localhost", 6379);
  if (!redis_or.ok()) {
    LOG(FATAL) << "Failed to connect to Redis: " << redis_or.status();
    ABSL_ASSUME(false);
  }
  CHECK_OK((*redis_or)->Hello());
  return *std::move(redis_or);
}

int main(int, char**) {
  absl::InstallFailureSignalHandler({});
  // create some users
  const std::vector users = {
      User{.name = "John Doe", .email = "johndoe@example.com"},
      User{.name = "Alice Smith", .email = "smith@example.com"},
      User{.name = "Bob Jones", .email = "jones@example.com"},
  };

  std::string store_id = absl::StrCat("users_", eglt::GenerateUUID4());

  // std::unique_ptr<eglt::redis::Redis> redis = GetRedisOrDie();
  //
  //
  // auto store = std::make_unique<eglt::redis::ChunkStore>(redis.get(), store_id,
  //                                                        absl::Seconds(300));

  auto store = eglt::MakeChunkStore<eglt::LocalChunkStore>(store_id);

  // create a node
  auto node_that_streams_users = AsyncNode(
      /*id=*/store_id, /*node_map=*/nullptr,
      /*chunk_store=*/std::move(store));
  node_that_streams_users.SetReaderOptions(
      /*ordered=*/true, /*remove_chunks=*/false);
  for (const auto& user : users) {
    if (const auto status = node_that_streams_users.Put(user); !status.ok()) {
      LOG(FATAL) << "Error: " << status;
      ABSL_ASSUME(false);
    }
  }
  node_that_streams_users
      .Put(
          User{
              .name = "Helena Pankov",
              .email = "helenapankov@google.com",
          },
          4)
      .IgnoreError();
  node_that_streams_users
      .Put(
          User{
              .name = "Paramjit Sandhu",
              .email = "params@google.com",
          },
          3)
      .IgnoreError();
  node_that_streams_users.Put(eglt::EndOfStream()).IgnoreError();

  eglt::SleepFor(absl::Seconds(1));

  int user_number = 0;
  while (true) {
    std::optional<User> user = node_that_streams_users.NextOrDie<User>();

    if (!node_that_streams_users.GetReaderStatus().ok()) {
      break;
    }

    if (!user.has_value()) {
      break;
    }

    // print the user to see that it is correctly serialized and deserialized.
    std::cout << "User " << user_number << ":\n"
              << "Name: " << user->name << "\n"
              << "Email: " << user->email << "\n"
              << std::endl;

    ++user_number;
  }

  return 0;
}
