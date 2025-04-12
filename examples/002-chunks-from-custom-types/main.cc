// ------------------------------------------------------------------------------
// This example shows how to define custom types that can be serialized to
// Evergreen chunks, completely at your own discretion and at application level.
//
// You can run this example with:
// blaze run //third_party/eglt/examples:chunks_from_custom_types
// ------------------------------------------------------------------------------

#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <eglt/absl_headers.h>
#include <eglt/data/eg_structs.h>
#include <eglt/nodes/async_node.h>

// Simply some type aliases to make the code more readable. These declarations
// are not included anywhere, so it is okay to use such shorthands to make the
// example easier to read.
using Chunk = eglt::base::Chunk;
using AsyncNode = eglt::AsyncNode;

// A custom data type: a user with a name and an email.
struct User {
  std::string name;
  std::string email;
};

absl::Status EgltAssignInto(const User& user, Chunk* chunk) {
  chunk->metadata = eglt::base::ChunkMetadata{
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

int main(int argc, char** argv) {
  // create some users
  std::vector<User> users = {
      User{.name = "John Doe", .email = "johndoe@example.com"},
      User{.name = "Alice Smith", .email = "smith@example.com"},
      User{.name = "Bob Jones", .email = "jones@example.com"},
  };

  // LOG(INFO) << eglt::Converters<Chunk>::From(users[0]);

  // create a node
  auto node_that_streams_users = AsyncNode(/*id=*/"users");
  for (const auto& user : users) {
    if (const auto status = node_that_streams_users.Put(user); !status.ok()) {
      LOG(FATAL) << "Error: " << status;
    }
  }
  node_that_streams_users.Put(eglt::EndOfStream()).IgnoreError();

  int user_number = 0;
  std::optional<User> user;
  while (true) {
    user = node_that_streams_users.Next<User>();

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
