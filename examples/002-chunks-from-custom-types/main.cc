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

// Define how to serialize a User to a Chunk.
template <>
Chunk eglt::base::ConstructFrom(User value) {
  Chunk chunk;
  // we need to assign a mimetype to the chunk that is unique to this type.
  // This is used by the Evergreen protocol to determine how to deserialize the
  // chunk.
  chunk.metadata.mimetype = "application/x-eglt;User";
  chunk.data = absl::StrCat(value.name, ":::", value.email);
  return chunk;
}

// Define how to deserialize a User from a Chunk.
template <>
absl::StatusOr<User> eglt::base::MoveAsAndReturnStatus(Chunk value) {
  // this validation is application-level logic, but it is considered best
  // practice to validate the mimetype of the chunk to ensure that it is
  // compatible with the type we are trying to deserialize it to.
  if (value.metadata.mimetype != "application/x-eglt;User") {
    return absl::InvalidArgumentError("Invalid mimetype.");
  }

  // this validation is application-level logic
  std::vector<std::string> parts = absl::StrSplit(value.data, ":::");
  if (parts.size() != 2) {
    return absl::InvalidArgumentError("Invalid data.");
  }

  return User{.name = parts[0], .email = parts[1]};
}

int main(int argc, char** argv) {
  // create some users
  std::vector<User> users = {
      User{.name = "John Doe", .email = "johndoe@example.com"},
      User{.name = "Alice Smith", .email = "smith@example.com"},
      User{.name = "Bob Jones", .email = "jones@example.com"},
  };

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
