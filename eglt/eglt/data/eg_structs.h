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

/**
 * @file
 * @brief
 *   Evergreen data structures used to implement actions and nodes (data
 *   streams).
 */

#ifndef EGLT_DATA_EG_STRUCTS_H_
#define EGLT_DATA_EG_STRUCTS_H_

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/data/mimetypes.h"

namespace eglt {
namespace base {

/// @private
std::vector<std::string> Indent(std::vector<std::string> fields,
                                int indentation = 0,
                                bool indent_first_line = false);

/// @private
std::string Indent(std::string field, int indentation = 0,
                   bool indent_first_line = false);

/**
 * @brief
 *   Constructs an object of type S from a value of type T.
 *
 * Library users may want to implement this function to enable library's
 * IO syntax to be used with their own types.
 *
* ```cc
 * // A custom data type: a user with a name and an email.
 * struct User {
 *   std::string name;
 *   std::string email;
 * };
 * ```
 *
 * <details>
 *
 * <summary>Example: Reading `User` directly from AsyncNode</summary>
 *
 * ```cc
 * // Define how to serialize a User to a Chunk.
 * template <>
 * Chunk eglt::base::ConstructFrom(User value) {
 *   Chunk chunk;
 *   // we need to assign a mimetype to the chunk that is unique to this type.
 *   // This is used by the Evergreen protocol to determine how to deserialize the
 *   // chunk.
 *   chunk.metadata.mimetype = "application/x-eglt;User";
 *   chunk.data = absl::StrCat(value.name, ":::", value.email);
 *   return chunk;
 * }
 *
 * void SomeApplicationCode() {
 *   // create some users
 *   std::vector<User> users = {
 *       User{.name = "John Doe", .email = "johndoe@example.com"},
 *       User{.name = "Alice Smith", .email = "smith@example.com"},
 *       User{.name = "Bob Jones", .email = "jones@example.com"},
 *   };
 *
 *   // create a node
 *   auto node_that_streams_users = AsyncNode(/"users");
 *   for (const auto& user : users) {
 *     if (const auto status = node_that_streams_users.Put(user); !status.ok()) {
 *       LOG(FATAL) << "Error: " << status;
 *     }
 *   }
 *   node_that_streams_users.Put(eglt::EndOfStream()).IgnoreError();
 * ```
 * </details>
 *
 * \tparam S The type of the object to construct.
 * \tparam T The type of the value to convert.
 * @param value The value to convert.
 * @return
 *   An object of type S constructed from the value.
 */
template <typename S, typename T>
S ConstructFrom(T value);

/**
 * @brief
 *   Constructs an object of type T from a value of type S,
 *   returning an error if the conversion fails.
 *
 * Library users may want to implement this function to enable library's
 * IO syntax to be used with their own types.
 *
 * ```cc
 * // A custom data type: a user with a name and an email.
 * struct User {
 *   std::string name;
 *   std::string email;
 * };
 * ```
 *
 * <details>
 *
 * <summary>Example: Reading `User` directly from AsyncNode</summary>
 * ```cc
 * // Define how to deserialize a User from a Chunk.
 * template <>
 * absl::StatusOr<User> eglt::base::MoveAsAndReturnStatus(Chunk value) {
 *   // this validation is application-level logic, but it is considered best
 *   // practice to validate the mimetype of the chunk to ensure that it is
 *   // compatible with the type we are trying to deserialize it to.
 *   if (value.metadata.mimetype != "application/x-eglt;User") {
 *     return absl::InvalidArgumentError("Invalid mimetype.");
 *   }
 *
 *   // this validation is application-level logic
 *   std::vector<std::string> parts = absl::StrSplit(value.data, ":::");
 *   if (parts.size() != 2) {
 *     return absl::InvalidArgumentError("Invalid data.");
 *   }
 *
 *   return User{.name = parts[0], .email = parts[1]};
 * }
 *
 * void SomeApplicationCode() {
 *   AsyncNode node_that_streams_users("users");
 *
 *   std::optional<User> user;
 *   while (true) {
 *     user = node_that_streams_users.Next<User>();
 *
 *     if (!node_that_streams_users.GetReaderStatus().ok()) {
 *       break;
 *     }
 *
 *     if (!user.has_value()) {
 *       break;
 *     }
 *   }
 * ```
 * </details>
 *
 * @tparam T The type of the object to construct.
 * @tparam S The type of the value to convert.
 * @param value The value to convert.
 * @return
 *  An object of type T constructed from the value, or an error if the
 *  conversion fails.
 */
template <typename T, typename S>
absl::StatusOr<T> MoveAsAndReturnStatus(S value);

/// @private
template <typename T, typename S>
T MoveAs(S value) {
  absl::StatusOr<T> result = MoveAsAndReturnStatus<T, S>(std::move(value));
  if (!result.ok()) {
    LOG(FATAL) << "Failed to move as " << typeid(T).name() << ": "
               << result.status();
  }
  return *std::move(result);
}

/// Evergreen chunk metadata.
///
/// This structure is used to store metadata about a chunk of data in the
/// Evergreen format. It includes fields for mimetype and timestamp.
/// @headerfile eglt/data/eg_structs.h
struct ChunkMetadata {
  std::string mimetype; /// The mimetype of the data in the chunk.
  absl::Time timestamp; /// The timestamp associated with the chunk.

  /// @private
  template <typename T>
  static ChunkMetadata From(T&& value) {
    return ConstructFrom<ChunkMetadata>(std::forward<T>(value));
  }

  /// Checks if the metadata is empty.
  /// @return
  ///   true if both fields are empty, false otherwise.
  [[nodiscard]] bool Empty() const {
    return mimetype.empty() && timestamp == absl::Time();
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ChunkMetadata& metadata) {
    if (!metadata.mimetype.empty()) {
      absl::Format(&sink, "mimetype: %s\n", metadata.mimetype);
    }
    if (metadata.timestamp != absl::Time()) {
      absl::Format(&sink, "timestamp: %s\n",
                   absl::FormatTime(metadata.timestamp));
    }
  }
};

/// Evergreen chunk.
///
/// This structure is used to store a chunk of data in the Evergreen format.
/// It includes fields for metadata, a reference to the data, and the actual
/// data itself. Data can be either a reference or the actual data, but not both.
/// However, this is not enforced in the structure itself at this time.
/// @headerfile eglt/data/eg_structs.h
struct Chunk {
  ChunkMetadata metadata;

  std::string ref;
  std::string data;

  /// @private
  template <typename T>
  static Chunk From(T&& value) {
    return ConstructFrom<Chunk>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Chunk& chunk) {
    if (!chunk.metadata.Empty()) {
      absl::Format(&sink, "metadata: \n%s",
                   Indent(absl::StrCat(chunk.metadata), 2, true));
    }
    if (!chunk.ref.empty() && !chunk.data.empty()) {
      LOG(FATAL) << "Chunk has both ref and data set.";
    }
    if (!chunk.ref.empty()) {
      absl::Format(&sink, "ref: %s\n", chunk.ref);
    }
    if (!chunk.data.empty()) {
      absl::Format(&sink, "data: %s\n", chunk.data);
    }
  }
};

/// Evergreen node fragment.
/// @headerfile eglt/data/eg_structs.h
struct NodeFragment {
  /// The node ID for this fragment.
  std::string id;
  /// The chunk of data associated with the node fragment. May be empty.
  std::optional<Chunk> chunk = std::nullopt;
  /// The chunk's order in the sequence.
  int32_t seq = -1;
  /// Whether more node fragments are expected.
  bool continued = false;

  /// @private
  template <typename T>
  static NodeFragment From(T&& value) {
    return ConstructFrom<NodeFragment>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const NodeFragment& fragment) {
    if (!fragment.id.empty()) {
      absl::Format(&sink, "id: %s\n", fragment.id);
    }
    if (fragment.chunk.has_value()) {
      absl::Format(&sink, "chunk: \n%s",
                   Indent(absl::StrCat(*fragment.chunk), 2, true));
    }
    if (fragment.seq != -1) {
      absl::Format(&sink, "seq: %d\n", fragment.seq);
    }
    if (!fragment.continued) {
      sink.Append("continued: false\n");
    }
  }
};

/// A mapping of a parameter name to its node ID in an action.
/// @headerfile eglt/data/eg_structs.h
struct NamedParameter {
  std::string name;
  std::string id;

  /// @private
  template <typename T>
  static NamedParameter From(T&& value) {
    return ConstructFrom<NamedParameter>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const NamedParameter& parameter) {
    if (!parameter.name.empty()) {
      sink.Append(absl::StrCat("name: ", parameter.name, "\n"));
    }
    if (!parameter.id.empty()) {
      sink.Append(absl::StrCat("id: ", parameter.id, "\n"));
    }
  }
};

/**
 * @brief Evergreen action message.
 *
 * This structure represents an Evergreen action call, which can be sent on the
 * wire level (in a SessionMessage).
 */
struct ActionMessage {
  std::string id;
  std::string name;
  std::vector<NamedParameter> inputs;
  std::vector<NamedParameter> outputs;

  /// @private
  template <typename T>
  static ActionMessage From(T&& value) {
    return ConstructFrom<ActionMessage>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionMessage& action) {
    if (!action.name.empty()) {
      absl::Format(&sink, "name: %s\n", action.name);
    }
    if (!action.inputs.empty()) {
      sink.Append(absl::StrCat("inputs:\n"));
      for (const auto& input : action.inputs) {
        absl::Format(&sink, "%s\n", Indent(absl::StrCat(input), 2, true));
      }
    }
    if (!action.outputs.empty()) {
      sink.Append(absl::StrCat("outputs:\n"));
      for (const auto& output : action.outputs) {
        absl::Format(&sink, "%s\n", Indent(absl::StrCat(output), 2, true));
      }
    }
  }
};

struct SessionMessage {
  std::vector<NodeFragment> node_fragments;
  std::vector<ActionMessage> actions;

  /// @private
  template <typename T>
  static SessionMessage From(T&& value) {
    return ConstructFrom<SessionMessage>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const SessionMessage& message) {
    if (!message.node_fragments.empty()) {
      sink.Append("node_fragments: \n");
      for (const auto& node_fragment : message.node_fragments) {
        absl::Format(&sink, "%s\n",
                     Indent(absl::StrCat(node_fragment), 2, true));
      }
    }
    if (!message.actions.empty()) {
      sink.Append(absl::StrCat("actions: \n"));
      for (const auto& action : message.actions) {
        absl::Format(&sink, "%s\n", Indent(absl::StrCat(action), 2, true));
      }
    }
  }
};

/// @private
bool IsNullChunk(const Chunk& chunk);

} // namespace base

constexpr base::Chunk MakeNullChunk() {
  return base::Chunk{
      .metadata =
      base::ChunkMetadata{
          .mimetype = kMimetypeBytes,
      },
      .data = "",
  };
}

constexpr base::Chunk EndOfStream() {
  return MakeNullChunk();
}

} // namespace eglt

#endif  // EGLT_DATA_EG_STRUCTS_H_
