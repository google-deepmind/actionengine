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
 *   ActionEngine data structures used to implement actions and nodes (data
 *   streams).
 */

#ifndef ACTIONENGINE_DATA_TYPES_H_
#define ACTIONENGINE_DATA_TYPES_H_

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/base/optimization.h>
#include <absl/container/flat_hash_map.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/time.h>

#include "actionengine/data/conversion.h"
#include "actionengine/data/mimetypes.h"

namespace act {

namespace internal {
std::vector<std::string> Indent(std::vector<std::string> fields,
                                int num_spaces = 0,
                                bool indent_first_line = false);

std::string Indent(std::string field, int num_spaces = 0,
                   bool indent_first_line = false);
}  // namespace internal

/**
 *  ActionEngine chunk metadata.
 *
 *  This structure is used to store metadata about a chunk of data in the
 *  ActionEngine format. It includes fields for mimetype and timestamp.
 *
 *  @headerfile actionengine/data/types.h
 */
struct ChunkMetadata {
  /** The mimetype of the data in the chunk. */
  std::string mimetype = kMimetypeBytes;

  /** The timestamp associated with the chunk. */
  std::optional<absl::Time> timestamp;

  /** Additional free-form attributes associated with the chunk.
   *
   * This is a mapping of attribute names to their values, allowing for
   * extensibility and the inclusion of custom metadata as needed.
   */
  absl::flat_hash_map<std::string, std::string> attributes;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ChunkMetadata& metadata);

  friend bool operator==(const ChunkMetadata& lhs, const ChunkMetadata& rhs) {
    if (lhs.mimetype != rhs.mimetype || lhs.timestamp != rhs.timestamp) {
      return false;
    }
    bool attributes_equal = lhs.attributes.size() == rhs.attributes.size();
    for (const auto& [key, value] : lhs.attributes) {
      if (!attributes_equal)
        break;
      auto it = rhs.attributes.find(key);
      attributes_equal &= it != rhs.attributes.end() && it->second == value;
    }
    return attributes_equal;
  }
};

/**
 *  ActionEngine chunk.
 *
 *  This structure is used to store a chunk of data in the ActionEngine format.
 *  It includes fields for metadata, a reference to the data, and the actual
 *  data itself. Data can be either a reference or the actual data, but not both.
 *
 *  @headerfile actionengine/data/types.h
 */
struct Chunk {
  /** The metadata associated with the chunk.
   *
   * This includes the mimetype and timestamp of the chunk.
   */
  std::optional<ChunkMetadata> metadata;

  /** A reference to the data in the chunk.
   *
   * This is a string that can be used to reference the data in an external
   * storage system, such as a database, object store, or file system.
   * It is used when the data is too large to be sent directly in the chunk.
   * If this field is set, the `data` field should be empty.
   */
  std::string ref;

  /** The inline data in the chunk.
   *
   * This is a (raw byte) string that contains the actual data of the chunk.
   * It is used when the data is small enough to be sent directly in the chunk.
   * If this field is set, the `ref` field should be empty.
   */
  std::string data;

  [[nodiscard]] std::string GetMimetype() const {
    return metadata ? metadata->mimetype : kEmptyMimetype;
  }

  [[nodiscard]] bool IsEmpty() const { return data.empty() && ref.empty(); }

  /** Checks if the chunk is null.
   *
   * A chunk is considered null if it has no data and its metadata mimetype is
   * set explicitly to kMimetypeBytes (indicating that it contains no
   * meaningful data, disambiguating it from a chunk that has no data but
   * may have logical meaning for the application in the context of a
   * particular mimetype).
   *
   * @return
   *   true if the chunk is empty, false otherwise.
   */
  [[nodiscard]] bool IsNull() const;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Chunk& chunk);

  friend bool operator==(const Chunk& lhs, const Chunk& rhs) {
    return lhs.metadata == rhs.metadata && lhs.ref == rhs.ref &&
           lhs.data == rhs.data;
  }
};

struct NodeRef {
  /** The node ID for this reference. */
  std::string id;

  /** The start offset of the span, if any. */
  uint32_t offset = 0;

  /** The length of the span, if any.
   *
   * This is the number of bytes in the span, starting from the offset.
   * If this field is not set, it means the span extends to the end of the node.
   */
  std::optional<uint32_t> length;

  friend bool operator==(const NodeRef& lhs, const NodeRef& rhs) {
    return lhs.id == rhs.id && lhs.offset == rhs.offset &&
           lhs.length == rhs.length;
  }
};

/** ActionEngine node fragment.
 *
 * This structure represents a fragment of a node in the ActionEngine format:
 * a data chunk, the node ID, the sequence number of the chunk, and a flag
 * indicating whether more fragments are expected.
 *
 * @headerfile actionengine/data/types.h
 */
struct NodeFragment {
  /** The node ID for this fragment. */
  std::string id;

  /** The data associated with the node fragment. */
  std::variant<Chunk, NodeRef> data = Chunk{};

  /** The chunk's order in the sequence. -1 means "undefined" or "not set". */
  std::optional<int32_t> seq;

  /** A flag indicating whether more fragments are expected. */
  bool continued = false;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const NodeFragment& fragment);

  friend bool operator==(const NodeFragment& lhs, const NodeFragment& rhs) {
    return lhs.id == rhs.id && lhs.data == rhs.data && lhs.seq == rhs.seq &&
           lhs.continued == rhs.continued;
  }

  absl::StatusOr<std::reference_wrapper<Chunk>> GetChunk();
  Chunk& GetChunkOrDie();

  absl::StatusOr<std::reference_wrapper<NodeRef>> GetNodeRef();
  NodeRef& GetNodeRefOrDie();
};

/// A mapping of a parameter name to its node ID in an action.
/// @headerfile actionengine/data/types.h
struct Port {
  std::string name;
  std::string id;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Port& parameter);

  friend bool operator==(const Port& lhs, const Port& rhs) {
    return lhs.name == rhs.name && lhs.id == rhs.id;
  }
};

/**
 * An action message containing the information necessary to call an action.
 *
 * This structure represents an ActionEngine action call, which can be sent
 * on the wire level (in a WireMessage).
 */
struct ActionMessage {
  /** The identifier of the action instance. */
  std::string id;

  /** The name of the action to call (look up in the ActionRegistry) */
  std::string name;

  /** The input ports for the action.
   *
   * These are the parameters that the action expects to receive. Each port
   * is represented by a Port structure, which contains the name and ID of the
   * port.
   */
  std::vector<Port> inputs;

  /** The output ports for the action.
   *
   * These are the parameters that the action will produce as output. Each port
   * is represented by a Port structure, which contains the name and ID of the
   * port.
   */
  std::vector<Port> outputs;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionMessage& action);

  friend bool operator==(const ActionMessage& lhs, const ActionMessage& rhs) {
    return lhs.id == rhs.id && lhs.name == rhs.name &&
           lhs.inputs == rhs.inputs && lhs.outputs == rhs.outputs;
  }
};

/**
 * A message type containing node fragments and action calls.
 *
 * This structure represents a message that can be sent over a stream in the
 * ActionEngine format. It contains a list of node fragments and a list of
 * action messages. This is the singular unit of communication in ActionEngine,
 * and it is used to send data and actions between nodes in the system.
 *
 * @headerfile actionengine/data/types.h
 */
struct WireMessage {
  /** A list of node fragments, each representing a piece of data for some node. */
  std::vector<NodeFragment> node_fragments;

  /**
   * A list of action messages, each representing an action to be called by
   * the receiving node or some end destination in case of a router/proxy.
   */
  std::vector<ActionMessage> actions;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const WireMessage& message);

  friend bool operator==(const WireMessage& lhs, const WireMessage& rhs) {
    return lhs.node_fragments == rhs.node_fragments &&
           lhs.actions == rhs.actions;
  }
};

template <typename T>
absl::Status EgltAssignInto(T src, T* absl_nonnull dst) {
  static_assert(std::is_move_constructible_v<T>);
  static_assert(std::is_move_assignable_v<T>);
  *dst = std::move(src);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(Chunk chunk, std::string* absl_nonnull string);
absl::Status EgltAssignInto(std::string string, Chunk* absl_nonnull chunk);

absl::Status EgltAssignInto(const Chunk& chunk,
                            absl::Status* absl_nonnull status);
absl::Status EgltAssignInto(const absl::Status& status,
                            Chunk* absl_nonnull chunk);

template <typename Sink>
void AbslStringify(Sink& sink, const ChunkMetadata& metadata) {
  if (!metadata.mimetype.empty()) {
    absl::Format(&sink, "mimetype: %s\n", metadata.mimetype);
  }
  if (metadata.timestamp) {
    absl::Format(&sink, "timestamp: %s\n",
                 absl::FormatTime(*metadata.timestamp));
  }
  if (!metadata.attributes.empty()) {
    sink.Append("attributes:\n");
    for (const auto& [key, value] : metadata.attributes) {
      sink.Append(internal::Indent(absl::StrCat(key, ": ", value), 2, true));
    }
  }
}

template <typename Sink>
void AbslStringify(Sink& sink, const Chunk& chunk) {
  if (chunk.metadata) {
    absl::Format(&sink, "metadata: \n%s",
                 internal::Indent(absl::StrCat(*chunk.metadata), 2, true));
  }
  if (!chunk.data.empty() || (chunk.data.empty() && chunk.ref.empty())) {
    absl::Format(&sink, "data: %s\n", chunk.data);
  }
  if (!chunk.ref.empty()) {
    absl::Format(&sink, "ref: %s\n", chunk.ref);
  }
}

template <typename Sink>
void AbslStringify(Sink& sink, const NodeFragment& fragment) {
  if (!fragment.id.empty()) {
    absl::Format(&sink, "id: %s\n", fragment.id);
  }
  if (std::holds_alternative<Chunk>(fragment.data)) {
    absl::Format(&sink, "data: \n%s",
                 internal::Indent(absl::StrCat(std::get<Chunk>(fragment.data)),
                                  2, true));
  } else if (std::holds_alternative<NodeRef>(fragment.data)) {
    const auto& node_ref = std::get<NodeRef>(fragment.data);
    absl::Format(&sink, "node_ref: id: %s, offset: %d", node_ref.id,
                 node_ref.offset);
    if (node_ref.length.has_value()) {
      absl::Format(&sink, ", length: %d", *node_ref.length);
    }
    sink.Append("\n");
  }
  if (fragment.seq) {
    absl::Format(&sink, "seq: %d\n", *fragment.seq);
  }
  if (!fragment.continued) {
    sink.Append("continued: false\n");
  }
}

template <typename Sink>
void AbslStringify(Sink& sink, const Port& parameter) {
  if (!parameter.name.empty()) {
    sink.Append(absl::StrCat("name: ", parameter.name, "\n"));
  }
  if (!parameter.id.empty()) {
    sink.Append(absl::StrCat("id: ", parameter.id, "\n"));
  }
}

template <typename Sink>
void AbslStringify(Sink& sink, const ActionMessage& action) {
  if (!action.name.empty()) {
    absl::Format(&sink, "name: %s\n", action.name);
  }
  if (!action.inputs.empty()) {
    sink.Append(absl::StrCat("inputs:\n"));
    for (const auto& input : action.inputs) {
      absl::Format(&sink, "%s\n",
                   internal::Indent(absl::StrCat(input), 2, true));
    }
  }
  if (!action.outputs.empty()) {
    sink.Append(absl::StrCat("outputs:\n"));
    for (const auto& output : action.outputs) {
      absl::Format(&sink, "%s\n",
                   internal::Indent(absl::StrCat(output), 2, true));
    }
  }
}

template <typename Sink>
void AbslStringify(Sink& sink, const WireMessage& message) {
  if (!message.node_fragments.empty()) {
    sink.Append("node_fragments: \n");
    for (const auto& node_fragment : message.node_fragments) {
      absl::Format(&sink, "%s\n",
                   internal::Indent(absl::StrCat(node_fragment), 2, true));
    }
  }
  if (!message.actions.empty()) {
    sink.Append(absl::StrCat("actions: \n"));
    for (const auto& action : message.actions) {
      absl::Format(&sink, "%s\n",
                   internal::Indent(absl::StrCat(action), 2, true));
    }
  }
}

template <typename T>
concept ConvertibleToChunk = requires(T t) {
  {EgltAssignInto(std::move(t), std::declval<Chunk*>())}
      ->std::same_as<absl::Status>;
};

template <typename T>
concept ConvertibleFromChunk = requires(Chunk chunk) {
  {EgltAssignInto(std::move(chunk), std::declval<T*>())}
      ->std::same_as<absl::Status>;
};

inline Chunk EndOfStream() {
  return Chunk{
      .metadata = ChunkMetadata{.mimetype = kMimetypeBytes},
      .data = "",
  };
}

}  // namespace act

#endif  // ACTIONENGINE_DATA_TYPES_H_
