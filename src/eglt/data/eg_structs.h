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

#ifndef EGLT_DATA_EG_STRUCTS_H_
#define EGLT_DATA_EG_STRUCTS_H_

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/base/optimization.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/time.h>

#include "eglt/data/conversion.h"
#include "eglt/data/mimetypes.h"

namespace eglt {

namespace base {
std::vector<std::string> Indent(std::vector<std::string> fields,
                                int num_spaces = 0,
                                bool indent_first_line = false);

std::string Indent(std::string field, int num_spaces = 0,
                   bool indent_first_line = false);

/**
 *  ActionEngine chunk metadata.
 *
 *  This structure is used to store metadata about a chunk of data in the
 *  ActionEngine format. It includes fields for mimetype and timestamp.
 *
 *  @headerfile eglt/data/eg_structs.h
 */
struct ChunkMetadata {
  std::string mimetype =
      kMimetypeBytes;    /// The mimetype of the data in the chunk.
  absl::Time timestamp;  /// The timestamp associated with the chunk.

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

  friend bool operator==(const ChunkMetadata& lhs, const ChunkMetadata& rhs) {
    return lhs.mimetype == rhs.mimetype && lhs.timestamp == rhs.timestamp;
  }
};

/**
 *  ActionEngine chunk.
 *
 *  This structure is used to store a chunk of data in the ActionEngine format.
 *  It includes fields for metadata, a reference to the data, and the actual
 *  data itself. Data can be either a reference or the actual data, but not both.
 *
 *  @headerfile eglt/data/eg_structs.h
 */
struct Chunk {
  /** The metadata associated with the chunk.
   *
   * This includes the mimetype and timestamp of the chunk.
   */
  ChunkMetadata metadata;

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
  [[nodiscard]] bool IsNull() const {
    return metadata.mimetype == kMimetypeBytes && IsEmpty();
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Chunk& chunk) {
    if (!chunk.metadata.Empty()) {
      absl::Format(&sink, "metadata: \n%s",
                   Indent(absl::StrCat(chunk.metadata), 2, true));
    }
    if (!chunk.ref.empty() && !chunk.data.empty()) {
      LOG(FATAL) << "Chunk has both ref and data set.";
      ABSL_ASSUME(false);
    }
    if (!chunk.ref.empty()) {
      absl::Format(&sink, "ref: %s\n", chunk.ref);
    }
    if (!chunk.data.empty()) {
      absl::Format(&sink, "data: %s\n", chunk.data);
    }
  }

  friend bool operator==(const Chunk& lhs, const Chunk& rhs) {
    return lhs.metadata == rhs.metadata && lhs.ref == rhs.ref &&
           lhs.data == rhs.data;
  }
};

/** ActionEngine node fragment.
 *
 * This structure represents a fragment of a node in the ActionEngine format:
 * a data chunk, the node ID, the sequence number of the chunk, and a flag
 * indicating whether more fragments are expected.
 *
 * @headerfile eglt/data/eg_structs.h
 */
struct NodeFragment {
  /** The node ID for this fragment. */
  std::string id;

  /** The chunk of data associated with the node fragment. May be empty. */
  std::optional<Chunk> chunk = std::nullopt;

  /** The chunk's order in the sequence. -1 means "undefined" or "not set". */
  int32_t seq = -1;

  /** A flag indicating whether more fragments are expected. */
  bool continued = false;

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

  friend bool operator==(const NodeFragment& lhs, const NodeFragment& rhs) {
    return lhs.id == rhs.id && lhs.chunk == rhs.chunk && lhs.seq == rhs.seq &&
           lhs.continued == rhs.continued;
  }
};

/// A mapping of a parameter name to its node ID in an action.
/// @headerfile eglt/data/eg_structs.h
struct Port {
  std::string name;
  std::string id;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Port& parameter) {
    if (!parameter.name.empty()) {
      sink.Append(absl::StrCat("name: ", parameter.name, "\n"));
    }
    if (!parameter.id.empty()) {
      sink.Append(absl::StrCat("id: ", parameter.id, "\n"));
    }
  }

  friend bool operator==(const Port& lhs, const Port& rhs) {
    return lhs.name == rhs.name && lhs.id == rhs.id;
  }
};

/**
 * An action message containing the information necessary to call an action.
 *
 * This structure represents an ActionEngine action call, which can be sent
 * on the wire level (in a SessionMessage).
 */
struct ActionMessage {
  /** The identifier of the action instance. */
  std::string id;

  /**
   * The name of the action to call.
   *
   * This name is used to look up the action in the ActionRegistry.
   */
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
 * @headerfile eglt/data/eg_structs.h
 */
struct SessionMessage {
  /** A list of node fragments, each representing a piece of data for some node. */
  std::vector<NodeFragment> node_fragments;

  /**
   * A list of action messages, each representing an action to be called by
   * the receiving node or some end destination in case of a router/proxy.
   */
  std::vector<ActionMessage> actions;

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

  friend bool operator==(const SessionMessage& lhs, const SessionMessage& rhs) {
    return lhs.node_fragments == rhs.node_fragments &&
           lhs.actions == rhs.actions;
  }
};

absl::Status EgltAssignInto(Chunk chunk, std::string* string);
absl::Status EgltAssignInto(std::string string, Chunk* chunk);

absl::Status EgltAssignInto(const Chunk& chunk, absl::Status* status);
absl::Status EgltAssignInto(const absl::Status& status, Chunk* chunk);

}  // namespace base

using ChunkMetadata = base::ChunkMetadata;

using Chunk = base::Chunk;
using NodeFragment = base::NodeFragment;
using Port = base::Port;
using ActionMessage = base::ActionMessage;
using SessionMessage = base::SessionMessage;

template <typename T>
concept ConvertibleToChunk = requires(T t) {
  {
    EgltAssignInto(std::move(t), std::declval<Chunk*>())
  } -> std::same_as<absl::Status>;
};

template <typename T>
concept ConvertibleFromChunk = requires(Chunk chunk) {
  {
    EgltAssignInto(std::move(chunk), std::declval<T*>())
  } -> std::same_as<absl::Status>;
};

constexpr Chunk EndOfStream() {
  return Chunk{
      .metadata = ChunkMetadata{.mimetype = kMimetypeBytes},
      .data = "",
  };
}

}  // namespace eglt

#endif  // EGLT_DATA_EG_STRUCTS_H_
