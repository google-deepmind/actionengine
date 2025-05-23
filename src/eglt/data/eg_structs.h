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

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/data/conversion.h"  // IWYU pragma: keep
#include "eglt/data/mimetypes.h"

namespace eglt {

namespace base {
/// @private
inline std::vector<std::string> Indent(std::vector<std::string> fields,
                                       int indentation = 0,
                                       bool indent_first_line = false) {
  if (fields.empty()) {
    return fields;
  }

  std::vector<std::string> result = std::move(fields);
  const size_t start_index = indent_first_line ? 0 : 1;

  for (size_t index = start_index; index < result.size(); ++index) {
    result[index] = absl::StrCat(std::string(indentation, ' '), result[index]);
  }

  return result;
}

/// @private
inline std::string Indent(std::string field, int indentation = 0,
                          bool indent_first_line = false) {
  const std::vector<std::string> lines = Indent(
      absl::StrSplit(std::move(field), '\n'), indentation, indent_first_line);

  return absl::StrJoin(lines, "\n",
                       [](std::string* out, const std::string_view line) {
                         absl::StrAppend(out, line);
                       });
}

/// Evergreen chunk metadata.
///
/// This structure is used to store metadata about a chunk of data in the
/// Evergreen format. It includes fields for mimetype and timestamp.
/// @headerfile eglt/data/eg_structs.h
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

  [[nodiscard]] bool IsEmpty() const { return data.empty() && ref.empty(); }

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
 * @brief Evergreen action message.
 *
 * This structure represents an Evergreen action call, which can be sent on the
 * wire level (in a SessionMessage).
 */
struct ActionMessage {
  std::string id;
  std::string name;
  std::vector<Port> inputs;
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

struct SessionMessage {
  std::vector<NodeFragment> node_fragments;
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

inline absl::Status EgltAssignInto(Chunk chunk, std::string* string) {
  if (!MimetypeIsTextual(chunk.metadata.mimetype)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot move as std::string from a non-textual chunk: ",
                     chunk.metadata.mimetype));
  }
  *string = std::move(chunk.data);
  return absl::OkStatus();
}

inline absl::Status EgltAssignInto(std::string string, Chunk* chunk) {
  chunk->metadata = ChunkMetadata{
      .mimetype = kMimetypeTextPlain,
      .timestamp = absl::Now(),
  };
  chunk->data = std::move(string);
  return absl::OkStatus();
}

inline absl::Status EgltAssignInto(const Chunk& chunk, absl::Status* status) {
  if (chunk.metadata.mimetype != "__status__") {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid mimetype: ", chunk.metadata.mimetype));
  }
  if (chunk.data.empty()) {
    return absl::InvalidArgumentError(absl::StrCat("Empty data: ", chunk.data));
  }
  std::string message;
  int raw_code = static_cast<uint8_t>(chunk.data[0]);
  if (chunk.data.size() > 1) {
    message = chunk.data.substr(1);
  }

  *status = absl::Status(static_cast<absl::StatusCode>(raw_code), message);
  return absl::OkStatus();
}

inline absl::Status EgltAssignInto(const absl::Status& status, Chunk* chunk) {
  chunk->metadata = ChunkMetadata{
      .mimetype = "__status__",
      .timestamp = absl::Now(),
  };
  chunk->data =
      absl::StrCat(static_cast<uint8_t>(status.raw_code()), status.message());
  return absl::OkStatus();
}

}  // namespace base

using ChunkMetadata = base::ChunkMetadata;
using Chunk = base::Chunk;
using NodeFragment = base::NodeFragment;
using Port = base::Port;
using ActionMessage = base::ActionMessage;
using SessionMessage = base::SessionMessage;

template <typename T>
concept ConvertibleToChunk = requires(T t) {
  {eglt::ConvertTo<Chunk>(std::move(t))};
};

template <typename T>
concept ConvertibleFromChunk = requires(Chunk chunk) {
  {eglt::ConvertTo<T>(std::move(chunk))};
};

constexpr Chunk EndOfStream() {
  return Chunk{
      .metadata = ChunkMetadata{.mimetype = kMimetypeBytes},
      .data = "",
  };
}

}  // namespace eglt

#endif  // EGLT_DATA_EG_STRUCTS_H_
