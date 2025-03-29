#ifndef EGLT_DATA_EG_STRUCTS_H_
#define EGLT_DATA_EG_STRUCTS_H_

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <eglt/absl_headers.h>

namespace eglt {
namespace base {

std::vector<std::string> Indent(std::vector<std::string> fields,
                                int indentation = 0,
                                bool indent_first_line = false);

std::string Indent(std::string field, int indentation = 0,
                   bool indent_first_line = false);

template <typename S, typename T>
S ConstructFrom(T value);

template <typename T, typename S>
absl::StatusOr<T> MoveAsAndReturnStatus(S value);

template <typename T, typename S>
T MoveAs(S value) {
  absl::StatusOr<T> result = MoveAsAndReturnStatus<T, S>(std::move(value));
  if (!result.ok()) {
    LOG(FATAL) << "Failed to move as " << typeid(T).name() << ": "
               << result.status();
  }
  return *std::move(result);
}

// Evergreen v2 ChunkMetadata.
struct ChunkMetadata {
  std::string mimetype;
  std::string role;
  std::string channel;
  std::string environment;
  std::string original_file_name;
  // google::protobuf::Timestamp capture_time;
  // std::vector<google::protobuf::Any> experimental;

  template <typename T>
  static ChunkMetadata From(T&& value) {
    return ConstructFrom<ChunkMetadata>(std::forward<T>(value));
  }

  [[nodiscard]] bool Empty() const {
    return mimetype.empty() && role.empty() && channel.empty() &&
      environment.empty() && original_file_name.empty();
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ChunkMetadata& metadata) {
    if (!metadata.mimetype.empty()) {
      absl::Format(&sink, "mimetype: %s\n", metadata.mimetype);
    }
    if (!metadata.role.empty()) {
      absl::Format(&sink, "role: %s\n", metadata.role);
    }
    if (!metadata.channel.empty()) {
      absl::Format(&sink, "channel: %s\n", metadata.channel);
    }
    if (!metadata.environment.empty()) {
      absl::Format(&sink, "environment: %s\n", metadata.environment);
    }
    if (!metadata.original_file_name.empty()) {
      absl::Format(&sink, "original_file_name: %s\n",
                   metadata.original_file_name);
    }
  }
};

struct Chunk {
  ChunkMetadata metadata;

  std::string ref;
  std::string data;

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
    if (!chunk.ref.empty()) { absl::Format(&sink, "ref: %s\n", chunk.ref); }
    if (!chunk.data.empty()) { absl::Format(&sink, "data: %s\n", chunk.data); }
  }
};

struct NodeFragment {
  std::string id;
  std::optional<Chunk> chunk = std::nullopt;
  int32_t seq = -1;
  bool continued = false;
  std::vector<std::string> child_ids;

  template <typename T>
  static NodeFragment From(T&& value) {
    return ConstructFrom<NodeFragment>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const NodeFragment& fragment) {
    if (!fragment.id.empty()) { absl::Format(&sink, "id: %s\n", fragment.id); }
    if (fragment.chunk.has_value()) {
      absl::Format(&sink, "chunk: \n%s",
                   Indent(absl::StrCat(*fragment.chunk), 2, true));
    }
    if (fragment.seq != -1) { absl::Format(&sink, "seq: %d\n", fragment.seq); }
    if (!fragment.continued) { sink.Append("continued: false\n"); }
    if (!fragment.child_ids.empty()) {
      absl::Format(&sink, "child_ids: %s\n",
                   absl::StrJoin(fragment.child_ids, ", "));
    }
  }
};

struct NamedParameter {
  std::string name;
  std::string id;

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

struct ActionMessage {
  std::string name;
  std::vector<NamedParameter> inputs;
  std::vector<NamedParameter> outputs;

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

  template <typename T>
  static SessionMessage From(T&& value) {
    return ConstructFrom<SessionMessage>(std::forward<T>(value));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const SessionMessage& message) {
    if (!message.node_fragments.empty()) {
      sink.Append("node_fragments: \n");
      for (const auto& node_fragment : message.node_fragments) {
        absl::Format(
          &sink, "%s\n",
          Indent(absl::StrCat(node_fragment), 2, true));
      }
    }
    if (!message.actions.empty()) {
      sink.Append(absl::StrCat("actions: \n"));
      for (const auto& action : message.actions) {
        absl::Format(&sink, "%s\n",
                     Indent(absl::StrCat(action), 2, true));
      }
    }
  }
};

bool IsNullChunk(const Chunk& chunk);

} // namespace base

constexpr base::Chunk MakeNullChunk() {
  return base::Chunk{
    .metadata =
    base::ChunkMetadata{
      .mimetype = "application/octet-stream",
    },
    .data = "",
  };
}

constexpr base::Chunk EndOfStream() { return MakeNullChunk(); }

} // namespace eglt

#endif  // EGLT_DATA_EG_STRUCTS_H_
