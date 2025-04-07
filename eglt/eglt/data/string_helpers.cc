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

#include "eglt/data/eg_structs.h"

namespace eglt::base {
/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const ChunkMetadata& metadata) {
  if (!metadata.mimetype.empty()) {
    absl::Format(&sink, "mimetype: %s\n", metadata.mimetype);
  }
  if (metadata.timestamp != absl::Time()) {
    absl::Format(&sink, "timestamp: %s\n",
                 absl::FormatTime(metadata.timestamp));
  }
}

/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const Chunk& chunk) {
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

/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const NodeFragment& fragment) {
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

/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const NamedParameter& parameter) {
  if (!parameter.name.empty()) {
    sink.Append(absl::StrCat("name: ", parameter.name, "\n"));
  }
  if (!parameter.id.empty()) {
    sink.Append(absl::StrCat("id: ", parameter.id, "\n"));
  }
}

/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const ActionMessage& action) {
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

/// @private
template <typename Sink>
void AbslStringify(Sink& sink, const SessionMessage& message) {
  if (!message.node_fragments.empty()) {
    sink.Append("node_fragments: \n");
    for (const auto& node_fragment : message.node_fragments) {
      absl::Format(&sink, "%s\n", Indent(absl::StrCat(node_fragment), 2, true));
    }
  }
  if (!message.actions.empty()) {
    sink.Append(absl::StrCat("actions: \n"));
    for (const auto& action : message.actions) {
      absl::Format(&sink, "%s\n", Indent(absl::StrCat(action), 2, true));
    }
  }
}
}  // namespace eglt::base
