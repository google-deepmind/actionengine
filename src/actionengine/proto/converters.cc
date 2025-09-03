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

#include "actionengine/proto/converters.h"

#include <variant>

#include <google/protobuf/util/time_util.h>

#include "actionengine/util/status_macros.h"

namespace act::proto {

absl::Status EgltAssignInto(act::ChunkMetadata from,
                            act::proto::ChunkMetadata* to) {
  to->set_mimetype(std::move(from.mimetype));
  if (from.timestamp) {
    *to->mutable_timestamp() =
        google::protobuf::util::TimeUtil::MillisecondsToTimestamp(
            absl::ToUnixMillis(*from.timestamp));
  }
  to->clear_attributes();
  for (auto& [key, value] : std::move(from.attributes)) {
    (*to->mutable_attributes())[key] = std::move(value);
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::ChunkMetadata from,
                            act::ChunkMetadata* to) {
  to->mimetype = std::move(*from.mutable_mimetype());
  if (from.has_timestamp()) {
    to->timestamp = absl::FromUnixMillis(
        google::protobuf::util::TimeUtil::TimestampToMilliseconds(
            from.timestamp()));
  } else {
    to->timestamp.reset();
  }
  to->attributes.clear();
  for (auto& [key, value] : *from.mutable_attributes()) {
    to->attributes[key] = std::move(value);
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::Chunk from, act::proto::Chunk* to) {
  if (from.metadata) {
    ASSIGN_OR_RETURN(auto metadata, ConvertTo<act::proto::ChunkMetadata>(
                                        std::move(*from.metadata)));
    *to->mutable_metadata() = std::move(metadata);
  }
  *to->mutable_ref() = std::move(from.ref);
  *to->mutable_data() = std::move(from.data);
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::Chunk from, act::Chunk* to) {
  if (from.has_metadata()) {
    ASSIGN_OR_RETURN(auto metadata,
                     ConvertTo<act::ChunkMetadata>(*from.mutable_metadata()));
    to->metadata = std::move(metadata);
  } else {
    to->metadata.reset();
  }
  to->ref = std::move(*from.mutable_ref());
  to->data = std::move(*from.mutable_data());
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::NodeRef from, act::proto::NodeRef* to) {
  to->set_id(std::move(from.id));
  to->set_offset(from.offset);
  if (from.length) {
    to->set_length(*from.length);
  } else {
    to->clear_length();
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::NodeRef from, act::NodeRef* to) {
  to->id = std::move(*from.mutable_id());
  to->offset = from.offset();
  if (from.has_length()) {
    to->length = from.length();
  } else {
    to->length.reset();
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::NodeFragment from,
                            act::proto::NodeFragment* to) {
  to->set_id(std::move(from.id));
  if (std::holds_alternative<act::Chunk>(from.data)) {
    ASSIGN_OR_RETURN(auto chunk, ConvertTo<act::proto::Chunk>(std::move(
                                     std::get<act::Chunk>(from.data))));
    *to->mutable_chunk() = std::move(chunk);
  } else if (std::holds_alternative<act::NodeRef>(from.data)) {
    ASSIGN_OR_RETURN(auto node_ref, ConvertTo<act::proto::NodeRef>(std::move(
                                        std::get<act::NodeRef>(from.data))));
    *to->mutable_ref() = std::move(node_ref);
  } else {
    return absl::InvalidArgumentError(
        "NodeFragment data must be either Chunk or NodeRef.");
  }
  if (from.seq) {
    to->set_seq(*from.seq);
  } else {
    to->clear_seq();
  }
  if (from.continued) {
    to->set_continued(from.continued);
  } else {
    to->clear_continued();
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::NodeFragment from,
                            act::NodeFragment* to) {
  to->id = std::move(*from.mutable_id());
  if (from.has_chunk()) {
    ASSIGN_OR_RETURN(auto chunk, ConvertTo<act::Chunk>(*from.mutable_chunk()));
    to->data = std::move(chunk);
  } else if (from.has_ref()) {
    ASSIGN_OR_RETURN(auto node_ref,
                     ConvertTo<act::NodeRef>(*from.mutable_ref()));
    to->data = std::move(node_ref);
  } else {
    return absl::InvalidArgumentError(
        "NodeFragment must contain either Chunk or NodeRef.");
  }
  if (from.has_seq()) {
    to->seq = from.seq();
  } else {
    to->seq.reset();
  }
  to->continued = from.continued();
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::Port from, act::proto::Port* to) {
  to->set_name(std::move(from.name));
  to->set_id(std::move(from.id));
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::Port from, act::Port* to) {
  to->name = std::move(*from.mutable_name());
  to->id = std::move(*from.mutable_id());
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::ActionMessage from,
                            act::proto::ActionMessage* to) {
  to->set_id(std::move(from.id));

  for (std::vector<act::Port> inputs = std::move(from.inputs);
       auto& port : inputs) {
    ASSIGN_OR_RETURN(auto converted_port,
                     ConvertTo<act::proto::Port>(std::move(port)));
    *to->mutable_inputs()->Add() = std::move(converted_port);
  }

  for (std::vector<act::Port> outputs = std::move(from.outputs);
       auto& port : outputs) {
    ASSIGN_OR_RETURN(auto converted_port,
                     ConvertTo<act::proto::Port>(std::move(port)));
    *to->mutable_outputs()->Add() = std::move(converted_port);
  }

  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::proto::ActionMessage from,
                            act::ActionMessage* to) {
  to->id = std::move(*from.mutable_id());

  for (auto& port : *from.mutable_inputs()) {
    ASSIGN_OR_RETURN(auto converted_port,
                     ConvertTo<act::Port>(std::move(port)));
    to->inputs.push_back(std::move(converted_port));
  }
  for (auto& port : *from.mutable_outputs()) {
    ASSIGN_OR_RETURN(auto converted_port,
                     ConvertTo<act::Port>(std::move(port)));
    to->outputs.push_back(std::move(converted_port));
  }
  return absl::OkStatus();
}

absl::Status EgltAssignInto(act::WireMessage from,
                            act::proto::WireMessage* to) {
  for (std::vector<act::ActionMessage> actions = std::move(from.actions);
       auto& action : actions) {
    ASSIGN_OR_RETURN(auto converted_action,
                     ConvertTo<act::proto::ActionMessage>(std::move(action)));
    *to->mutable_actions()->Add() = std::move(converted_action);
  }

  for (std::vector<act::NodeFragment> fragments =
           std::move(from.node_fragments);
       auto& fragment : fragments) {
    ASSIGN_OR_RETURN(auto converted_fragment,
                     ConvertTo<act::proto::NodeFragment>(std::move(fragment)));
    *to->mutable_node_fragments()->Add() = std::move(converted_fragment);
  }

  return absl::OkStatus();
}

}  // namespace act::proto