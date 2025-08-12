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

#ifndef ACTIONENGINE_PROTO_CONVERTERS_H_
#define ACTIONENGINE_PROTO_CONVERTERS_H_

#include <absl/base/nullability.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "actionengine/data/types.h"
#include "actionengine/proto/types.pb.h"

namespace act::proto {

absl::Status EgltAssignInto(act::ChunkMetadata from,
                            act::proto::ChunkMetadata* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::ChunkMetadata from,
                            act::ChunkMetadata* absl_nonnull to);

absl::Status EgltAssignInto(act::Chunk from,
                            act::proto::Chunk* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::Chunk from,
                            act::Chunk* absl_nonnull to);

absl::Status EgltAssignInto(act::NodeRef from,
                            act::proto::NodeRef* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::NodeRef from,
                            act::NodeRef* absl_nonnull to);

absl::Status EgltAssignInto(act::NodeFragment from,
                            act::proto::NodeFragment* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::NodeFragment from,
                            act::NodeFragment* absl_nonnull to);

absl::Status EgltAssignInto(act::Port from, act::proto::Port* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::Port from, act::Port* absl_nonnull to);

absl::Status EgltAssignInto(act::ActionMessage from,
                            act::proto::ActionMessage* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::ActionMessage from,
                            act::ActionMessage* absl_nonnull to);

absl::Status EgltAssignInto(act::WireMessage from,
                            act::proto::WireMessage* absl_nonnull to);
absl::Status EgltAssignInto(act::proto::WireMessage from,
                            act::WireMessage* absl_nonnull to);

}  // namespace act::proto

#endif  // ACTIONENGINE_PROTO_CONVERTERS_H_