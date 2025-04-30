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

#ifndef EGLT_DATA_SERIALIZATION_H_
#define EGLT_DATA_SERIALIZATION_H_

#include <any>
#include <functional>
#include <string>
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"

namespace eglt {

using Bytes = std::string;
using MimeSerializer = std::function<Chunk(std::any)>;
using MimeDeserializer = std::function<absl::StatusOr<std::any>(Chunk)>;

class SerializerRegistry {
 public:
  template <typename T>
  [[nodiscard]] absl::StatusOr<Chunk> Serialize(
      T value, std::string_view mimetype) const {
    if (mimetype.empty()) {
      return absl::InvalidArgumentError(
          "Serialize(value, mimetype) was called with an empty mimetype.");
    }

    const auto it = mime_serializers_.find(mimetype);
    if (it == mime_serializers_.end()) {
      return absl::UnimplementedError(absl::StrFormat(
          "No serializer is registered for mimetype %v.", mimetype));
    }

    return it->second({std::move(value)});
  }

  [[nodiscard]] absl::StatusOr<std::any> Deserialize(
      Chunk chunk, std::string_view mimetype) const {
    if (mimetype.empty()) {
      return absl::InvalidArgumentError(
          "Deserialize(chunk, mimetype) was called with an empty mimetype.");
    }

    const auto it = mime_deserializers_.find(mimetype);
    if (it == mime_deserializers_.end()) {
      return absl::UnimplementedError(absl::StrFormat(
          "No deserializer is registered for mimetype %v.", mimetype));
    }

    return it->second({std::move(chunk)});
  }

  void RegisterMimeSerializer(std::string_view mimetype,
                              MimeSerializer serializer) {
    mime_serializers_.insert_or_assign(mimetype, std::move(serializer));
  }

  void RegisterMimeDeserializer(std::string_view mimetype,
                                MimeDeserializer deserializer) {
    mime_deserializers_.insert_or_assign(mimetype, std::move(deserializer));
  }

 protected:
  absl::flat_hash_map<std::string, MimeSerializer> mime_serializers_;
  absl::flat_hash_map<std::string, MimeDeserializer> mime_deserializers_;
};

}  // namespace eglt

#endif  //EGLT_DATA_SERIALIZATION_H_
