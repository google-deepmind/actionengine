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
using Serializer = std::function<Bytes(std::any)>;
using Deserializer = std::function<absl::StatusOr<std::any>(Bytes)>;

class SerializerRegistry {
 public:
  template <typename T>
  [[nodiscard]] absl::StatusOr<Bytes> Serialize(
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
      Bytes data, std::string_view mimetype) const {
    if (mimetype.empty()) {
      return absl::InvalidArgumentError(
          "Deserialize(data, mimetype) was called with an empty mimetype.");
    }

    const auto it = mime_deserializers_.find(mimetype);
    if (it == mime_deserializers_.end()) {
      return absl::UnimplementedError(absl::StrFormat(
          "No deserializer is registered for mimetype %v.", mimetype));
    }

    return it->second({std::move(data)});
  }

  // The registered deserializer must return an std::any which actually
  // contains the type T. This is not checked at compile time and is not
  // the responsibility of the registry. Essentially, this method is just
  // a convenience wrapper for std::any_cast<T>(result)-or-status.
  template <typename T>
  [[nodiscard]] absl::StatusOr<T> DeserializeAs(
      Bytes data, std::string_view mimetype) const {
    auto deserialized = Deserialize(std::move(data), mimetype);
    if (!deserialized.ok()) {
      return deserialized.status();
    }
    if (std::any_cast<T>(&*deserialized) == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Deserialized type does not match expected type %s.",
                          typeid(T).name()));
    }
    return std::any_cast<T>(*std::move(deserialized));
  }

  void RegisterSerializer(std::string_view mimetype, Serializer serializer) {
    mime_serializers_.insert_or_assign(mimetype, std::move(serializer));
  }

  void RegisterDeserializer(std::string_view mimetype,
                            Deserializer deserializer) {
    mime_deserializers_.insert_or_assign(mimetype, std::move(deserializer));
  }

  bool HasDeserializer(std::string_view mimetype) const {
    return mime_deserializers_.contains(mimetype);
  }

 protected:
  absl::flat_hash_map<std::string, Serializer> mime_serializers_;
  absl::flat_hash_map<std::string, Deserializer> mime_deserializers_;
};

static SerializerRegistry* GetGlobalSerializerRegistryPtr() {
  static auto registry = new SerializerRegistry();
  return registry;
}

static SerializerRegistry& GetGlobalSerializerRegistry() {
  return *GetGlobalSerializerRegistryPtr();
}

static void SetGlobalSerializerRegistry(const SerializerRegistry& registry) {
  GetGlobalSerializerRegistry() = registry;
}

template <typename T>
absl::StatusOr<Chunk> Serialize(T value, std::string_view mimetype = {},
                                SerializerRegistry* const registry = nullptr)
    requires(ConvertibleToChunk<T>) {
  if (mimetype.empty()) {
    return eglt::ConvertTo<Chunk>(std::move(value));
  }

  SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  auto data = resolved_registry->Serialize(std::move(value), mimetype);
  if (!data.ok()) {
    return data.status();
  }
  return Chunk{.metadata = ChunkMetadata{.mimetype = std::string(mimetype)},
               .data = std::move(*data)};
}

template <typename T>
absl::StatusOr<Chunk> Serialize(T value, std::string_view mimetype = {},
                                SerializerRegistry* const registry = nullptr)
    requires(!ConvertibleToChunk<T>) {
  if (mimetype.empty()) {
    return absl::FailedPreconditionError(
        "Serialize(value, mimetype) was called with an empty mimetype, and "
        "value's type is not a candidate for ADL-based conversion.");
  }

  SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  auto data = resolved_registry->Serialize(std::move(value), mimetype);
  if (!data.ok()) {
    return data.status();
  }
  return Chunk{.metadata = ChunkMetadata{.mimetype = std::string(mimetype)},
               .data = std::move(*data)};
}

template <typename T>
absl::StatusOr<T> DeserializeAs(Chunk chunk, std::string_view mimetype = {},
                                SerializerRegistry* const registry = nullptr)
    requires(ConvertibleFromChunk<T>) {
  if (mimetype.empty()) {
    return eglt::ConvertTo<T>(std::move(chunk));
  }

  if (chunk.metadata.mimetype.empty()) {
    chunk.metadata.mimetype = mimetype;
    return eglt::ConvertTo<T>(std::move(chunk));
  }

  SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->DeserializeAs<T>(std::move(chunk), mimetype);
}

template <typename T>
absl::StatusOr<T> DeserializeAs(Chunk chunk, std::string_view mimetype = {},
                                SerializerRegistry* const registry = nullptr)
    requires(!ConvertibleFromChunk<T>) {
  if (mimetype.empty()) {
    return absl::FailedPreconditionError(
        "Deserialize(chunk, mimetype) was called with an empty mimetype, "
        "and chunk's type is not a candidate for ADL-based conversion.");
  }

  SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->DeserializeAs<T>(std::move(chunk), mimetype);
}

inline absl::StatusOr<std::any> Deserialize(Bytes data,
                                            std::string_view mimetype = {}) {
  return GetGlobalSerializerRegistry().Deserialize(std::move(data), mimetype);
}

}  // namespace eglt

#endif  //EGLT_DATA_SERIALIZATION_H_
