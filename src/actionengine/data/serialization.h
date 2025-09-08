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

#ifndef ACTIONENGINE_DATA_SERIALIZATION_H_
#define ACTIONENGINE_DATA_SERIALIZATION_H_

#include <any>
#include <functional>
#include <string>
#include <string_view>

#include <absl/container/flat_hash_map.h>

#include "actionengine/data/types.h"

namespace act {

using Bytes = std::string;
using Serializer = std::function<absl::StatusOr<Bytes>(std::any)>;
using Deserializer = std::function<absl::StatusOr<std::any>(Bytes)>;

class SerializerRegistry {
 public:
  template <typename T>
  [[nodiscard]] absl::StatusOr<Bytes> Serialize(
      T value, std::string_view mimetype) const;

  [[nodiscard]] absl::StatusOr<std::any> Deserialize(
      Bytes data, std::string_view mimetype) const;

  // The registered deserializer must return an std::any which actually
  // contains the type T. This is not checked at compile time and is not
  // the responsibility of the registry. Essentially, this method is just
  // a convenience wrapper for std::any_cast<T>(result)-or-status.
  template <typename T>
  [[nodiscard]] absl::StatusOr<T> DeserializeAs(
      const Bytes& data, std::string_view mimetype) const;

  void RegisterSerializer(std::string_view mimetype, Serializer serializer);

  void RegisterDeserializer(std::string_view mimetype,
                            Deserializer deserializer);

  [[nodiscard]] bool HasSerializers(std::string_view mimetype) const;

  [[nodiscard]] bool HasDeserializers(std::string_view mimetype) const;

  [[nodiscard]] void* GetUserData() const;

  void SetUserData(std::shared_ptr<void> user_data);

 protected:
  absl::flat_hash_map<std::string, absl::InlinedVector<Serializer, 2>>
      mime_serializers_;
  absl::flat_hash_map<std::string, absl::InlinedVector<Deserializer, 2>>
      mime_deserializers_;
  std::shared_ptr<void> user_data_ =
      nullptr;  // Optional user data for custom use.
};

template <typename T>
absl::StatusOr<Bytes> SerializerRegistry::Serialize(
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

  for (const auto& serializer : it->second | std::views::reverse) {
    // Attempt to serialize the value using the registered serializer.
    auto result = serializer(std::move(value));
    if (result.ok()) {
      return std::move(*result);
    }
  }

  return absl::UnimplementedError(
      absl::StrFormat("No serializer could handle value of type %s for "
                      "mimetype %v.",
                      typeid(T).name(), mimetype));
}

template <typename T>
absl::StatusOr<T> SerializerRegistry::DeserializeAs(
    const Bytes& data, std::string_view mimetype) const {
  auto deserialized = Deserialize(data, mimetype);
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

void InitSerializerRegistryWithDefaults(SerializerRegistry* registry);

SerializerRegistry& GetGlobalSerializerRegistry();

inline SerializerRegistry* GetGlobalSerializerRegistryPtr() {
  return &GetGlobalSerializerRegistry();
}

void SetGlobalSerializerRegistry(const SerializerRegistry& registry);

template <typename T>
absl::StatusOr<Chunk> ToChunk(T value, std::string_view mimetype = {},
                              SerializerRegistry* const registry = nullptr) {
  if (mimetype.empty()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "Serialize(value, mimetype) was called with an empty mimetype, and "
        "value's type %s is not a candidate for ADL-based conversion.",
        typeid(T).name()));
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

template <ConvertibleToChunk T>
absl::StatusOr<Chunk> ToChunk(T value, std::string_view mimetype = {},
                              SerializerRegistry* const registry = nullptr) {
  if (mimetype.empty()) {
    return act::ConvertToOrDie<Chunk>(std::move(value));
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
absl::StatusOr<T> FromChunkAs(Chunk chunk, std::string_view mimetype = {},
                              SerializerRegistry* const registry = nullptr)
    requires(ConvertibleFromChunk<T>) {
  if (mimetype.empty()) {
    return act::ConvertToOrDie<T>(std::move(chunk));
  }

  if (!chunk.metadata) {
    chunk.metadata.emplace();
  }

  if (chunk.metadata->mimetype.empty()) {
    chunk.metadata->mimetype = mimetype;
    return act::ConvertToOrDie<T>(std::move(chunk));
  }

  const SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->DeserializeAs<T>(std::move(chunk).data, mimetype);
}

absl::StatusOr<std::any> FromChunk(
    Chunk chunk, std::string_view mimetype = {},
    const SerializerRegistry* const registry = nullptr);

template <typename T>
absl::StatusOr<T> FromChunkAs(Chunk chunk, std::string_view mimetype = {},
                              SerializerRegistry* const registry = nullptr)
    requires(!ConvertibleFromChunk<T>) {
  if (mimetype.empty()) {
    return absl::FailedPreconditionError(
        "FromChunkAs(chunk, mimetype) was called with an empty mimetype, "
        "and chunk's type is not a candidate for ADL-based conversion.");
  }

  SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->DeserializeAs<T>(std::move(chunk), mimetype);
}

}  // namespace act

#endif  //ACTIONENGINE_DATA_SERIALIZATION_H_
