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

#include <absl/container/flat_hash_map.h>

#include "eglt/data/eg_structs.h"

namespace eglt {

using Bytes = std::string;
using Serializer = std::function<absl::StatusOr<Bytes>(std::any)>;
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

  [[nodiscard]] absl::StatusOr<std::any> Deserialize(
      const Bytes& data, std::string_view mimetype) const {
    if (mimetype.empty()) {
      return absl::InvalidArgumentError(
          "Deserialize(data, mimetype) was called with an empty mimetype.");
    }

    const auto it = mime_deserializers_.find(mimetype);
    if (it == mime_deserializers_.end()) {
      return absl::UnimplementedError(absl::StrFormat(
          "No deserializer is registered for mimetype %v.", mimetype));
    }

    for (const auto& deserializer : it->second | std::views::reverse) {
      // Attempt to deserialize the data using the registered deserializer.
      if (auto result = deserializer(data); result.ok()) {
        return std::move(*result);
      }
    }

    return absl::UnimplementedError(absl::StrFormat(
        "No deserializer could handle data for mimetype %v.", mimetype));
  }

  // The registered deserializer must return an std::any which actually
  // contains the type T. This is not checked at compile time and is not
  // the responsibility of the registry. Essentially, this method is just
  // a convenience wrapper for std::any_cast<T>(result)-or-status.
  template <typename T>
  [[nodiscard]] absl::StatusOr<T> DeserializeAs(
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

  void RegisterSerializer(std::string_view mimetype, Serializer serializer) {
    mime_serializers_[mimetype].push_back(std::move(serializer));
  }

  void RegisterDeserializer(std::string_view mimetype,
                            Deserializer deserializer) {
    mime_deserializers_[mimetype].push_back(std::move(deserializer));
  }

  [[nodiscard]] bool HasSerializers(std::string_view mimetype) const {
    return mime_serializers_.contains(mimetype);
  }

  [[nodiscard]] bool HasDeserializers(std::string_view mimetype) const {
    return mime_deserializers_.contains(mimetype);
  }

  [[nodiscard]] void* GetUserData() const { return user_data_.get(); }

  void SetUserData(std::shared_ptr<void> user_data) {
    user_data_ = std::move(user_data);
  }

 protected:
  absl::flat_hash_map<std::string, absl::InlinedVector<Serializer, 2>>
      mime_serializers_;
  absl::flat_hash_map<std::string, absl::InlinedVector<Deserializer, 2>>
      mime_deserializers_;
  std::shared_ptr<void> user_data_ =
      nullptr;  // Optional user data for custom use.
};

static inline absl::once_flag kInitSerializerRegistryFlag;

static void InitSerializerRegistryWithDefaults(SerializerRegistry* registry) {
  // Initialize the global serializer registry with default serializers.
  // This can be extended to include more serializers as needed.
  registry->RegisterSerializer(
      "text/plain", [](std::any value) -> absl::StatusOr<Bytes> {
        if (const auto str = std::any_cast<std::string>(&value);
            str != nullptr) {
          return std::move(*str);
        }
        return absl::InvalidArgumentError(
            "Cannot serialize value to text/plain: not a string.");
      });
  registry->RegisterDeserializer("text/plain",
                                 [](Bytes data) -> absl::StatusOr<std::any> {
                                   return std::any(std::move(data));
                                 });
  registry->RegisterSerializer(
      "application/octet-stream", [](std::any value) -> absl::StatusOr<Bytes> {
        if (const auto bytes = std::any_cast<Bytes>(&value); bytes != nullptr) {
          return std::move(*bytes);
        }
        return absl::InvalidArgumentError(
            "Cannot serialize value to application/octet-stream: not bytes.");
      });
}

inline SerializerRegistry& GetGlobalSerializerRegistry() {
  static SerializerRegistry global_registry;
  absl::call_once(kInitSerializerRegistryFlag,
                  InitSerializerRegistryWithDefaults, &global_registry);
  return global_registry;
}

inline SerializerRegistry* GetGlobalSerializerRegistryPtr() {
  return &GetGlobalSerializerRegistry();
}

static void SetGlobalSerializerRegistry(const SerializerRegistry& registry) {
  GetGlobalSerializerRegistry() = registry;
}

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
    return eglt::ConvertToOrDie<Chunk>(std::move(value));
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
    return eglt::ConvertToOrDie<T>(std::move(chunk));
  }

  if (chunk.metadata.mimetype.empty()) {
    chunk.metadata.mimetype = mimetype;
    return eglt::ConvertToOrDie<T>(std::move(chunk));
  }

  const SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->DeserializeAs<T>(std::move(chunk).data, mimetype);
}

inline absl::StatusOr<std::any> FromChunk(
    const Chunk& chunk, std::string_view mimetype = {},
    const SerializerRegistry* const registry = nullptr) {
  const SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->Deserialize(
      chunk.data, !mimetype.empty() ? mimetype : chunk.metadata.mimetype);
}

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

}  // namespace eglt

#endif  //EGLT_DATA_SERIALIZATION_H_
