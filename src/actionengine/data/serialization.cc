#include "actionengine/data/serialization.h"

namespace act {
absl::StatusOr<std::any> SerializerRegistry::Deserialize(
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

void SerializerRegistry::RegisterSerializer(std::string_view mimetype,
                                            Serializer serializer) {
  mime_serializers_[mimetype].push_back(std::move(serializer));
}

void SerializerRegistry::RegisterDeserializer(std::string_view mimetype,
                                              Deserializer deserializer) {
  mime_deserializers_[mimetype].push_back(std::move(deserializer));
}

bool SerializerRegistry::HasSerializers(std::string_view mimetype) const {
  return mime_serializers_.contains(mimetype);
}

bool SerializerRegistry::HasDeserializers(std::string_view mimetype) const {
  return mime_deserializers_.contains(mimetype);
}

void* SerializerRegistry::GetUserData() const {
  return user_data_.get();
}

void SerializerRegistry::SetUserData(std::shared_ptr<void> user_data) {
  user_data_ = std::move(user_data);
}

static inline absl::once_flag kInitSerializerRegistryFlag;

void InitSerializerRegistryWithDefaults(SerializerRegistry* registry) {
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

SerializerRegistry& GetGlobalSerializerRegistry() {
  static SerializerRegistry global_registry;
  absl::call_once(kInitSerializerRegistryFlag,
                  InitSerializerRegistryWithDefaults, &global_registry);
  return global_registry;
}

void SetGlobalSerializerRegistry(const SerializerRegistry& registry) {
  GetGlobalSerializerRegistry() = registry;
}

absl::StatusOr<std::any> FromChunk(const Chunk& chunk,
                                   std::string_view mimetype,
                                   const SerializerRegistry* const registry) {
  const SerializerRegistry* resolved_registry =
      registry ? registry : GetGlobalSerializerRegistryPtr();

  return resolved_registry->Deserialize(
      chunk.data, !mimetype.empty() ? mimetype : chunk.metadata.mimetype);
}
}  // namespace act