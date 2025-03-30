#ifndef EGLT_DATA_SERIALIZATION_H_
#define EGLT_DATA_SERIALIZATION_H_

#include <any>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"

namespace eglt {

using Bytes = std::vector<uint8_t>;
using MimeSerializer = std::function<Bytes(const std::any&)>;
using MimeDeserializer = std::function<std::optional<std::any>(const Bytes&)>;

class Serializer {
public:
  virtual ~Serializer() = default;

  [[nodiscard]] virtual Bytes Serialize(const std::any& value) const = 0;

  [[nodiscard]] Bytes Serialize(const std::any& value,
                                const std::string_view mimetype) const {
    if (serializers_.contains(mimetype)) {
      return serializers_.at(mimetype)(value);
    }
    return {};
  }

  [[nodiscard]] virtual std::optional<std::any> Deserialize(
    const Bytes& data) const = 0;

  [[nodiscard]] std::optional<std::any> Deserialize(
    const Bytes& data,
    const std::string_view mimetype) const {
    if (deserializers_.contains(mimetype)) {
      return deserializers_.at(mimetype)(data);
    }
    return std::nullopt;
  }

  void RegisterSerializer(std::string_view mimetype,
                          MimeSerializer serializer) {
    serializers_[mimetype] = std::move(serializer);
  }

  void RegisterDeserializer(std::string_view mimetype,
                            MimeDeserializer deserializer) {
    deserializers_[mimetype] = std::move(deserializer);
  }

private:
  absl::flat_hash_map<std::string, MimeSerializer> serializers_;
  absl::flat_hash_map<std::string, MimeDeserializer> deserializers_;
};

} // namespace eglt

#endif //EGLT_DATA_SERIALIZATION_H_
