#include <cstddef>
#include <string>
#include <string_view>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"

namespace eglt::base {

template <>
Chunk ConstructFrom(std::nullptr_t) { return MakeNullChunk(); }

template <>
Chunk ConstructFrom(std::string value) {
  return Chunk{.metadata = {.mimetype = kMimetypeTextPlain},
               .data = std::move(value)};
}

template <>
Chunk ConstructFrom(const std::string_view value) {
  return Chunk{.metadata = {.mimetype = kMimetypeTextPlain},
               .data = std::string(value)};
}

template <>
Chunk ConstructFrom(const char* value) {
  return ConstructFrom<Chunk>(std::string(value));
}

template <>
absl::StatusOr<std::string> MoveAsAndReturnStatus(Chunk value) {
  if (!MimetypeIsTextual(value.metadata.mimetype)) {
    return absl::InvalidArgumentError(
      absl::StrCat("Cannot move as std::string from a non-textual chunk: ",
                   value.metadata.mimetype));
  }
  return std::move(value.data);
}

} // namespace eglt::base


