#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

#include <eglt/data/eg_structs.h>
#include <eglt/absl_headers.h>

namespace eglt {

constexpr auto kMimetypeBytes = "application/octet-stream";
constexpr auto kMimetypeTextPlain = "text/plain";

constexpr bool MimetypeIsTextual(std::string_view mimetype) {
  return mimetype == kMimetypeTextPlain || mimetype == kMimetypeBytes;
}

namespace base {

template <>
Chunk ConstructFrom(std::nullptr_t) { return MakeNullChunk(); }

template <>
Chunk ConstructFrom(std::string value) {
  return Chunk{.metadata = {.mimetype = "text/plain"},
               .data = std::move(value)};
}

template <>
Chunk ConstructFrom(std::string_view value) {
  return Chunk{.metadata = {.mimetype = "text/plain"},
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

} // namespace base

} // namespace eglt
