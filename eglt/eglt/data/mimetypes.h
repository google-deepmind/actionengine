#ifndef EGLT_DATA_MIMETYPES_H_
#define EGLT_DATA_MIMETYPES_H_

#include <string_view>

namespace eglt {
constexpr auto kMimetypeBytes = "application/octet-stream";
constexpr auto kMimetypeTextPlain = "text/plain";

constexpr bool MimetypeIsTextual(const std::string_view mimetype) {
  return mimetype == kMimetypeTextPlain || mimetype == kMimetypeBytes;
}
} // namespace eglt

#endif  // EGLT_DATA_MIMETYPES_H_
