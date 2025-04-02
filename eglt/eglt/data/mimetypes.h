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

#ifndef EGLT_DATA_MIMETYPES_H_
#define EGLT_DATA_MIMETYPES_H_

#include <string_view>

namespace eglt {
constexpr auto kMimetypeBytes = "application/octet-stream";
constexpr auto kMimetypeTextPlain = "text/plain";

constexpr bool MimetypeIsTextual(const std::string_view mimetype) {
  return mimetype == kMimetypeTextPlain || mimetype == kMimetypeBytes;
}
}  // namespace eglt

#endif  // EGLT_DATA_MIMETYPES_H_
