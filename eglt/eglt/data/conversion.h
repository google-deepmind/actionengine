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

/**
 * @file
 * @brief
 *   Evergreen data structures used to implement actions and nodes (data
 *   streams).
 */

#ifndef EGLT_DATA_CONVERSION_H_
#define EGLT_DATA_CONVERSION_H_

#include "eglt/absl_headers.h"

namespace eglt {

template <typename Dst, typename Src>
absl::Status StatusOrAssign(Src&& from, Dst* to) {
  return EgltAssignInto(std::forward<Src>(from), to);
}

template <typename Dst, typename Src>
void Assign(Src&& from, Dst* to) {
  if (const absl::Status status = StatusOrAssign(std::forward<Src>(from), to);
      !status.ok()) {
    LOG(FATAL) << "Conversion failed: " << status;
  }
}

template <typename Dst, typename Src>
absl::StatusOr<Dst> StatusOrConvertTo(Src&& from) {
  Dst result;
  if (auto status = StatusOrAssign(std::forward<Src>(from), &result);
      !status.ok()) {
    return status;
  }
  return result;
}

template <typename Dst, typename Src>
Dst ConvertTo(Src&& from) {
  if (auto result = StatusOrConvertTo<Dst>(std::forward<Src>(from));
      !result.ok()) {
    LOG(FATAL) << "Conversion failed: " << result.status();
    std::terminate();
  } else {
    return *result;
  }
}

}  // namespace eglt

#endif  // EGLT_DATA_CONVERSION_H_