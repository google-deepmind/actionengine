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
#include "eglt/data/eg_structs.h"

namespace eglt {
template <typename T>
class Converters {
 public:
  template <typename FromT>
  static absl::StatusOr<T> TryConstructFrom(FromT from) {
    T result;
    if (auto status = EgltAssignInto(std::move(from), &result); !status.ok()) {
      return status;
    }
    return result;
  }

  template <typename FromT>
  static T From(FromT from) {
    auto result = TryConstructFrom(std::move(from));
    if (!result.ok()) {
      LOG(FATAL) << "Conversion failed: " << result.status();
    }
    return *std::move(result);
  }

  template <typename ToT>
  static absl::StatusOr<ToT> TryConvertTo(T from) {
    ToT result;
    if (auto status = EgltAssignInto(std::move(from), &result); !status.ok()) {
      return status;
    }
    return result;
  }

  template <typename ToT>
  static absl::Status TryConvertTo(T&& from, ToT* to) {
    return EgltAssignInto(std::move(from), to);
  }

  template <typename ToT>
  static ToT To(T from) {
    absl::StatusOr<ToT> result = TryConvertTo<ToT>(std::move(from));
    if (result.ok()) {
      return *std::move(result);
    }

    LOG(FATAL) << "Conversion failed: " << result.status() << from;
    std::terminate();
  }

  template <typename ToT>
  static void To(T&& from, ToT* to) {
    if (const absl::Status status = TryConvertTo(std::move(from), to);
        !status.ok()) {
      LOG(FATAL) << "Conversion failed: " << status;
    }
  }
};
}  // namespace eglt

#endif  // EGLT_DATA_CONVERSION_H_