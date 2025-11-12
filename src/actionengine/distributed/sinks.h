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

#ifndef ACTIONENGINE_DISTRIBUTED_SINKS_H_
#define ACTIONENGINE_DISTRIBUTED_SINKS_H_

#include <string>
#include <string_view>
#include <utility>

#include "actionengine/data/conversion.h"

namespace act::distributed {

class Sink {
 public:
  virtual ~Sink() = default;
  virtual absl::Status Append(std::string_view s) = 0;

  template <typename T>
  absl::Status Append(T&& value) {
    auto value_str = act::ConvertTo<std::string>(std::forward<T>(value));
    if (!value_str.ok()) {
      return value_str.status();
    }
    return this->Append(*std::move(value_str));
  }

  [[nodiscard]] virtual std::string_view View() const = 0;
};

class StringSink final : public Sink {
 public:
  StringSink() = default;

  explicit StringSink(std::string_view s);

  absl::Status Append(std::string_view s) override;

  [[nodiscard]] std::string_view View() const override;

 private:
  std::string str_;
};

}  // namespace act::distributed

#endif  // ACTIONENGINE_DISTRIBUTED_SINKS_H_
