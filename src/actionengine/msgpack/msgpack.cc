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

#include "actionengine/msgpack/msgpack.h"

namespace act::msgpack {

Packer::Packer(SerializedBytesVector bytes) : bytes_(std::move(bytes)) {}

void Packer::Reset() {
  bytes_.clear();
  offset_ = 0;
}

void Packer::Reserve(size_t size) {
  bytes_.reserve(size);
}

const SerializedBytesVector& Packer::GetVector() const {
  return bytes_;
}

std::vector<Byte> Packer::GetStdVector() const {
  return ToStdVector(bytes_);
}

SerializedBytesVector Packer::ReleaseVector() {
  SerializedBytesVector result = std::move(bytes_);
  bytes_.clear();
  offset_ = 0;
  return result;
}

std::vector<Byte> Packer::ReleaseStdVector() {
  std::vector<Byte> result = ToStdVector(std::move(bytes_));
  bytes_.clear();
  offset_ = 0;
  return result;
}

}  // namespace act::msgpack