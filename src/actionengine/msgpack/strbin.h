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

#ifndef ACTIONENGINE_MSGPACK_STRBIN_H
#define ACTIONENGINE_MSGPACK_STRBIN_H

#include "actionengine/msgpack/core_helpers.h"

namespace act::msgpack {
absl::StatusOr<uint32_t> GetStrOrBinExtent(const LookupPointer& data,
                                           std::string_view type_for_error);

absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::string* absl_nullable);

absl::Status EgltMsgpackSerialize(const std::string& value,
                                  const InsertInfo& insert);

absl::StatusOr<uint32_t> EgltMsgpackDeserializeBin(
    const LookupPointer& data, std::vector<Byte>* absl_nonnull output);

absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, std::string* absl_nonnull output);

absl::StatusOr<uint32_t> EgltMsgpackGetExtent(const LookupPointer& data,
                                              std::vector<Byte>* absl_nullable);

absl::Status EgltMsgpackSerialize(const std::vector<uint8_t>& value,
                                  const InsertInfo& insert);

absl::StatusOr<uint32_t> EgltMsgpackDeserialize(
    const LookupPointer& data, std::vector<uint8_t>* absl_nonnull output);

}  // namespace act::msgpack

#endif  // ACTIONENGINE_MSGPACK_STRBIN_H