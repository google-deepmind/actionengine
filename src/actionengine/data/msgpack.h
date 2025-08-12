/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ACTIONENGINE_DATA_MSGPACK_H_
#define ACTIONENGINE_DATA_MSGPACK_H_

#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/data/types.h"

namespace cppack {
class Packer;
class Unpacker;
}  // namespace cppack

namespace cppack {

void CppackToBytes(const absl::Status& status, Packer& packer);
void CppackFromBytes(absl::Status& status, Unpacker& unpacker);

void CppackToBytes(const absl::Time& obj, Packer& packer);
void CppackFromBytes(absl::Time& obj, Unpacker& unpacker);

void CppackToBytes(const act::ChunkMetadata& obj, Packer& packer);
void CppackFromBytes(act::ChunkMetadata& obj, Unpacker& unpacker);

void CppackToBytes(const act::Chunk& obj, Packer& packer);
void CppackFromBytes(act::Chunk& obj, Unpacker& unpacker);

void CppackToBytes(const act::NodeFragment& obj, Packer& packer);
void CppackFromBytes(act::NodeFragment& obj, Unpacker& unpacker);

void CppackToBytes(const act::Port& obj, Packer& packer);
void CppackFromBytes(act::Port& obj, Unpacker& unpacker);

void CppackToBytes(const act::ActionMessage& obj, Packer& packer);
void CppackFromBytes(act::ActionMessage& obj, Unpacker& unpacker);

void CppackToBytes(const act::SessionMessage& obj, Packer& packer);
void CppackFromBytes(act::SessionMessage& obj, Unpacker& unpacker);

}  // namespace cppack

#endif  // ACTIONENGINE_DATA_MSGPACK_H_
