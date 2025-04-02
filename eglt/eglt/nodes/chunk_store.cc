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

#include "chunk_store.h"

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"

namespace eglt {

auto ChunkStore::Get(int seq_id, float timeout) -> absl::StatusOr<base::Chunk> {
  if (auto status = this->WaitForSeqId(seq_id, timeout); !status.ok()) {
    LOG(ERROR) << "failed to wait for seq_id: " << status;
    return status;
  }

  return this->GetImmediately(seq_id);
}

absl::StatusOr<base::Chunk> ChunkStore::Pop(int seq_id, float timeout) {
  if (auto status = this->WaitForSeqId(seq_id, timeout); !status.ok()) {
    return status;
  }

  return this->PopImmediately(seq_id);
}

absl::Status ChunkStore::Put(int seq_id, base::Chunk chunk, bool final) {
  int final_seq_id = this->GetFinalSeqId();

  concurrency::MutexLock lock(&mutex_);

  bool can_put = final_seq_id == -1 || seq_id <= final_seq_id;
  if (!can_put) {
    return absl::FailedPreconditionError(
        "Cannot put chunks with seq_id > final_seq_id.");
  }

  const bool is_null = base::IsNullChunk(chunk);

  if (final) {
    final_seq_id = is_null ? seq_id - 1 : seq_id;
    this->SetFinalSeqId(final_seq_id);
  }

  auto offset = this->WriteToImmediateStore(seq_id, std::move(chunk));
  if (!offset.ok()) {
    return offset.status();
  }

  if (final_seq_id != -1 && *offset >= final_seq_id) {
    this->NotifyAllWaiters();
  } else {
    this->NotifyWaiters(seq_id, offset.value());
  }

  return absl::OkStatus();
}

}  // namespace eglt
