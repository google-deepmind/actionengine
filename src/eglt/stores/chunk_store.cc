#include "eglt/stores/chunk_store.h"

#include <absl/log/check.h>
#include <absl/time/time.h>

#include "eglt/util/status_macros.h"

namespace eglt {

absl::StatusOr<Chunk> ChunkStore::Get(int64_t seq, absl::Duration timeout) {
  ASSIGN_OR_RETURN(const Chunk& chunk, GetRef(seq, timeout));
  return chunk;
}

absl::StatusOr<Chunk> ChunkStore::GetByArrivalOrder(int64_t arrival_order,
                                                    absl::Duration timeout) {
  ASSIGN_OR_RETURN(const Chunk& chunk, GetRefByArrivalOrder(arrival_order, timeout));
  return chunk;
}

absl::StatusOr<std::reference_wrapper<const Chunk>> ChunkStore::GetRef(
    int64_t seq, absl::Duration timeout) {
  return absl::UnimplementedError("Not implemented.");
}

absl::StatusOr<std::reference_wrapper<const Chunk>>
ChunkStore::GetRefByArrivalOrder(int64_t seq, absl::Duration timeout) {
  return absl::UnimplementedError("Not implemented.");
}

std::optional<Chunk> ChunkStore::PopOrDie(int64_t seq) noexcept {
  absl::StatusOr<std::optional<Chunk>> chunk = Pop(seq);
  CHECK_OK(chunk.status()) << "Failed to pop chunk with seq " << seq << ": "
                           << chunk.status();
  return *chunk;
}

void ChunkStore::CloseWritesWithStatusOrDie(absl::Status status) noexcept {
  const absl::Status s = CloseWritesWithStatus(status);
  CHECK_OK(s) << "Failed to close writes with status: " << s;
}

size_t ChunkStore::SizeOrDie() noexcept {
  absl::StatusOr<size_t> size = Size();
  CHECK_OK(size.status()) << "Failed to get size: " << size.status();
  return *size;
}

bool ChunkStore::ContainsOrDie(int64_t seq) noexcept {
  absl::StatusOr<bool> contains = Contains(seq);
  CHECK_OK(contains.status()) << "Failed to check if store contains seq " << seq
                              << ": " << contains.status();
  return *contains;
}

void ChunkStore::SetIdOrDie(std::string_view id) noexcept {
  const absl::Status status = SetId(id);
  CHECK_OK(status) << "Failed to set ID: " << status;
}

int64_t ChunkStore::GetSeqForArrivalOffsetOrDie(
    int64_t arrival_offset) noexcept {
  absl::StatusOr<int64_t> seq = GetSeqForArrivalOffset(arrival_offset);
  CHECK_OK(seq.status()) << "Failed to get seq ID for arrival offset "
                         << arrival_offset << ": " << seq.status();
  return *seq;
}

int64_t ChunkStore::GetFinalSeqOrDie() noexcept {
  absl::StatusOr<int64_t> seq = GetFinalSeq();
  CHECK_OK(seq.status()) << "Failed to get final seq: " << seq.status();
  return *seq;
}

}  // namespace eglt