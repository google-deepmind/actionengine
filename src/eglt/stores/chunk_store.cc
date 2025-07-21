#include "eglt/stores/chunk_store.h"
#include "eglt/util/status_macros.h"

namespace eglt {

absl::StatusOr<Chunk> ChunkStore::Get(int64_t seq, absl::Duration timeout) {
  ASSIGN_OR_RETURN(const Chunk& chunk, GetRef(seq, timeout));
  return chunk;
}

absl::StatusOr<Chunk> ChunkStore::GetByArrivalOrder(int64_t seq,
                                                    absl::Duration timeout) {
  ASSIGN_OR_RETURN(const Chunk& chunk, GetRefByArrivalOrder(seq, timeout));
  return chunk;
}

std::optional<Chunk> ChunkStore::Pop(int64_t seq) noexcept {
  absl::StatusOr<std::optional<Chunk>> chunk = StatusOrPop(seq);
  CHECK_OK(chunk.status()) << "Failed to pop chunk with seq " << seq << ": "
                           << chunk.status();
  return *chunk;
}

void ChunkStore::CloseWritesWithStatus(absl::Status status) noexcept {
  const absl::Status s = StatusOrCloseWritesWithStatus(status);
  CHECK_OK(s) << "Failed to close writes with status: " << s;
}

size_t ChunkStore::Size() noexcept {
  absl::StatusOr<size_t> size = StatusOrSize();
  CHECK_OK(size.status()) << "Failed to get size: " << size.status();
  return *size;
}

bool ChunkStore::Contains(int64_t seq) noexcept {
  absl::StatusOr<bool> contains = StatusOrContains(seq);
  CHECK_OK(contains.status()) << "Failed to check if store contains seq " << seq
                              << ": " << contains.status();
  return *contains;
}

void ChunkStore::SetId(std::string_view id) noexcept {
  const absl::Status status = StatusOrSetId(id);
  CHECK_OK(status) << "Failed to set ID: " << status;
}

int64_t ChunkStore::GetSeqForArrivalOffset(int64_t arrival_offset) noexcept {
  absl::StatusOr<int64_t> seq = StatusOrGetSeqForArrivalOffset(arrival_offset);
  CHECK_OK(seq.status()) << "Failed to get seq ID for arrival offset "
                         << arrival_offset << ": " << seq.status();
  return *seq;
}

int64_t ChunkStore::GetFinalSeq() noexcept {
  absl::StatusOr<int64_t> seq = StatusOrGetFinalSeq();
  CHECK_OK(seq.status()) << "Failed to get final seq: " << seq.status();
  return *seq;
}

}  // namespace eglt