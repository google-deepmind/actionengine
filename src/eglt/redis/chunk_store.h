#ifndef EGLT_REDIS_CHUNK_STORE_H
#define EGLT_REDIS_CHUNK_STORE_H

#include <cppack/msgpack.h>

#include "eglt/data/msgpack.h"
#include "eglt/redis/redis.h"
#include "eglt/redis/streams.h"
#include "eglt/stores/chunk_store.h"

namespace eglt::redis {

struct ChunkStoreEvent {
  std::string type;
  int seq_id = -1;
  int arrival_offset = -1;
  std::string stream_message_id;

  static ChunkStoreEvent FromString(const std::string& message) {
    std::vector<std::string> parts =
        absl::StrSplit(message, absl::MaxSplits(':', 4));

    bool success = true;

    ChunkStoreEvent event;
    event.type = parts[0];
    success &= absl::SimpleAtoi(parts[1], &event.seq_id);
    success &= absl::SimpleAtoi(parts[2], &event.arrival_offset);
    event.stream_message_id = parts[3];

    return success ? event : ChunkStoreEvent{"", -1, -1, ""};
  }
};

class ChunkStore final : public eglt::ChunkStore {
 public:
  explicit ChunkStore(Redis* absl_nonnull redis, std::string_view id)
      : redis_(redis), stream_(redis, id) {
    subscription_ = redis_->Subscribe(id, [this](Reply reply) {
      auto event_message = StatusOrConvertTo<std::string>(std::move(reply));
      if (!event_message.ok()) {
        LOG(ERROR) << "Failed to convert reply to string: "
                   << event_message.status();
        return;
      }
      auto event = ChunkStoreEvent::FromString(event_message.value());
      eglt::MutexLock lock(&mu_);
      if (auto event_it = pending_gets_by_seq_id_.find(event.seq_id);
          event_it != pending_gets_by_seq_id_.end()) {}
    });
  }

  ~ChunkStore() override {
    eglt::MutexLock lock(&mu_);
    allow_new_gets_ = false;
    while (pending_gets_ > 0) {
      cv_.Wait(&mu_);
    }
  }

  // No copy or move semantics allowed.
  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore& other) = delete;

  absl::StatusOr<std::reference_wrapper<const Chunk>> Get(
      int seq_id, absl::Duration timeout) const override {}
  absl::StatusOr<std::reference_wrapper<const Chunk>> GetByArrivalOrder(
      int arrival_offset, absl::Duration timeout) const override;
  std::optional<Chunk> Pop(int seq_id) override;
  absl::Status Put(int seq_id, Chunk chunk, bool final) override {}

  void NoFurtherPuts() override {
    eglt::MutexLock lock(&mu_);
    allow_new_puts_ = false;
    auto reply = redis_->ExecuteCommand("HSET", GetMetadataHashKey(),
                                        "writes_closed", "1");
    CHECK_OK(reply.status());
  }

  size_t Size() override {
    auto size_reply =
        redis_->ExecuteCommand("HINCRBY", GetMetadataHashKey(), "size", "0");
    CHECK_OK(size_reply.status());

    auto size = StatusOrConvertTo<int64_t>(*std::move(size_reply));
    CHECK_OK(size.status());

    return *size;
  }
  bool Contains(int seq_id) override {
    std::string key =
        redis_->GetKey(absl::StrCat("streams:", id_, ":c:", seq_id));

    auto reply = redis_->ExecuteCommand("EXISTS", key);
    CHECK_OK(reply.status());

    auto times_exists = StatusOrConvertTo<int64_t>(reply.value());
    CHECK_OK(times_exists.status());

    return *times_exists > 0;
  }

  void SetId(std::string_view id) override {
    CHECK(id == id_) << "Cannot change the ID of a ChunkStore.";
  }
  std::string_view GetId() const override { return id_; }

  int GetSeqIdForArrivalOffset(int arrival_offset) override;
  int GetFinalSeqId() override {
    std::string key = redis_->GetKey(absl::StrCat("streams:", id_, ":final"));
    auto reply = redis_->ExecuteCommand("GET", key);
    CHECK_OK(reply.status());

    auto final_seq_id = StatusOrConvertTo<int64_t>(reply.value());
    CHECK_OK(final_seq_id.status());

    return static_cast<int>(*final_seq_id);
  }

 private:
  std::string GetMetadataHashKey() const {
    return redis_->GetKey(absl::StrCat("streams:", id_, ":meta"));
  }

  mutable eglt::Mutex mu_;
  eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);

  bool allow_new_gets_ ABSL_GUARDED_BY(mu_) = true;
  bool allow_new_puts_ ABSL_GUARDED_BY(mu_) = true;
  size_t pending_gets_ ABSL_GUARDED_BY(mu_) = 0;

  absl::flat_hash_map<int, std::vector<thread::PermanentEvent*>>
      pending_gets_by_seq_id_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, std::vector<thread::PermanentEvent*>>
      pending_gets_by_arrival_offset_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, std::string> seq_to_stream_id_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, std::string> arrival_offset_to_stream_id_
      ABSL_GUARDED_BY(mu_);

  Redis* absl_nonnull redis_;
  RedisStream stream_;
  const std::string_view id_;
  std::shared_ptr<Subscription> subscription_;
};

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_H