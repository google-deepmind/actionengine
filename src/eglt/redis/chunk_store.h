#ifndef EGLT_REDIS_CHUNK_STORE_H_
#define EGLT_REDIS_CHUNK_STORE_H_

#include <cppack/msgpack.h>

#include <absl/strings/str_replace.h>

#include "eglt/data/msgpack.h"
#include "eglt/redis/chunk_store_ops/close_writes.lua.h"
#include "eglt/redis/chunk_store_ops/pop.lua.h"
#include "eglt/redis/chunk_store_ops/put.lua.h"
#include "eglt/redis/redis.h"
#include "eglt/redis/streams.h"
#include "eglt/stores/chunk_store.h"

namespace eglt::redis {

struct ChunkStoreEvent {
  std::string type;
  int seq = -1;
  int arrival_offset = -1;
  std::string stream_message_id;

  static ChunkStoreEvent FromString(const std::string& message) {
    const std::vector<std::string> parts =
        absl::StrSplit(message, absl::MaxSplits(':', 4));

    ChunkStoreEvent event;
    event.type = parts[0];
    if (event.type == "CLOSE") {
      return event;
    }
    if (event.type == "NEW") {
      bool success = true;
      success &= absl::SimpleAtoi(parts[1], &event.seq);
      success &= absl::SimpleAtoi(parts[2], &event.arrival_offset);
      event.stream_message_id = parts[3];
      if (!success) {
        LOG(ERROR) << "Failed to parse NEW event: " << message;
        return ChunkStoreEvent{"", -1, -1, ""};
      }
      return event;
    }

    LOG(FATAL) << "Unknown ChunkStoreEvent type: " << event.type
               << " in message: " << message;
    ABSL_ASSUME(false);
  }
};

class ChunkStore final : public eglt::ChunkStore {
 public:
  explicit ChunkStore(Redis* absl_nonnull redis, std::string_view id,
                      absl::Duration ttl = absl::InfiniteDuration());

  // No copy or move semantics allowed.
  ChunkStore(const ChunkStore&) = delete;
  ChunkStore& operator=(const ChunkStore& other) = delete;
  ~ChunkStore() override {
    eglt::MutexLock lock(&mu_);

    allow_new_gets_ = false;
    while (num_pending_gets_ > 0) {
      cv_.Wait(&mu_);
    }
  }

  absl::StatusOr<Chunk> Get(int64_t seq, absl::Duration timeout) override {
    absl::Time deadline = absl::Now() + timeout;
    eglt::MutexLock lock(&mu_);

    ASSIGN_OR_RETURN(std::optional<Chunk> chunk, TryGet(seq));
    if (chunk.has_value()) {
      return *chunk;
    }

    if (!allow_new_gets_) {
      return absl::FailedPreconditionError(
          "ChunkStore is closed for new gets.");
    }
    ++num_pending_gets_;

    while (absl::Now() < deadline) {
      if (cv_.WaitWithDeadline(&mu_, deadline)) {
        break;  // Timeout reached.
      }

      absl::StatusOr<std::optional<Chunk>> chunk_or_error = TryGet(seq);
      if (!chunk_or_error.ok()) {
        --num_pending_gets_;
        cv_.SignalAll();
        return chunk_or_error.status();
      }
      if (chunk_or_error->has_value()) {
        --num_pending_gets_;
        cv_.SignalAll();
        return **chunk_or_error;
      }
    }

    --num_pending_gets_;
    cv_.SignalAll();
    return absl::DeadlineExceededError(
        absl::StrCat("Timed out waiting for chunk with seq ", seq));
  }

  absl::StatusOr<Chunk> GetByArrivalOrder(int64_t arrival_offset,
                                          absl::Duration timeout) override {
    return absl::UnimplementedError("not implemented yet");
  }

  absl::StatusOr<std::optional<Chunk>> Pop(int64_t seq) override {
    return absl::UnimplementedError("not implemented yet");
  }

  absl::Status Put(int64_t seq, Chunk chunk, bool final) override {
    const std::string stream_id = GetKey();

    std::vector<std::string> key_strings;
    CommandArgs keys;
    key_strings.reserve(kPutScriptKeys.size());
    keys.reserve(kPutScriptKeys.size());
    for (auto& key : kPutScriptKeys) {
      std::string fully_qualified_key = key;
      absl::StrReplaceAll({{"{}", stream_id}}, &fully_qualified_key);
      key_strings.push_back(std::move(fully_qualified_key));
      keys.push_back(key_strings.back());
    }

    const std::string arg_seq = absl::StrCat(seq);
    const std::vector<uint8_t> chunk_bytes = cppack::Pack(std::move(chunk));
    std::string arg_data(chunk_bytes.begin(), chunk_bytes.end());
    const std::string arg_final = final ? "1" : "0";
    const std::string arg_ttl = ttl_ == absl::InfiniteDuration()
                                    ? "0"
                                    : absl::StrCat(absl::ToInt64Seconds(ttl_));
    const std::string arg_status_ttl =
        absl::StrCat(60 * 60 * 24 * 2);  // 2 days

    absl::StatusOr<Reply> reply =
        redis_->ExecuteScript("CHUNK STORE PUT", keys, arg_seq, arg_data,
                              arg_final, arg_ttl, arg_status_ttl);
    if (!reply.ok()) {
      return reply.status();
    }
    if (reply->IsError()) {
      if (auto status = std::get<ErrorReplyData>(reply->data).AsAbslStatus();
          status.message() == "SEQ_EXISTS") {
        return absl::AlreadyExistsError(
            absl::StrCat("Chunk with seq ", seq, " already exists."));
      } else {
        return status;
      }
    }
    return absl::OkStatus();
  }

  absl::Status CloseWritesWithStatus(absl::Status status) override {
    std::string arg_status = status.ToString();

    std::vector<std::string> key_strings;
    CommandArgs keys;
    keys.reserve(kCloseWritesScriptKeys.size());
    for (auto& key : kCloseWritesScriptKeys) {
      std::string fully_qualified_key = key;
      absl::StrReplaceAll({{"{}", id_}}, &fully_qualified_key);
      key_strings.push_back(std::move(fully_qualified_key));
      keys.push_back(key_strings.back());
    }

    ASSIGN_OR_RETURN(
        Reply reply,
        redis_->ExecuteScript("CHUNK STORE CLOSE WRITES", keys, arg_status));
    if (reply.IsError()) {
      return std::get<ErrorReplyData>(reply.data).AsAbslStatus();
    }
    if (reply.type != ReplyType::String) {
      return absl::InternalError(
          absl::StrCat("Unexpected reply type: ", reply.type));
    }
    return absl::OkStatus();
  }

  absl::StatusOr<size_t> Size() override {
    const std::string offset_to_seq_key = GetKey("offset_to_seq");
    ASSIGN_OR_RETURN(Reply reply,
                     redis_->ExecuteCommand("ZCARD", offset_to_seq_key));
    return ConvertToOrDie<int64_t>(std::move(reply));
  }

  absl::StatusOr<bool> Contains(int64_t seq) override {
    const std::string seq_to_id_key = GetKey("seq_to_id");
    ASSIGN_OR_RETURN(
        Reply reply,
        redis_->ExecuteCommand("HEXISTS", seq_to_id_key, absl::StrCat(seq)));
    if (reply.type != ReplyType::Integer) {
      return absl::InternalError(
          absl::StrCat("Unexpected reply type: ", reply.type));
    }
    ASSIGN_OR_RETURN(const auto exists, ConvertTo<int64_t>(std::move(reply)));
    return exists == 1;
  }

  absl::Status SetId(std::string_view id) override {
    CHECK(id == id_) << "Cannot change the ID of a ChunkStore.";
    return absl::OkStatus();
  }
  std::string_view GetId() const override { return id_; }

  absl::StatusOr<int64_t> GetSeqForArrivalOffset(
      int64_t arrival_offset) override {
    ASSIGN_OR_RETURN(auto value_map,
                     redis_->ZRange(GetKey("offset_to_seq"), arrival_offset,
                                    arrival_offset, /*withscores=*/true));
    if (value_map.empty()) {
      return -1;
    }
    return value_map.begin()->second.value_or(-1);
  }
  absl::StatusOr<int64_t> GetFinalSeq() override {
    const std::string final_seq_key = GetKey("final_seq");
    ASSIGN_OR_RETURN(const Reply reply,
                     redis_->ExecuteCommand("GET", final_seq_key));
    if (reply.type != ReplyType::String) {
      return absl::InternalError(
          absl::StrCat("Unexpected reply type: ", reply.type));
    }
    if (std::get<StringReplyData>(reply.data).value.empty()) {
      return -1;  // No final sequence set.
    }
    ASSIGN_OR_RETURN(auto final_seq, ConvertTo<int64_t>(std::move(reply)));
    return final_seq;
  }

 private:
  std::string GetKey(std::string_view key = "") const {
    if (key.empty()) {
      return redis_->GetKey(absl::StrCat("streams:", id_));
    }
    return redis_->GetKey(absl::StrCat("streams:", id_, ":", key));
  }

  absl::StatusOr<Chunk> GetInternal(int64_t seq, absl::Duration timeout)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::StatusOr<std::optional<Chunk>> TryGet(int64_t seq)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    const std::string seq_to_id_key = GetKey("seq_to_id");

    ASSIGN_OR_RETURN(
        Reply stream_id_reply,
        redis_->ExecuteCommand("HGET", seq_to_id_key, absl::StrCat(seq)));
    ASSIGN_OR_RETURN(std::string stream_message_id_str,
                     ConvertTo<std::string>(std::move(stream_id_reply)));

    if (stream_message_id_str.empty()) {
      return std::nullopt;  // No message found for this sequence number.
    }

    ASSIGN_OR_RETURN(auto stream_message_id,
                     StreamMessageId::FromString(stream_message_id_str));
    ASSIGN_OR_RETURN(std::vector<StreamMessage> messages,
                     stream_.XRange(stream_message_id, stream_message_id, 1));
    if (messages.empty()) {
      return absl::NotFoundError(
          absl::StrCat("No message found for seq ", seq));
    }
    if (messages.size() > 1) {
      return absl::InternalError(
          absl::StrCat("Expected a single message for seq ", seq, ", got ",
                       messages.size()));
    }
    const auto& message = messages[0];
    if (message.fields.size() != 2) {
      return absl::InternalError(
          absl::StrCat("Expected 2 fields in message for seq ", seq, ", got ",
                       message.fields.size()));
    }
    auto it = message.fields.find("data");
    if (it == message.fields.end()) {
      return absl::InternalError(
          absl::StrCat("Missing 'data' field in message for seq ", seq));
    }
    std::vector<uint8_t> message_bytes(it->second.begin(), it->second.end());
    ASSIGN_OR_RETURN(auto chunk, cppack::Unpack<Chunk>(message_bytes));
    return chunk;
  }

  mutable eglt::Mutex mu_;
  eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);

  bool allow_new_gets_ ABSL_GUARDED_BY(mu_) = true;
  bool writes_closed_ ABSL_GUARDED_BY(mu_) = false;
  size_t num_pending_gets_ ABSL_GUARDED_BY(mu_) = 0;
  absl::Duration ttl_ = absl::InfiniteDuration();

  absl::flat_hash_map<int, std::string> seq_to_stream_id_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<int, std::string> arrival_offset_to_stream_id_
      ABSL_GUARDED_BY(mu_);

  Redis* absl_nonnull redis_;
  const std::string id_;
  RedisStream stream_;
  std::shared_ptr<Subscription> subscription_;
};

}  // namespace eglt::redis

#endif  // EGLT_REDIS_CHUNK_STORE_H_