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

#include "actionengine/redis/chunk_store.h"

#include <absl/functional/any_invocable.h>

#include "actionengine/data/msgpack.h"
#include "actionengine/redis/chunk_store_ops/pop.lua.h"
#include "actionengine/redis/chunk_store_ops/put.lua.h"
#include "cppack/msgpack.h"

namespace act::redis {
ChunkStoreEvent ChunkStoreEvent::FromString(const std::string& message) {
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

ChunkStore::ChunkStore(Redis* redis, std::string_view id, absl::Duration ttl)
    : redis_(redis), id_(id), stream_(redis, GetKey("s")) {
  if (ttl != absl::InfiniteDuration()) {
    CHECK(ttl >= absl::Seconds(1))
        << "TTL must not be less than one second, got: " << ttl;
    const auto ttl_whole_seconds = absl::Seconds(absl::ToInt64Seconds(ttl));
    DCHECK(ttl_whole_seconds == ttl)
        << "TTL must be a whole number of seconds, got: " << ttl;
    ttl_ = ttl_whole_seconds;
  }

  absl::Status registration_status;
  registration_status.Update(redis_
                                 ->RegisterScript("CHUNK STORE CLOSE WRITES",
                                                  kCloseWritesScriptCode,
                                                  /*overwrite_existing=*/false)
                                 .status());
  registration_status.Update(redis_
                                 ->RegisterScript("CHUNK STORE POP",
                                                  kPopScriptCode,
                                                  /*overwrite_existing=*/false)
                                 .status());
  registration_status.Update(redis_
                                 ->RegisterScript("CHUNK STORE PUT",
                                                  kPutScriptCode,
                                                  /*overwrite_existing=*/false)
                                 .status());
  // TODO(helenapankov): Handle registration errors more gracefully, minding
  //   it is a constructor.
  if (!registration_status.ok()) {
    LOG(FATAL) << "Failed to register chunk store scripts: "
               << registration_status;
  }
  absl::StatusOr<std::shared_ptr<Subscription>> subscription =
      redis_->Subscribe(GetKey("events"), [this](Reply reply) {
        auto event_message = ConvertTo<std::string>(std::move(reply));
        if (!event_message.ok()) {
          LOG(ERROR) << "Failed to convert reply to string: "
                     << event_message.status();
          return;
        }
        auto event = ChunkStoreEvent::FromString(event_message.value());
        act::MutexLock lock(&mu_);
        cv_.SignalAll();
      });
  CHECK_OK(subscription.status())
      << "Failed to subscribe to chunk store events: " << subscription.status();
  thread::Select({subscription->get()->OnSubscribe()});
  subscription_ = std::move(subscription.value());
}

ChunkStore::~ChunkStore() {
  act::MutexLock lock(&mu_);

  allow_new_gets_ = false;
  cv_.SignalAll();
  while (num_pending_gets_ > 0) {
    cv_.Wait(&mu_);
  }

  redis_->RemoveSubscription(GetKey("events"), subscription_);
  subscription_.reset();
}

absl::StatusOr<Chunk> ChunkStore::Get(int64_t seq, absl::Duration timeout) {
  absl::Time deadline = absl::Now() + timeout;
  act::MutexLock lock(&mu_);

  mu_.Unlock();
  ASSIGN_OR_RETURN(std::optional<Chunk> chunk, TryGet(seq));
  mu_.Lock();
  if (chunk.has_value()) {
    return *chunk;
  }

  if (!allow_new_gets_) {
    return absl::FailedPreconditionError("ChunkStore is closed for new gets.");
  }
  ++num_pending_gets_;

  while (absl::Now() < deadline) {
    if (cv_.WaitWithDeadline(&mu_, deadline)) {
      break;  // Timeout reached.
    }

    if (!allow_new_gets_) {
      --num_pending_gets_;
      cv_.SignalAll();
      return absl::FailedPreconditionError(
          "ChunkStore is closed for new gets.");
    }

    mu_.Unlock();
    absl::StatusOr<std::optional<Chunk>> chunk_or_error = TryGet(seq);
    mu_.Lock();
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

absl::StatusOr<Chunk> ChunkStore::GetByArrivalOrder(int64_t arrival_offset,
                                                    absl::Duration timeout) {
  return absl::UnimplementedError("not implemented yet");
}

absl::StatusOr<std::optional<Chunk>> ChunkStore::Pop(int64_t seq) {
  return absl::UnimplementedError("not implemented yet");
}

absl::Status ChunkStore::Put(int64_t seq, Chunk chunk, bool final) {
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
  const std::string arg_status_ttl = absl::StrCat(60 * 60 * 24 * 2);  // 2 days

  absl::StatusOr<Reply> reply = redis_->ExecuteScript(
      "CHUNK STORE PUT", keys,
      {arg_seq, arg_data, arg_final, arg_ttl, arg_status_ttl});
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

absl::Status ChunkStore::CloseWritesWithStatus(absl::Status status) {
  std::string arg_status = status.ToString();

  std::string stream_id = GetKey();

  std::vector<std::string> key_strings;
  CommandArgs keys;
  keys.reserve(kCloseWritesScriptKeys.size());
  for (auto& key : kCloseWritesScriptKeys) {
    std::string fully_qualified_key = key;
    absl::StrReplaceAll({{"{}", stream_id}}, &fully_qualified_key);
    key_strings.push_back(std::move(fully_qualified_key));
    keys.push_back(key_strings.back());
  }

  ASSIGN_OR_RETURN(
      Reply reply,
      redis_->ExecuteScript("CHUNK STORE CLOSE WRITES", keys, {arg_status}));
  if (reply.IsError()) {
    return std::get<ErrorReplyData>(reply.data).AsAbslStatus();
  }
  if (reply.type != ReplyType::String) {
    return absl::InternalError(
        absl::StrCat("Unexpected reply type: ", reply.type));
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> ChunkStore::Size() {
  const std::string offset_to_seq_key = GetKey("offset_to_seq");
  ASSIGN_OR_RETURN(Reply reply,
                   redis_->ExecuteCommand("ZCARD", {offset_to_seq_key}));
  return ConvertToOrDie<int64_t>(std::move(reply));
}

absl::StatusOr<bool> ChunkStore::Contains(int64_t seq) {
  const std::string seq_to_id_key = GetKey("seq_to_id");
  ASSIGN_OR_RETURN(
      Reply reply,
      redis_->ExecuteCommand("HEXISTS", {seq_to_id_key, absl::StrCat(seq)}));
  if (reply.type == ReplyType::Error) {
    return std::get<ErrorReplyData>(reply.data).AsAbslStatus();
  }
  if (reply.type != ReplyType::Integer) {
    return absl::InternalError(
        absl::StrCat("Unexpected reply type: ", reply.type));
  }
  ASSIGN_OR_RETURN(const auto exists, ConvertTo<int64_t>(std::move(reply)));
  return exists == 1;
}

absl::Status ChunkStore::SetId(std::string_view id) {
  CHECK(id == id_) << "Cannot change the ID of a ChunkStore.";
  return absl::OkStatus();
}

absl::StatusOr<int64_t> ChunkStore::GetSeqForArrivalOffset(
    int64_t arrival_offset) {
  ASSIGN_OR_RETURN(auto value_map,
                   redis_->ZRange(GetKey("offset_to_seq"), arrival_offset,
                                  arrival_offset, /*withscores=*/true));
  if (value_map.empty()) {
    return -1;
  }
  return value_map.begin()->second.value_or(-1);
}

absl::StatusOr<int64_t> ChunkStore::GetFinalSeq() {
  const std::string final_seq_key = GetKey("final_seq");
  ASSIGN_OR_RETURN(const Reply reply,
                   redis_->ExecuteCommand("GET", {final_seq_key}));
  if (reply.type == ReplyType::Nil) {
    return -1;  // No final sequence set.
  }
  if (reply.type != ReplyType::String) {
    return absl::InternalError(absl::StrCat(
        "Unexpected reply type: ", MapReplyEnumToTypeName(reply.type)));
  }
  if (std::get<StringReplyData>(reply.data).value.empty()) {
    return -1;  // No final sequence set.
  }
  ASSIGN_OR_RETURN(auto final_seq, ConvertTo<int64_t>(std::move(reply)));
  return final_seq;
}

absl::StatusOr<std::optional<Chunk>> ChunkStore::TryGet(int64_t seq) {
  const std::string seq_to_id_key = GetKey("seq_to_id");

  ASSIGN_OR_RETURN(
      Reply stream_id_reply,
      redis_->ExecuteCommand("HGET", {seq_to_id_key, absl::StrCat(seq)}));
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
    return absl::NotFoundError(absl::StrCat("No message found for seq ", seq));
  }
  if (messages.size() > 1) {
    return absl::InternalError(absl::StrCat(
        "Expected a single message for seq ", seq, ", got ", messages.size()));
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

}  // namespace act::redis