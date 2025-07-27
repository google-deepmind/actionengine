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

#include "eglt/redis/chunk_store.h"

#include <absl/functional/any_invocable.h>

#include "cppack/msgpack.h"
#include "eglt/data/msgpack.h"
#include "eglt/redis/chunk_store_ops/pop.lua.h"
#include "eglt/redis/chunk_store_ops/put.lua.h"

namespace eglt::redis {
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
  subscription_ = redis_->Subscribe(GetKey("events"), [this](Reply reply) {
    auto event_message = ConvertTo<std::string>(std::move(reply));
    if (!event_message.ok()) {
      LOG(ERROR) << "Failed to convert reply to string: "
                 << event_message.status();
      return;
    }
    auto event = ChunkStoreEvent::FromString(event_message.value());
    eglt::MutexLock lock(&mu_);
    cv_.SignalAll();
  });
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

absl::StatusOr<std::optional<Chunk>> ChunkStore::TryGet(int64_t seq)
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

}  // namespace eglt::redis