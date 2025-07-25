#ifndef EGLT_REDIS_REPLY_CONVERTERS_H_
#define EGLT_REDIS_REPLY_CONVERTERS_H_

#include <absl/status/status.h>

#include "eglt/data/conversion.h"
#include "eglt/redis/reply.h"

namespace eglt::redis {

constexpr std::string_view MapReplyEnumToTypeName(ReplyType type);

// Converters to the primitive types of RESP2 and RESP3.
absl::Status EgltAssignInto(const Reply& from, absl::Status* to);
absl::Status EgltAssignInto(const Reply& from, int64_t* to);
absl::Status EgltAssignInto(const Reply& from, double* to);
absl::Status EgltAssignInto(Reply from, std::string* to);
absl::Status EgltAssignInto(const Reply& from, bool* to);

// Converters to the structured types of RESP2 and RESP3.
absl::Status EgltAssignInto(Reply from, ArrayReplyData* to);
absl::Status EgltAssignInto(Reply from, MapReplyData* to);
absl::Status EgltAssignInto(Reply from, SetReplyData* to);
absl::Status EgltAssignInto(Reply from, PushReplyData* to);
absl::Status EgltAssignInto(Reply from, VerbatimReplyData* to);

// Converters between structured types of RESP2 and RESP3.
absl::Status EgltAssignInto(ArrayReplyData from, MapReplyData* to);
absl::Status EgltAssignInto(MapReplyData from, ArrayReplyData* to);
absl::Status EgltAssignInto(PushReplyData from, ArrayReplyData* to);

// Converters of structured types into containers of Replies.
absl::Status EgltAssignInto(ArrayReplyData from, std::vector<Reply>* to);
absl::Status EgltAssignInto(ArrayReplyData from,
                            absl::flat_hash_map<std::string, Reply>* to);
absl::Status EgltAssignInto(MapReplyData from, std::vector<Reply>* to);
absl::Status EgltAssignInto(MapReplyData from,
                            absl::flat_hash_map<std::string, Reply>* to);
absl::Status EgltAssignInto(SetReplyData from, std::vector<Reply>* to);
absl::Status EgltAssignInto(PushReplyData from, std::vector<Reply>* to);
absl::Status EgltAssignInto(VerbatimReplyData from, std::string* to);

absl::Status EgltAssignInto(Reply from, std::vector<Reply>* to);
absl::Status EgltAssignInto(Reply from,
                            absl::flat_hash_map<std::string, Reply>* to);

// Converters of structured types into containers of native types.
template <typename T>
absl::Status EgltAssignInto(Reply from, std::vector<T>* to) {
  ASSIGN_OR_RETURN(std::vector<Reply> reply_vector,
                   ConvertTo<std::vector<Reply>>(std::move(from)));
  std::vector<T> converted_vector;
  converted_vector.reserve(reply_vector.size());
  for (const Reply& reply : reply_vector) {
    T value;
    ASSIGN_OR_RETURN(value, ConvertTo<T>(reply));
    converted_vector.push_back(std::move(value));
  }
  *to = std::move(converted_vector);
  return absl::OkStatus();
}

template <typename T>
absl::Status EgltAssignInto(Reply from,
                            absl::flat_hash_map<std::string, T>* to) {
  auto reply_map = ConvertTo<absl::flat_hash_map<std::string, Reply>>(
      std::move(from));
  RETURN_IF_ERROR(reply_map.status());
  absl::flat_hash_map<std::string, T> converted_map;
  converted_map.reserve(reply_map->size());
  for (const auto& [key, reply] : *reply_map) {
    T value;
    ASSIGN_OR_RETURN(value, ConvertTo<T>(reply));
    converted_map.emplace(std::move(key), std::move(value));
  }
  *to = std::move(converted_map);
  return absl::OkStatus();
}

}  // namespace eglt::redis

#endif  // EGLT_REDIS_REPLY_CONVERTERS_H_