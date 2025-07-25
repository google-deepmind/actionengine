#include "eglt/redis/chunk_store.h"

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

}  // namespace eglt::redis