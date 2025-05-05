#ifndef EGLT_NET_RECOVERABLE_STREAM_H_
#define EGLT_NET_RECOVERABLE_STREAM_H_

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/net/stream.h"
#include "eglt/util/random.h"

namespace eglt::net {

using GetStreamFn = absl::AnyInvocable<EvergreenStream*() const>;

class RecoverableStream final : public eglt::EvergreenStream {
 public:
  explicit RecoverableStream(GetStreamFn get_stream, std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration())
      : get_stream_(std::move(get_stream)),
        id_(!id.empty() ? id : GenerateUUID4()),
        timeout_(timeout),
        timeout_event_(std::make_unique<concurrency::PermanentEvent>()) {}

  ~RecoverableStream() override {
    {
      concurrency::MutexLock lock(&mutex_);
      if (closed_) {
        return;
      }
    }
    CloseAndNotify(/*ignore_lost=*/false);
  }

  [[nodiscard]] concurrency::Case OnTimeout() const {
    concurrency::MutexLock lock(&mutex_);
    return timeout_event_->OnEvent();
  }

  void RecoverAndNotify(GetStreamFn new_get_stream = {}) {
    concurrency::MutexLock lock(&mutex_);
    if (new_get_stream) {
      get_stream_ = std::move(new_get_stream);
    }
    lost_ = false;
    timeout_event_ = std::make_unique<concurrency::PermanentEvent>();
    cv_.SignalAll();
  }

  [[nodiscard]] bool IsClosed() const {
    concurrency::MutexLock lock(&mutex_);
    return closed_;
  }

  [[nodiscard]] bool IsLost() const {
    concurrency::MutexLock lock(&mutex_);
    return lost_;
  }

  void CloseAndNotify(bool ignore_lost = false) {
    concurrency::MutexLock lock(&mutex_);
    CHECK(!closed_) << "Cannot close stream, it is already closed";
    if (!half_closed_) {
      if (get_stream_() != nullptr) {
        LOG(FATAL)
            << "Calling CloseAndNotify() on a stream that is not half-closed";
        return;
      }
      if (!ignore_lost) {
        LOG(FATAL)
            << "Cannot close stream: the underlying stream is lost, we haven't "
               "been half-closed, and ignore_lost is false";
        return;
      }
    }
    // if we're here, we're either half-closed or the stream is lost and we're
    // allowed to ignore that
    closed_ = true;
    cv_.SignalAll();
  }

  absl::Status Send(SessionMessage message) override {
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }
    return (*stream)->Send(std::move(message));
  }

  std::optional<SessionMessage> Receive() override {
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return std::nullopt;
    }
    return (*stream)->Receive();
  }

  absl::Status Accept() override {
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }
    return (*stream)->Accept();
  }

  absl::Status Start() override {
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }
    return (*stream)->Start();
  }

  void HalfClose() override {
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      LOG(ERROR) << "Trying to half-close unavailable stream: "
                 << stream.status();
      return;
    }

    concurrency::MutexLock lock(&mutex_);
    (*stream)->HalfClose();
    half_closed_ = true;
  }

  [[nodiscard]] absl::Status GetStatus() const override {
    concurrency::MutexLock lock(&mutex_);
    if (closed_) {
      return absl::UnavailableError("Recoverable stream is closed.");
    }
    if (lost_) {
      return absl::UnavailableError("Recoverable stream is currently lost.");
    }

    // In this method, we always want to return the momentary status of a stream,
    // so if the underlying stream is suddenly unavailable, we don't want to
    // block waiting for it to be available again. It is a possibility that
    // stream == nullptr, but neither closed_ nor lost_ is (yet) true, because
    // get_stream_'s behaviour is completely external and our methods might
    // not have been called yet to set those flags. It's okay; we just return
    // the status of the stream, which is rightfully "unavailable".
    const auto stream = get_stream_();
    if (stream == nullptr) {
      return absl::UnavailableError(
          "Recoverable stream is unavailable for an unknown reason (not "
          "flagged closed or lost).");
    }
    return stream->GetStatus();
  }

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override {
    // TODO: determine whether this method should return observed stream or
    //   (observed stream)->GetImpl(). For now, we return the latter.
    concurrency::MutexLock lock(&mutex_);
    const auto stream = get_stream_();
    if (stream == nullptr) {
      return nullptr;
    }
    return stream->GetImpl();
  }

 private:
  absl::StatusOr<EvergreenStream*> GetObservedStream()
      ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);

    if (closed_) {
      return absl::UnavailableError("Recoverable stream is closed");
    }

    // get_stream_ returns a valid stream, just return it
    EvergreenStream* stream = get_stream_();
    if (stream != nullptr) {
      return stream;
    }

    // wait for the stream to be available
    lost_ = true;
    if (cv_.WaitWithTimeout(&mutex_, timeout_)) {
      timeout_event_->Notify();
    }

    if (closed_) {
      return absl::UnavailableError("Recoverable stream is closed");
    }
    stream = get_stream_();
    if (stream == nullptr) {
      return absl::DeadlineExceededError(
          "Recoverable stream is not available after waiting for timeout");
    }
    lost_ = false;
    return stream;
  }

  GetStreamFn get_stream_ ABSL_GUARDED_BY(mutex_);
  const std::string id_;
  const absl::Duration timeout_{absl::InfiniteDuration()};

  bool half_closed_ ABSL_GUARDED_BY(mutex_){false};
  bool closed_ ABSL_GUARDED_BY(mutex_){false};
  bool lost_ ABSL_GUARDED_BY(mutex_){false};
  std::unique_ptr<concurrency::PermanentEvent> timeout_event_
      ABSL_GUARDED_BY(mutex_);

  mutable concurrency::Mutex mutex_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace eglt::net

#endif  // EGLT_NET_RECOVERABLE_STREAM_H_