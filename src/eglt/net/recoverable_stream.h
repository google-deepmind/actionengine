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

#ifndef EGLT_NET_RECOVERABLE_STREAM_H_
#define EGLT_NET_RECOVERABLE_STREAM_H_

#include "eglt/concurrency/concurrency.h"
#include "eglt/net/stream.h"
#include "eglt/util/random.h"

namespace eglt::net {

using GetStreamFn = absl::AnyInvocable<WireStream*() const>;

class RecoverableStream final : public eglt::WireStream {
 public:
  static constexpr auto kFinalizationTimeout = absl::Seconds(5);

  explicit RecoverableStream(GetStreamFn get_stream, std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration())
      : get_stream_(std::move(get_stream)),
        id_(!id.empty() ? id : GenerateUUID4()),
        timeout_(timeout),
        timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

  explicit RecoverableStream(std::unique_ptr<WireStream> stream = nullptr,
                             std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration())
      : get_stream_([stream = std::move(stream)]() { return stream.get(); }),
        id_(!id.empty() ? id : GenerateUUID4()),
        timeout_(timeout),
        timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

  explicit RecoverableStream(std::shared_ptr<WireStream> stream,
                             std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration())
      : get_stream_([stream = std::move(stream)]() { return stream.get(); }),
        id_(!id.empty() ? id : GenerateUUID4()),
        timeout_(timeout),
        timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

  ~RecoverableStream() override {
    CloseAndNotify(/*ignore_lost=*/false);

    concurrency::EnsureExclusiveAccess waiter(&finalization_guard_,
                                              absl::Now() + timeout_);

    if (waiter.TimedOut()) {
      LOG(ERROR) << "Recoverable stream's senders or receivers are still "
                    "pending after "
                    "timeout";
    }
    CHECK(closed_)
        << "Recoverable stream is not closed after waiting for pending IO";
  }

  [[nodiscard]] thread::Case OnTimeout() const {
    eglt::MutexLock lock(&mu_);
    return timeout_event_->OnEvent();
  }

  void RecoverAndNotify(GetStreamFn new_get_stream = {}) {
    eglt::MutexLock lock(&mu_);
    if (new_get_stream) {
      get_stream_ = std::move(new_get_stream);
    }
    lost_ = false;
    timeout_event_ = std::make_unique<thread::PermanentEvent>();
    cv_.SignalAll();
  }

  [[nodiscard]] bool IsClosed() const {
    eglt::MutexLock lock(&mu_);
    return closed_;
  }

  [[nodiscard]] bool IsLost() const {
    eglt::MutexLock lock(&mu_);
    return lost_;
  }

  void CloseAndNotify(bool ignore_lost = false) {
    eglt::MutexLock lock(&mu_);
    CHECK(!closed_) << "Cannot close stream, it is already closed";
    if (!half_closed_) {
      if (get_stream_() != nullptr) {
        LOG(WARNING)
            << "Calling CloseAndNotify() on a stream that is not half-closed";
        return;
      }
      if (!ignore_lost) {
        LOG(FATAL)
            << "Cannot close stream: the underlying stream is lost, we haven't "
               "been half-closed, and ignore_lost is false";
        ABSL_ASSUME(false);
      }
    }
    // if we're here, we're either half-closed or the stream is lost and we're
    // allowed to ignore that
    closed_ = true;
    cv_.SignalAll();
  }

  absl::Status Send(SessionMessage message) override {
    eglt::MutexLock lock(&mu_);
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }

    if (half_closed_) {
      return absl::FailedPreconditionError(
          "Cannot send message on a half-closed stream");
    }

    concurrency::PreventExclusiveAccess pending(&finalization_guard_);
    absl::Status status = (*stream)->Send(std::move(message));
    return status;
  }

  std::optional<SessionMessage> Receive() override {
    eglt::MutexLock lock(&mu_);
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return std::nullopt;
    }

    concurrency::PreventExclusiveAccess pending(&finalization_guard_);
    return (*stream)->Receive();
  }

  thread::Case OnReceive(std::optional<SessionMessage>* absl_nonnull message,
                         absl::Status* absl_nonnull status) override {
    return thread::NonSelectableCase();
  }

  absl::Status Accept() override {
    eglt::MutexLock lock(&mu_);
    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }

    concurrency::PreventExclusiveAccess pending(&finalization_guard_);
    return (*stream)->Accept();
  }

  absl::Status Start() override {
    eglt::MutexLock lock(&mu_);

    auto stream = GetObservedStream();
    if (!stream.ok()) {
      return stream.status();
    }

    concurrency::PreventExclusiveAccess pending(&finalization_guard_);
    return (*stream)->Start();
  }

  absl::Status HalfClose() override {
    eglt::MutexLock lock(&mu_);

    auto stream = GetObservedStream();
    if (!stream.ok()) {
      LOG(ERROR) << "Trying to half-close unavailable stream: "
                 << stream.status();
      return stream.status();
    }
    if (auto s = (*stream)->HalfClose(); !s.ok()) {
      return s;
    }
    half_closed_ = true;
    return absl::OkStatus();
  }

  void OnHalfClose(absl::AnyInvocable<void(WireStream*)> fn) override {
    eglt::MutexLock lock(&mu_);

    auto stream = GetObservedStream();
    if (!stream.ok()) {
      LOG(ERROR) << "Trying to set half-close callback on unavailable stream: "
                 << stream.status();
      return;
    }

    CHECK(*stream != nullptr)
        << "Cannot set half-close callback on a null stream";

    (*stream)->OnHalfClose(std::move(fn));
  }

  [[nodiscard]] absl::Status GetStatus() const override {
    eglt::MutexLock lock(&mu_);
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

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override {
    // TODO: determine whether this method should return observed stream or
    //   (observed stream)->GetImpl(). For now, we return the latter.
    eglt::MutexLock lock(&mu_);
    const auto stream = get_stream_();
    if (stream == nullptr) {
      return nullptr;
    }
    return stream->GetImpl();
  }

 private:
  absl::StatusOr<WireStream*> GetObservedStream()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (closed_) {
      return absl::UnavailableError("Recoverable stream is closed");
    }

    // get_stream_ returns a valid stream, just return it
    WireStream* stream = get_stream_();
    if (stream != nullptr) {
      return stream;
    }

    // wait for the stream to be available
    lost_ = true;
    if (cv_.WaitWithTimeout(&mu_, timeout_)) {
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

  GetStreamFn get_stream_ ABSL_GUARDED_BY(mu_);
  const std::string id_;
  const absl::Duration timeout_{absl::InfiniteDuration()};

  bool half_closed_ ABSL_GUARDED_BY(mu_){false};
  bool closed_ ABSL_GUARDED_BY(mu_){false};
  bool lost_ ABSL_GUARDED_BY(mu_){false};
  std::unique_ptr<thread::PermanentEvent> timeout_event_ ABSL_GUARDED_BY(mu_);

  mutable eglt::Mutex mu_;
  mutable eglt::CondVar cv_ ABSL_GUARDED_BY(mu_);
  concurrency::ExclusiveAccessGuard finalization_guard_{&mu_, &cv_};
};

}  // namespace eglt::net

#endif  // EGLT_NET_RECOVERABLE_STREAM_H_