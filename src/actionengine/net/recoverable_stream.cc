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

#include "actionengine/net/recoverable_stream.h"

namespace act::net {
RecoverableStream::RecoverableStream(GetStreamFn get_stream,
                                     std::string_view id,
                                     absl::Duration timeout)
    : get_stream_(std::move(get_stream)),
      id_(!id.empty() ? id : GenerateUUID4()),
      timeout_(timeout),
      timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

RecoverableStream::RecoverableStream(std::unique_ptr<WireStream> stream,
                                     std::string_view id,
                                     absl::Duration timeout)
    : get_stream_([stream = std::move(stream)]() { return stream.get(); }),
      id_(!id.empty() ? id : GenerateUUID4()),
      timeout_(timeout),
      timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

RecoverableStream::RecoverableStream(std::shared_ptr<WireStream> stream,
                                     std::string_view id,
                                     absl::Duration timeout)
    : get_stream_([stream = std::move(stream)]() { return stream.get(); }),
      id_(!id.empty() ? id : GenerateUUID4()),
      timeout_(timeout),
      timeout_event_(std::make_unique<thread::PermanentEvent>()) {}

RecoverableStream::~RecoverableStream() {
  CloseAndNotify();

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

thread::Case RecoverableStream::OnTimeout() const {
  act::MutexLock lock(&mu_);
  return timeout_event_->OnEvent();
}

void RecoverableStream::RecoverAndNotify(GetStreamFn new_get_stream) {
  act::MutexLock lock(&mu_);
  if (new_get_stream) {
    get_stream_ = std::move(new_get_stream);
  }
  lost_ = false;
  timeout_event_ = std::make_unique<thread::PermanentEvent>();
  cv_.SignalAll();
}

bool RecoverableStream::IsClosed() const {
  act::MutexLock lock(&mu_);
  return closed_;
}

bool RecoverableStream::IsLost() const {
  act::MutexLock lock(&mu_);
  return lost_;
}

void RecoverableStream::CloseAndNotify() {
  act::MutexLock lock(&mu_);
  if (!half_closed_) {
    HalfCloseInternal();
  }
  // if we're here, we're either half-closed or the stream is lost and we're
  // allowed to ignore that
  closed_ = true;
  cv_.SignalAll();
}

absl::Status RecoverableStream::Send(WireMessage message) {
  act::MutexLock lock(&mu_);
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

absl::StatusOr<std::optional<WireMessage>> RecoverableStream::Receive(
    absl::Duration timeout) {
  act::MutexLock lock(&mu_);
  auto stream = GetObservedStream();
  if (!stream.ok()) {
    return std::nullopt;
  }

  concurrency::PreventExclusiveAccess pending(&finalization_guard_);
  return (*stream)->Receive(timeout);
}

absl::Status RecoverableStream::Accept() {
  act::MutexLock lock(&mu_);
  auto stream = GetObservedStream();
  if (!stream.ok()) {
    return stream.status();
  }

  concurrency::PreventExclusiveAccess pending(&finalization_guard_);
  return (*stream)->Accept();
}

absl::Status RecoverableStream::Start() {
  act::MutexLock lock(&mu_);

  auto stream = GetObservedStream();
  if (!stream.ok()) {
    return stream.status();
  }

  concurrency::PreventExclusiveAccess pending(&finalization_guard_);
  return (*stream)->Start();
}

void RecoverableStream::HalfClose() {
  act::MutexLock lock(&mu_);
  HalfCloseInternal();
}

void RecoverableStream::HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (auto stream = GetObservedStream(); stream.ok()) {
    (*stream)->HalfClose();
  }
  half_closed_ = true;
}

void RecoverableStream::Abort() {
  act::MutexLock lock(&mu_);
  if (const auto stream = get_stream_(); stream != nullptr) {
    stream->Abort();
  }
  closed_ = true;
  lost_ = false;
  half_closed_ = false;
  cv_.SignalAll();
}

absl::Status RecoverableStream::GetStatus() const {
  act::MutexLock lock(&mu_);
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

const void* RecoverableStream::GetImpl() const {
  // TODO: determine whether this method should return observed stream or
  //   (observed stream)->GetImpl(). For now, we return the latter.
  act::MutexLock lock(&mu_);
  const auto stream = get_stream_();
  if (stream == nullptr) {
    return nullptr;
  }
  return stream->GetImpl();
}

absl::StatusOr<WireStream*> RecoverableStream::GetObservedStream() {
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

}  // namespace act::net