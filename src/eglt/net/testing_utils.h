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

#ifndef EGLT_NET_TESTING_UTILS_H_
#define EGLT_NET_TESTING_UTILS_H_

#include "eglt/concurrency/concurrency.h"
#include "eglt/net/stream.h"

namespace eglt::testing {

class PairableInMemoryStream final : public WireStream {
 public:
  explicit PairableInMemoryStream(std::string_view id = "") : id_(id) {}

  ~PairableInMemoryStream() override {
    CHECK(half_closed_) << "Cannot destroy stream, it is not half-closed";
  }

  void PairWith(PairableInMemoryStream* partner) {
    {
      eglt::MutexLock lock(&mu_);
      if (partner_ != nullptr) {
        LOG(FATAL) << "Cannot pair with another stream, already paired.";
        ABSL_ASSUME(false);
      }
    }

    if (partner == nullptr) {
      LOG(FATAL) << "Cannot pair with a null stream.";
      ABSL_ASSUME(false);
    }
    if (partner == this) {
      LOG(FATAL) << "Cannot pair with itself.";
      ABSL_ASSUME(false);
    }
    concurrency::TwoMutexLock lock(&mu_, &partner->mu_);
    partner_ = partner;
    partner->partner_ = this;
  }

  absl::Status Send(SessionMessage message) override {
    eglt::MutexLock lock(&mu_);
    if (half_closed_) {
      return absl::CancelledError("Stream is half-closed");
    }
    if (partner_) {
      return partner_->Feed(std::move(message), this);
    }
    return absl::FailedPreconditionError(
        "Cannot send message, partner stream is not set");
  }

  absl::StatusOr<std::optional<SessionMessage>> Receive(
      absl::Duration timeout) override {
    SessionMessage message;
    bool ok;
    if (thread::SelectUntil(absl::Now() + timeout,
                            {recv_queue_.reader()->OnRead(&message, &ok)}) ==
        -1) {
      return absl::DeadlineExceededError("Receive operation timed out.");
    }
    if (!ok) {
      return std::nullopt;
    }
    return message;
  }

  thread::Case OnReceive(std::optional<SessionMessage>* absl_nonnull message,
                         absl::Status* absl_nonnull status) override {
    return thread::NonSelectableCase();
  }

  absl::Status Start() override {
    eglt::MutexLock lock(&mu_);
    if (!partner_) {
      return absl::FailedPreconditionError(
          "Cannot call Start(), partner stream is not set");
    }
    return partner_->AcknowledgeStart();
  }

  absl::Status Accept() override {
    eglt::MutexLock lock(&mu_);
    if (partner_ == nullptr) {
      return absl::FailedPreconditionError(
          "Cannot call Accept(), partner stream is not set");
    }
    return partner_->AcknowledgeAccept();
  }

  absl::Status HalfClose() override {
    PairableInMemoryStream* partner = nullptr;
    absl::Status status;
    {
      eglt::MutexLock lock(&mu_);
      partner = partner_;
      partner_ = nullptr;

      CHECK(!half_closed_)
          << "Cannot HalfClose(), stream is already half-closed";
      half_closed_ = true;
    }

    if (partner != nullptr) {
      {
        concurrency::TwoMutexLock lock(&mu_, &partner->mu_);
        partner->partner_ = nullptr;
      }
      status = partner->HalfClose();
    }

    recv_queue_.writer()->Close();
    return status;
  }

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override { return nullptr; }

  [[nodiscard]] absl::Status GetStatus() const override {
    eglt::MutexLock lock(&mu_);
    if (half_closed_) {
      return absl::CancelledError("Stream is half-closed");
    }
    return absl::OkStatus();
  }

 private:
  absl::Status Feed(SessionMessage message,
                    const PairableInMemoryStream* absl_nonnull from) {
    eglt::MutexLock lock(&mu_);

    CHECK(from != nullptr) << "Cannot feed message from null stream";
    CHECK(from == partner_) << "Cannot feed message from non-partner stream";

    if (!recv_queue_.writer()->WriteUnlessCancelled(std::move(message))) {
      return absl::CancelledError("Stream closed");
    }
    return absl::OkStatus();
  }

  absl::Status AcknowledgeAccept() ABSL_LOCKS_EXCLUDED(mu_) {
    return absl::OkStatus();
  }

  absl::Status AcknowledgeStart() ABSL_LOCKS_EXCLUDED(mu_) {
    return absl::OkStatus();
  }

  mutable eglt::Mutex mu_;

  std::string id_;
  PairableInMemoryStream* partner_ ABSL_GUARDED_BY(mu_){nullptr};

  bool half_closed_ ABSL_GUARDED_BY(mu_){false};
  thread::Channel<SessionMessage> recv_queue_{128};
};

}  // namespace eglt::testing

#endif  // EGLT_NET_TESTING_UTILS_H_