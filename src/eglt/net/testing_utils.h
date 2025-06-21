#ifndef EGLT_NET_TESTING_UTILS_H_
#define EGLT_NET_TESTING_UTILS_H_

#include "eglt/absl_headers.h"
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
      concurrency::MutexLock lock(&mu_);
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
    concurrency::MutexLock lock(&mu_);
    if (half_closed_) {
      return absl::CancelledError("Stream is half-closed");
    }
    if (partner_) {
      return partner_->Feed(std::move(message), this);
    }
    return absl::FailedPreconditionError(
        "Cannot send message, partner stream is not set");
  }

  std::optional<SessionMessage> Receive() override {
    SessionMessage message;
    if (recv_queue_.reader()->Read(&message)) {
      return message;
    }
    return std::nullopt;
  }

  absl::Status Start() override {
    concurrency::MutexLock lock(&mu_);
    if (!partner_) {
      return absl::FailedPreconditionError(
          "Cannot call Start(), partner stream is not set");
    }
    return partner_->AcknowledgeStart();
  }

  absl::Status Accept() override {
    concurrency::MutexLock lock(&mu_);
    if (partner_ == nullptr) {
      return absl::FailedPreconditionError(
          "Cannot call Accept(), partner stream is not set");
    }
    return partner_->AcknowledgeAccept();
  }

  void HalfClose() override {
    PairableInMemoryStream* partner = nullptr;
    {
      concurrency::MutexLock lock(&mu_);
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
      partner->HalfClose();
    }

    recv_queue_.writer()->Close();
  }

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override { return nullptr; }

  [[nodiscard]] absl::Status GetStatus() const override {
    concurrency::MutexLock lock(&mu_);
    if (half_closed_) {
      return absl::CancelledError("Stream is half-closed");
    }
    return absl::OkStatus();
  }

 private:
  absl::Status Feed(SessionMessage message,
                    const PairableInMemoryStream* absl_nonnull from) {
    concurrency::MutexLock lock(&mu_);

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

  mutable concurrency::Mutex mu_;

  std::string id_;
  PairableInMemoryStream* partner_ ABSL_GUARDED_BY(mu_){nullptr};

  bool half_closed_ ABSL_GUARDED_BY(mu_){false};
  concurrency::Channel<SessionMessage> recv_queue_{128};
};

}  // namespace eglt::testing

#endif  // EGLT_NET_TESTING_UTILS_H_