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

#ifndef ACTIONENGINE_NET_RECOVERABLE_STREAM_H_
#define ACTIONENGINE_NET_RECOVERABLE_STREAM_H_

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/stream.h"
#include "actionengine/util/random.h"

namespace act::net {

using GetStreamFn = absl::AnyInvocable<WireStream*() const>;

class RecoverableStream final : public act::WireStream {
 public:
  static constexpr auto kFinalizationTimeout = absl::Seconds(5);

  explicit RecoverableStream(GetStreamFn get_stream, std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration());

  explicit RecoverableStream(std::unique_ptr<WireStream> stream = nullptr,
                             std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration());

  explicit RecoverableStream(std::shared_ptr<WireStream> stream,
                             std::string_view id = "",
                             absl::Duration timeout = absl::InfiniteDuration());

  ~RecoverableStream() override;

  [[nodiscard]] thread::Case OnTimeout() const;

  void RecoverAndNotify(GetStreamFn new_get_stream = {});

  [[nodiscard]] bool IsClosed() const;

  [[nodiscard]] bool IsLost() const;

  void CloseAndNotify();

  absl::Status Send(WireMessage message) override;

  absl::StatusOr<std::optional<WireMessage>> Receive(
      absl::Duration timeout) override;

  absl::Status Accept() override;

  absl::Status Start() override;

  void HalfClose() override;

  void Abort() override;

  [[nodiscard]] absl::Status GetStatus() const override;

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override;

 private:
  absl::StatusOr<WireStream*> GetObservedStream()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  GetStreamFn get_stream_ ABSL_GUARDED_BY(mu_);
  const std::string id_;
  const absl::Duration timeout_{absl::InfiniteDuration()};

  bool half_closed_ ABSL_GUARDED_BY(mu_){false};
  bool closed_ ABSL_GUARDED_BY(mu_){false};
  bool lost_ ABSL_GUARDED_BY(mu_){false};
  std::unique_ptr<thread::PermanentEvent> timeout_event_ ABSL_GUARDED_BY(mu_);

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);
  concurrency::ExclusiveAccessGuard finalization_guard_{&mu_, &cv_};
};

}  // namespace act::net

#endif  // ACTIONENGINE_NET_RECOVERABLE_STREAM_H_