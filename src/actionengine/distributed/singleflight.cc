#include "actionengine/distributed/singleflight.h"

namespace act::distributed {

WaitGroup::WaitGroup(WaitGroup&& other) noexcept {
  act::MutexLock lock(&other.mu_);

  counter_ = other.counter_;
  other.counter_ = 0;

  other.cv_.SignalAll();
}

WaitGroup& WaitGroup::operator=(WaitGroup&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  act::concurrency::TwoMutexLock lock1(&mu_, &other.mu_);

  counter_ = other.counter_;
  other.counter_ = 0;
  other.cv_.SignalAll();

  return *this;
}

WaitGroup::~WaitGroup() {
  act::MutexLock lock(&mu_);
  while (counter_ > 0) {
    cv_.Wait(&mu_);
  }
}

void WaitGroup::Add(int delta) {
  act::MutexLock lock(&mu_);
  counter_ += delta;
}

void WaitGroup::Done() {
  act::MutexLock lock(&mu_);
  --counter_;
  if (counter_ <= 0) {
    cv_.SignalAll();
  }
}

void WaitGroup::Wait() {
  act::MutexLock lock(&mu_);
  while (counter_ > 0) {
    cv_.Wait(&mu_);
  }
}

absl::StatusOr<std::any> FlightGroup::Do(
    std::string_view key, absl::AnyInvocable<absl::StatusOr<std::any>()> fn) {
  act::MutexLock lock(&mu_);

  if (const auto it = calls_.find(key); it != calls_.end()) {
    // Another goroutine is already doing this work.
    std::shared_ptr<Call> call = it->second;
    mu_.unlock();
    call->wg.Wait();
    mu_.lock();
    return call->val;
  }

  auto call = std::make_shared<Call>();
  call->wg.Add(1);

  // Save a reference to the call while we do the work.
  const auto call_it = calls_.emplace(key, std::move(call)).first;

  mu_.unlock();
  call_it->second->val = fn();
  call_it->second->wg.Done();
  mu_.lock();

  const auto map_node = calls_.extract(call_it);
  return map_node.mapped()->val;
}

}  // namespace act::distributed