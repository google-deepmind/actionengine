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

#include <cstddef>
#include <cstdint>
#include <latch>

#include "g3_fiber/fiber.h"
#include "g3_fiber/thread_pool.h"
#include "g3_fiber/util.h"

namespace thread {

void WorkerThreadPool::Start(size_t num_threads) {
  eglt::concurrency::impl::MutexLock lock(&mu_);
  schedulers_.resize(num_threads);
  std::latch latch(num_threads);
  for (size_t idx = 0; idx < num_threads; ++idx) {
    Worker worker{
        .thread = std::thread([this, idx, &latch] {
          EnsureThreadHasScheduler<boost::fibers::algo::shared_work>(
              /*suspend=*/true);
          schedulers_[idx] = boost::fibers::context::active()->get_scheduler();
          latch.count_down();
          // TODO: cancellation
          while (!Cancelled()) {
            boost::fibers::context::active()->suspend();
          }
          DLOG(INFO) << absl::StrFormat("Worker %zu exiting.", idx);
        }),
    };
    workers_.push_back(std::move(worker));
  }
  latch.wait();

  for (const auto& scheduler : schedulers_) {
    scheduler_set_.insert(scheduler);
  }

  for (auto& [thread] : workers_) {
    thread.detach();
  }
}

void WorkerThreadPool::Schedule(boost::fibers::context* ctx) {
  if (schedule_only_through_same_thread_ &&
      !allow_schedule_through_same_thread_) {
    LOG(FATAL) << "Cannot schedule (only) through the same thread when "
                  "allow_schedule_through_same_thread is false.";
    ABSL_ASSUME(false);
  }

  const auto active_ctx = boost::fibers::context::active();
  boost::fibers::scheduler* scheduler = active_ctx->get_scheduler();
  const bool scheduling_from_within_pool = scheduler_set_.contains(scheduler);

  if (!allow_schedule_through_same_thread_ && schedulers_.size() == 1 &&
      scheduling_from_within_pool) {
    LOG(FATAL)
        << "Cannot schedule through different threads when there is only "
           "one thread in the pool.";
    ABSL_ASSUME(false);
  }

  if (!schedule_only_through_same_thread_ || !scheduling_from_within_pool) {
    size_t worker_idx =
        next_worker_idx_.fetch_add(1, std::memory_order_relaxed) %
        workers_.size();
    scheduler = schedulers_[worker_idx];

    if (!allow_schedule_through_same_thread_) {
      while (schedulers_[worker_idx] == scheduler) {
        worker_idx = Rand32() % workers_.size();
      }
      scheduler = schedulers_[worker_idx];
    }
  }

  if (scheduler == active_ctx->get_scheduler()) {
    active_ctx->attach(ctx);
    scheduler->schedule(ctx);
  } else {
    {
      eglt::concurrency::impl::MutexLock lock(&mu_);
      scheduler->attach_worker_context(ctx);
      scheduler->schedule_from_remote(ctx);
    }
  }
}

WorkerThreadPool& WorkerThreadPool::Instance() {
  static WorkerThreadPool instance;
  if (instance.workers_.empty()) {
    instance.Start();
  }
  return instance;
}

static absl::once_flag kInitWorkerThreadPoolFlag;

void EnsureWorkerThreadPool() {
  absl::call_once(kInitWorkerThreadPoolFlag, WorkerThreadPool::Instance);
}

}  // namespace thread