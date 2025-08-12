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

#include "thread/thread_pool.h"

#include <cstddef>
#include <cstdint>

#include <absl/base/call_once.h>
#include <boost/fiber/algo/shared_work.hpp>
#include <boost/fiber/context.hpp>
#include <latch>

#include "thread/fiber.h"
#include "thread/util.h"

namespace thread {
void WorkerThreadPool::Start(size_t num_threads) {
  act::concurrency::impl::MutexLock lock(&mu_);
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

  for (auto& [thread] : workers_) {
    thread.detach();
  }
}

void WorkerThreadPool::Schedule(boost::fibers::context* ctx) {
  size_t worker_idx =
      worker_idx_.fetch_add(1, std::memory_order_relaxed) % workers_.size();

  const auto active_ctx = boost::fibers::context::active();

  if constexpr (!kScheduleOnSelf) {
    while (schedulers_[worker_idx] == active_ctx->get_scheduler()) {
      worker_idx = Rand32() % workers_.size();
    }
  }

  if (schedulers_[worker_idx] == active_ctx->get_scheduler()) {
    active_ctx->attach(ctx);
    schedulers_[worker_idx]->schedule(ctx);
  } else {
    {
      act::concurrency::impl::MutexLock lock(&mu_);
      schedulers_[worker_idx]->attach_worker_context(ctx);
      schedulers_[worker_idx]->schedule_from_remote(ctx);
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