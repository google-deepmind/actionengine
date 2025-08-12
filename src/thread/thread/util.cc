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

#include "thread/util.h"

#include <atomic>

#include <absl/base/call_once.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>

namespace thread {
static std::atomic<uint32_t> last_rand32;
static absl::once_flag init_rand32_once;

static void InitRand32() {
  uint32_t seed = absl::Uniform<uint32_t>(absl::BitGen());
  // Avoid 0 which generates a sequence of 0s.
  if (seed == 0) {
    seed = 1;
  }
  last_rand32.store(seed, std::memory_order_release);
}

uint32_t Rand32() {
  // Primitive polynomial: x^32+x^22+x^2+x^1+1
  static constexpr uint32_t poly = 1 << 22 | 1 << 2 | 1 << 1 | 1 << 0;

  absl::call_once(init_rand32_once, InitRand32);
  uint32_t r = last_rand32.load(std::memory_order_relaxed);
  r = r << 1 ^ (static_cast<int32_t>(r) >> 31) & poly;
  // shift sign-extends
  last_rand32.store(r, std::memory_order_relaxed);
  return r;
}
}  // namespace thread