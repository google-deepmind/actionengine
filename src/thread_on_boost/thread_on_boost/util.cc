#include "thread_on_boost/util.h"

namespace thread {

static std::atomic<int32_t> last_rand32;
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
  r = r << 1 ^ (static_cast<int32_t>(r) >> 31) & poly;  // shift sign-extends
  last_rand32.store(r, std::memory_order_relaxed);
  return r;
}

}  // namespace thread