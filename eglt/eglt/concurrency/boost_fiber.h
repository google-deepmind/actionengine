#ifndef EGLT_CONCURRENCY_BOOST_FIBER_H_
#define EGLT_CONCURRENCY_BOOST_FIBER_H_

#define __EGLT_CONCURRENCY_IMPLEMENTATION__
#define __EGLT_CONCURRENCY_IMPLEMENTATION_BOOST_FIBER__

#include <cstddef>
#include <memory>
#include <type_traits>

#include <boost/fiber/all.hpp>

#include "eglt/absl_headers.h"

namespace eglt::concurrency::impl {
using cv_status = boost::fibers::cv_status;

static std::atomic<int32_t> last_rand32;
static absl::once_flag init_rand32_once;

static void InitRand32() {
  // GoogleOnceInit is an acquire barrier on remote-cpus.
  uint32_t seed = absl::Uniform<uint32_t>(absl::BitGen());
  // Avoid 0 which generates a sequence of 0s.
  if (seed == 0) seed = 1;
  last_rand32.store(seed, std::memory_order_release);
}

// Pseudo-random number generator using Linear Shift Feedback Register (LSFB)
static uint32_t Rand32() {
  // Primitive polynomial: x^32+x^22+x^2+x^1+1
  static constexpr uint32_t poly = (1 << 22) | (1 << 2) | (1 << 1) | (1 << 0);

  absl::call_once(init_rand32_once, InitRand32);
  uint32_t r = last_rand32.load(std::memory_order_relaxed);
  r = (r << 1) ^
    ((static_cast<int32_t>(r) >> 31) & poly); // shift sign-extends
  last_rand32.store(r, std::memory_order_relaxed);
  return r;
}

class ABSL_LOCKABLE ABSL_ATTRIBUTE_WARN_UNUSED Mutex {
public:
  Mutex();
  ~Mutex();

  void Lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); }
  void Unlock() ABSL_UNLOCK_FUNCTION() { mu_.unlock(); }

  friend class CondVar;

private:
  boost::fibers::mutex& GetImpl() { return mu_; }
  boost::fibers::mutex mu_;
};

class ABSL_SCOPED_LOCKABLE MutexLock {
public:
  explicit MutexLock(Mutex* absl_nonnull mu) ABSL_EXCLUSIVE_LOCK_FUNCTION(mu)
    : mu_(mu) { this->mu_->Lock(); }

  MutexLock(const MutexLock&) = delete; // NOLINT(runtime/mutex)
  MutexLock(MutexLock&&) = delete; // NOLINT(runtime/mutex)
  MutexLock& operator=(const MutexLock&) = delete;
  MutexLock& operator=(MutexLock&&) = delete;

  ~MutexLock() ABSL_UNLOCK_FUNCTION() { this->mu_->Unlock(); }

private:
  Mutex* absl_nonnull const mu_;
};

class CondVar {
public:
  CondVar();

  void Wait(Mutex* absl_nonnull mu) { return cv_.wait(mu->GetImpl()); }

  bool WaitWithTimeout(Mutex* absl_nonnull mu, const absl::Duration timeout) {
    const absl::Time deadline = absl::Now() + timeout;
    return WaitWithDeadline(mu, deadline);
  }

  bool WaitWithDeadline(Mutex* absl_nonnull mu, const absl::Time deadline) {
    const cv_status status = cv_.wait_until(mu->GetImpl(),
                                            absl::ToChronoTime(deadline));
    if (status == cv_status::timeout) { return false; }
    return true;
  }

  void Signal() { cv_.notify_one(); }

  void SignalAll() { cv_.notify_all(); }

private:
  boost::fibers::condition_variable_any cv_;
  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;
};

struct Selector {
  CondVar cv;
  Mutex mutex;
  int picked;

  static constexpr int kNonePicked = -1;
};

class Selectable;

struct Case {
  Selectable* selectable = nullptr;
  Selector* selector = nullptr;
  int selection_index = -1;
};

struct CaseState;

class Selectable {
public:
  virtual ~Selectable() = default;

  virtual bool Handle(CaseState* c, bool enqueue) = 0;
  virtual void Unregister(CaseState* c) = 0;
};

struct CaseState {
  const Case* params;
  int index;
  Selector* selector;
  CaseState* prev;
  CaseState* next;

  [[nodiscard]] bool Pick() const {
    if (selector->picked != Selector::kNonePicked) { return false; }

    selector->picked = index;
    selector->cv.Signal();

    return true;
  }
};

class PermanentEvent final : public Selectable {
public:
  PermanentEvent() = default;

  void Notify() {
    MutexLock lock(&mutex_);
    notified_ = true;
    cv_.SignalAll();
  }

  bool HasBeenNotified() const {
    MutexLock lock(&mutex_);
    return notified_;
  }

  Case OnEvent() const {
    Case c;
    c.selectable = const_cast<PermanentEvent*>(this);
    return c;
  }

  bool Handle(CaseState* case_state, bool enqueue) override {
    MutexLock event_lock(&mutex_);
    MutexLock selector_lock(&case_state->selector->mutex);

    if (notified_) { return case_state->Pick(); }

    return false;
  }

  void Unregister(CaseState* case_state) override {
    MutexLock event_lock(&mutex_);
  }

private:
  mutable Mutex mutex_;
  CondVar cv_;
  bool notified_ ABSL_GUARDED_BY(mutex_) = false;
};

typedef absl::InlinedVector<Case, 4> CaseArray;

class TreeOptions {};

template <typename T>
class ChannelReader {
  // Implementers of this class should ideally mimic google3's thread::Reader.
  // The required behaviours are described in method comments below.
public:
  // Block until either
  // (a) the next item has been read into *item; returns true.
  // (b) Close() has been called on the corresponding writer and
  //     all values have been consumed; returns false.
  //
  // When supported, Read() will move into *item.
  bool Read(T* item [[maybe_unused]]) { return false; }

  // Returns a Case (to pass to Select()) that will finish by either
  // (a) consuming the next item into *item and storing true in *ok, or
  // (b) storing false in *ok to indicate that the channel has been closed and
  //     all values have been consumed.
  //
  // The objects pointed to by both "item" and "ok" must outlive any call to
  // Select using this case.
  //
  // When supported, OnRead() will move into *item.
  Case OnRead(T* item [[maybe_unused]], bool* ok [[maybe_unused]]) {
    return {};
  }
};

template <typename T>
class ChannelWriter {
  // Implementers of this class should ideally mimic google3's thread::Writer.
  // The required behaviours are described in method comments below.
public:
  // Blocks until the channel is able to accept a value (for a description of
  // buffering see Channel()) and writes "item" to the channel.
  //
  // See thread::SelectUntil in select.h for how to write only if there's space.
  //
  // If "item" is a temporary or the result of calling std::move, it will be
  // move-assigned into the buffer or a waiting reader.
  //
  // REQUIRES: May not be called after this->Close().
  void Write(const T& item [[maybe_unused]]) {}
  void Write(T&& item [[maybe_unused]]) {}

  bool WriteUnlessCancelled(const T& item [[maybe_unused]]) { return false; }
  bool WriteUnlessCancelled(T&& item [[maybe_unused]]) { return false; }

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read().
  //
  // REQUIRES: May be called at most once.
  void Close() {}
};

template <typename T>
class Channel {
  // Implementers of this class should ideally mimic google3's thread::Channel.
  // The required behaviours are described in method comments below.
public:
  static_assert(std::is_move_assignable_v<T>,
                "Channel<T> requires T to be MoveAssignable.");

  explicit Channel(size_t buffer_size [[maybe_unused]] = 0) {}

  ChannelReader<T>* GetReader() { return nullptr; }
  ChannelWriter<T>* GetWriter() { return nullptr; }

  [[nodiscard]] size_t Size() const { return 0; }
};

struct FiberProperties {
  PermanentEvent joinable_;

  bool cancelled = false;
  PermanentEvent cancelled_event_;
};

class Fiber {
public:
  template <typename F,
            // Avoid binding Fiber(const Fiber&) or Fiber(Fiber):
            typename = std::invoke_result_t<F>>
  explicit Fiber(F f [[maybe_unused]]) {}

  void Cancel() {
    FiberProperties& props = GetProperties();
    if (props.cancelled) { return; }
    props.cancelled = true;
    props.cancelled_event_.Notify();
  }

  Case OnCancel() { return GetProperties().cancelled_event_.OnEvent(); }

  void Join() { fiber_.join(); }
  Case OnJoinable() { return GetProperties().joinable_.OnEvent(); }

  friend void Detach(const TreeOptions& options,
                     absl::AnyInvocable<void()>&& fn);

private:
  FiberProperties& GetProperties() {
    return fiber_.properties<FiberProperties>();
  }

  auto WrapBodyFunction(absl::AnyInvocable<void()>&& fn) {
    return [fn = std::move(fn), props = &GetProperties()]() mutable {
      fn();
      props->joinable_.Notify();
    };
  }

  boost::fibers::fiber fiber_;
};

inline void JoinOptimally(Fiber* fiber) { fiber->Join(); }

bool Cancelled();

Case OnCancel();

inline bool CvBlock(const absl::Time deadline, Selector* sel)
ABSL_SHARED_LOCKS_REQUIRED(sel->mutex) {
  // We must first check that no notification occurred between registration
  // with Handle and reaching here.
  while (sel->picked == Selector::kNonePicked) {
    // Note that implementations of WaitGeneric below are allowed to return
    // true (indicating timeout) when racing with Signal().  To handle this we
    // re-check against sel.picked before returning expiring_index.
    if (sel->cv.WaitWithDeadline(&sel->mutex, deadline) &&
      sel->picked == Selector::kNonePicked) { return false; }
  }
  return true;
}

inline int SelectUntil(const absl::Time deadline, const CaseArray& cases) {
  Selector sel;
  sel.picked = Selector::kNonePicked;
  const int num_cases = cases.size();

  // Initialize internal representation of passed Cases
  absl::FixedArray<CaseState, 4> case_states(num_cases);

  // Use inside-out Fisher-Yates shuffle to combine initialization and
  // permutation.
  if (num_cases > 0) { case_states[0].index = 0; }
  for (int i = 1; i < num_cases; i++) {
    int swap = Rand32() % (i + 1);
    case_states[i].index = case_states[swap].index;
    case_states[swap].index = i;
  }

  const bool blocking = deadline != absl::InfinitePast();
  bool ready = false;
  int registered_limit;
  for (registered_limit = 0; registered_limit < num_cases; registered_limit++) {
    CaseState* case_state = &case_states[registered_limit];
    const Case* assoc_case = &cases[case_state->index];
    case_state->params = assoc_case;
    case_state->prev = nullptr; // Not on any list.
    case_state->selector = &sel;
    if (assoc_case->selectable->Handle(case_state, /*enqueue=*/blocking)) {
      ready = true;
      break;
    }
  }

  if (!blocking) {
    // Do not wait.  Also, no need to Unregister() any cases since
    // we passed enqueue=false to each Handle() above.
    const int picked = ready ? sel.picked : -1;
    return picked;
  }

  if (!ready) {
    const bool expirable = deadline != absl::InfiniteFuture();

    MutexLock l(&sel.mutex);
    const bool expired = !CvBlock(deadline, &sel);
    DCHECK(expirable || !expired);
    if (expired) {
      // Deadline expiry. Ensure nothing is picked from this point.
      sel.picked = num_cases;
    }
  }

  // Unregister from all events with which we are registered.  We know that
  // there was no non-blocking index and that we attempted to enqueue against
  // all cases with index smaller than registered_limit.
  for (int i = 0; i < registered_limit; i++) {
    if (CaseState* case_state = &case_states[i]; case_state->index != sel.
      picked) {
      // sel.picked was unregistered by the notifier.
      case_state->params->selectable->Unregister(case_state);
    }
  }

  // sel.picked == num_cases denotes expiry
  const int picked = sel.picked < num_cases ? sel.picked : -1;
  return picked;
}

inline int Select(const CaseArray& cases) {
  return SelectUntil(absl::InfiniteFuture(), cases);
}

inline void Detach(const TreeOptions& options,
                   absl::AnyInvocable<void()>&& fn) {}

// inlined only to avoid ODR violations.
inline std::unique_ptr<Fiber> NewTree(const TreeOptions&,
                                      absl::AnyInvocable<void()>&& fn [[
                                        maybe_unused]]) { return nullptr; }
} // namespace eglt::concurrency::impl

#endif  // EGLT_CONCURRENCY_BOOST_FIBER_H_
