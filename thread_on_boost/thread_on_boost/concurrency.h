#ifndef THREAD_ON_BOOST_CONCURRENCY_H_
#define THREAD_ON_BOOST_CONCURRENCY_H_

#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/channel.h"
#include "thread_on_boost/fiber.h"
#include "thread_on_boost/select.h"
#include "thread_on_boost/selectables.h"

namespace eglt::concurrency::impl {

using Mutex = thread::Mutex;
using MutexLock = thread::MutexLock;
using CondVar = thread::CondVar;

using PermanentEvent = thread::PermanentEvent;
using Case = thread::Case;
using CaseArray = thread::CaseArray;
using TreeOptions = thread::TreeOptions;
using Fiber = thread::Fiber;

inline void SleepFor(absl::Duration duration) {
  boost::fibers::context* active_ctx = boost::fibers::context::active();
  active_ctx->wait_until(
      std::chrono::steady_clock::now() +
      absl::time_internal::ToChronoDuration<std::chrono::seconds>(duration));
}

template <typename T>
class Channel;

template <typename T>
class ChannelReader {
 public:
  ChannelReader() : impl_(nullptr) {}
  explicit ChannelReader(thread::Reader<T>* reader) : impl_(reader) {}

  bool Read(T* item) { return impl_->Read(item); }
  Case OnRead(T* item, bool* ok) { return impl_->OnRead(item, ok); }

 private:
  void SetImpl(thread::Reader<T>* impl) { impl_ = impl; }
  friend class Channel<T>;

  thread::Reader<T>* impl_;
};

template <typename T>
class ChannelWriter {
 public:
  ChannelWriter<T>() : impl_(nullptr) {}
  explicit ChannelWriter(thread::Writer<T>* writer) : impl_(writer) {}

  void Write(const T& item) { impl_->Write(item); }
  void Write(T&& item) { impl_->Write(std::move(item)); }

  bool WriteUnlessCancelled(const T& item) {
    return impl_->WriteUnlessCancelled(item);
  }
  bool WriteUnlessCancelled(T&& item) {
    return impl_->WriteUnlessCancelled(std::move(item));
  }

  void Close() { impl_->Close(); }

 private:
  void SetImpl(thread::Writer<T>* impl) { impl_ = impl; }
  friend class Channel<T>;

  thread::Writer<T>* impl_;
};

template <typename T>
class Channel {
 public:
  explicit Channel(std::unique_ptr<thread::Channel<T>> impl)
      : impl_(std::move(impl)) {
    reader_.SetImpl(impl_->reader());
    writer_.SetImpl(impl_->writer());
  }
  explicit Channel(size_t buffer_size)
      : impl_(std::make_unique<thread::Channel<T>>(buffer_size)) {
    reader_.SetImpl(impl_->reader());
    writer_.SetImpl(impl_->writer());
  }

  ChannelReader<T>* GetReader() { return &reader_; }

  ChannelWriter<T>* GetWriter() { return &writer_; }

  [[nodiscard]] size_t Size() const { return impl_->length(); }

 private:
  std::unique_ptr<thread::Channel<T>> impl_;

  ChannelReader<T> reader_;
  ChannelWriter<T> writer_;
};

inline bool Cancelled() {
  return thread::Cancelled();
}

inline Case OnCancel() {
  return thread::OnCancel();
}

inline void JoinOptimally(Fiber* fiber) {
  fiber->Join();
}

inline int Select(const CaseArray& cases) {
  return thread::Select(cases);
}

inline int SelectUntil(const absl::Time deadline, const CaseArray& cases) {
  return thread::SelectUntil(deadline, cases);
}

inline void Detach(const TreeOptions& options,
                   absl::AnyInvocable<void()>&& fn) {
  thread::Detach(options, std::move(fn));
}

inline std::unique_ptr<Fiber> NewTree(const TreeOptions& options,
                                      absl::AnyInvocable<void()>&& fn) {
  return thread::NewTree(options, std::move(fn));
}

}  // namespace eglt::concurrency::impl

#endif  // THREAD_ON_BOOST_CONCURRENCY_H_
