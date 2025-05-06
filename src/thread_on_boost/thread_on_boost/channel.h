// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)

#ifndef THREAD_FIBER_CHANNEL_H_
#define THREAD_FIBER_CHANNEL_H_

#include <cstddef>
#include <type_traits>

#include "thread_on_boost/channel-internal.h"
#include "thread_on_boost/fiber.h"
#include "thread_on_boost/select.h"

namespace thread {

template <class T>
class Channel;

template <class T>
class Reader {
 public:
  // This type is neither copyable nor movable.
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  typedef T ValueType;

  // Block until either
  // (a) the next item has been read into *item; returns true.
  // (b) Close() has been called on the corresponding writer and
  //     all values have been consumed; returns false.
  //
  // When supported, Read() will move into *item.
  bool Read(T* item) { return rep_->Get(item); }

  // Returns a Case (to pass to Select()) that will finish by either
  // (a) consuming the next item into *item and storing true in *ok, or
  // (b) storing false in *ok to indicate that the channel has been closed and
  //     all values have been consumed.
  //
  // The objects pointed to by both "item" and "ok" must outlive any call to
  // Select using this case.
  //
  // When supported, OnRead() will move into *item.
  Case OnRead(T* item, bool* ok) { return rep_->OnRead(item, ok); }

 private:
  internal::ChannelState<T>* rep_;
  friend class Channel<T>;
  explicit Reader(internal::ChannelState<T>* rep) : rep_(rep) {}
};

// Style approval: go/channel-move
template <class T>
class Writer {
 public:
  // This type is neither copyable nor movable.
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  typedef T ValueType;

  // Blocks until the channel is able to accept a value (for a description of
  // buffering see Channel()) and writes "item" to the channel.
  //
  // See thread::SelectUntil in select.h for how to write only if there's space.
  //
  // If "item" is a temporary or the result of calling std::move, it will be
  // move-assigned into the buffer or a waiting reader.
  //
  // REQUIRES: May not be called after this->Close().
  void Write(const T& item) { Select({OnWrite(item)}); }

  void Write(T&& item) { Select({OnWrite(std::move(item))}); }

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read().
  //
  // REQUIRES: May be called at most once.
  void Close() { rep_->Close(); }

  // Returns a Case (to pass to Select()) that will finish by adding the
  // supplied item to the channel. Selectable only when there is space available
  // on the channel.
  //
  // See Write() above for a description of whether "item" is copied or moved.
  //
  // CAUTION: The lifetime of "item" must be guaranteed to span any call to
  // Select() with this case. Care must be taken when passing temporaries, whose
  // lifetime may only be guaranteed for the current statement. For example:
  //
  //   // Legal: int(23)'s lifetime extends to end of statement.
  //   int index = thread::Select({ c_0, c_1, writer->OnWrite(23) });
  //   // Illegal: Lifetime of int(19) not guaranteed beyond c's declaration.
  //   thread::Case c = writer->OnWrite(19);
  //   int index = thread::Select({ c, ... });
  //
  Case OnWrite(const T& item) { return rep_->OnWrite(item); }

  // NOTE: "item" is not mutated (moved from) unless the returned Case is
  // selected. E.g.:
  //   // This is safe, ownership is only moved from if the case is selected.
  //   auto unique_ptr = absl::make_unique<T>();
  //   int index = thread::Select({ c_0, c_1,
  //                                writer->OnWrite(std::move(unique_ptr)) });
  //   if (index != 2) {
  //     unique_ptr->Foo();
  //   }
  Case OnWrite(T&& item) { return rep_->OnWrite(std::move(item)); }

  // Write the supplied value to the channel, returning false if and only if the
  // calling fiber is cancelled before the value can be written.
  //
  // See Write() above for a description of whether "item" is copied or moved.
  // If "item" is moved, the move happens iff this function returns true,
  // similarly to OnWrite(T&&) above.
  //
  // Use this function to add cancellation points to your control flow wherever
  // you write to a channel, necessary to avoid a deadlock in some cases (see
  // the best practices at the top of this file for details). For example:
  //
  //     util::Status ProduceLotsOfInts(thread::Writer<int>* c) {
  //       for (int i = 0; i < 1e9; ++i) {
  //         if (!c->WriteUnlessCancelled(i)) {
  //           return util::Status::CANCELLED;
  //         }
  //       }
  //       return util::Status::OK;
  //     }
  //
  bool WriteUnlessCancelled(const T& item) {
    return !Cancelled() && Select({OnCancel(), OnWrite(item)}) == 1;
  }

  bool WriteUnlessCancelled(T&& item) {
    return !Cancelled() && Select({OnCancel(), OnWrite(std::move(item))}) == 1;
  }

  // // Like WriteUnlessCancelled above, except also pay attention to the current
  // // context's deadline. Callers should use this to add interruption points to
  // // their code when the interface they export promises to be aware of
  // // the Context-associated deadline.
  // //
  // // While WriteUnlessCancelled will never post a value to a Channel once
  // // cancelled, an "Expired" deadline will not prevent writes if there is
  // // instantaneously room.
  // //
  // // "clock" is optional. Callers may set it for testing, e.g. as a
  // // SimulatedClock or MockClock.
  // bool WriteUnlessCancelledOrExpired(const T& item,
  //                                    util::Clock* clock = nullptr) {
  //   const absl::Time deadline = base::CurrentContext().deadline();
  //   return !Cancelled() &&
  //          SelectUntil(clock, deadline, {OnCancel(), OnWrite(item)}) == 1;
  // }
  //
  // bool WriteUnlessCancelledOrExpired(T&& item, util::Clock* clock = nullptr) {
  //   const absl::Time deadline = base::CurrentContext().deadline();
  //   return !Cancelled() &&
  //          SelectUntil(clock, deadline,
  //                      {OnCancel(), OnWrite(std::move(item))}) == 1;
  // }

 private:
  internal::ChannelState<T>* rep_;
  friend class Channel<T>;
  explicit Writer(internal::ChannelState<T>* rep) : rep_(rep) {}
};

template <class T>
class Channel {
  // Check that the type is a complete type. `std::is_move_assignable` will
  // always fail for incomplete types, so the following `static_assert` will
  // always fail when this assert fails. This assert exists simply to provide a
  // better error message.
  static_assert(sizeof(T), "Channel<T> must have a complete type T.");
  // Check the type requirements documented at the top of this file.
  static_assert(std::is_move_assignable_v<T>,
                "Channel<T> requires T to be MoveAssignable.");

 public:
  typedef Reader<T> ReaderType;
  typedef Writer<T> WriterType;
  typedef T ValueType;

  // Buffered channels ("buffer_capacity" > 0) behave like a producer-consumer
  // queue into which up to "buffer_capacity" items may be queued. Memory
  // usage is proportional to the number of items buffered, not the capacity.
  // Although unbounded channels are possible by passing SIZE_MAX, consider that
  // these introduce OOM risk.
  //
  // Unbuffered channels ("buffer_capacity" == 0) are synchronous; all writers
  // will block until there is an available matching reader, and vice versa.
  explicit Channel(size_t buffer_capacity)
      : state_(buffer_capacity), reader_(&state_), writer_(&state_) {}

  // This type is neither copyable nor movable.
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  // TODO(void): Should we require that the writer is closed?
  ~Channel() = default;

  // Accessors for the reader and writer endpoints of the channel. These
  // pointers are only valid for as long as the channel object exists.
  Reader<T>* reader() { return &reader_; }
  Writer<T>* writer() { return &writer_; }

  // Accessor for number of items currently held-up in a channel. Returns an
  // instantaneous value; client logic should not depend on it.
  size_t length() const { return state_.Length(); }

 private:
  internal::ChannelState<T> state_;
  Reader<T> reader_;
  Writer<T> writer_;
};

// Holds a scoped reference to the Writer<T> endpoint of a Channel.  On
// destruction, the hosted Channel will be automatically closed unless release()
// has been called.
//
//     void WriteValues(thread::Writer<int>* values) {
//       thread::WriterCloser<int> closer(values);
//       ...
//       if (error_condition) {
//         return; // Channel will be closed
//       }
//       ...
//       WriteValuesSomeOtherWay(closer.release());  // Channel won't be closed
//     }
//
// This can also be used to document in the type system that a function accepts
// responsibility for closing a channel, much the same way that accepting
// std::unique_ptr documents accepting ownership:
//
//     // Write values to the supplied channel. It goes without saying that this
//     // function will close the channel when it's done producing values.
//     void ProduceValues(thread::WriterCloser<int> values) {
//       values->Write(1);
//       values->Write(2);
//       values->Write(3);
//     }
//
template <typename T>
class WriterCloser {
 public:
  WriterCloser() = default;
  explicit WriterCloser(thread::Writer<T>* const writer) : writer_(writer) {}

  WriterCloser(WriterCloser&& that) noexcept : writer_(that.writer_) {
    that.writer_ = nullptr;
  }
  WriterCloser& operator=(WriterCloser&& that) noexcept {
    reset(that.release());
    return *this;
  }
  WriterCloser& operator=(std::nullptr_t) {
    reset(nullptr);
    return *this;
  }

  ~WriterCloser() { reset(); }

  // Return a pointer to the writer, for use in calling APIs that write to but
  // don't close the channel:
  //
  //    void WriteToAndCloseChannel(thread::WriterCloser<int> values) [
  //      WriteSomeValues(values.get());
  //      WriteOtherValues(values.get());
  //    }
  //
  Writer<T>* get() const { return writer_; }

  // Allow direct use of the writer via the writer closer using -> syntax:
  //
  //    void WriteToAndCloseChannel(thread::WriterCloser<int> values) [
  //      values->Write(1);
  //      values->Write(2);
  //      values->Write(3);
  //
  //      thread::Select({
  //        values->OnWrite(4),
  //        thread::OnCancel(),
  //      });
  //    }
  //
  Writer<T>* operator->() const { return get(); }

  // Change the writer held, closing the previous one if it was present.
  void reset(thread::Writer<T>* const writer = nullptr) {
    if (writer_) {
      writer_->Close();
    }

    writer_ = writer;
  }

  // Release "writer". Its Channel will not be closed when *this is destroyed.
  // Returns "writer", or nullptr if release() has already been called.
  Writer<T>* release() {
    auto tmp = writer_;
    writer_ = nullptr;
    return tmp;
  }

 private:
  Writer<T>* writer_ = nullptr;
};

// This allows us to create a WriterCloser without having to figure out its
// type, simply by writing:
//
//   auto closer = thread::MakeWriterCloser(writer);
//
template <typename T>
ABSL_MUST_USE_RESULT WriterCloser<T> MakeWriterCloser(Writer<T>* writer) {
  return WriterCloser<T>(writer);
}

}  // namespace thread

#endif  // THREAD_FIBER_CHANNEL_H_
