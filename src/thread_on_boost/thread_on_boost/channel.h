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

#ifndef THREAD_FIBER_CHANNEL_H_
#define THREAD_FIBER_CHANNEL_H_

#include <type_traits>

#include "thread_on_boost/channel-internal.h"
#include "thread_on_boost/fiber.h"
#include "thread_on_boost/select.h"

namespace thread {

template <class T>
requires std::is_move_assignable_v<T> class Channel;

template <class T>
class Reader {
 public:
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  bool Read(T* absl_nonnull item) { return channel_state_->Get(item); }
  Case OnRead(T* absl_nonnull item, bool* absl_nonnull ok) {
    return channel_state_->OnRead(item, ok);
  }

 private:
  internal::ChannelState<T>* absl_nonnull channel_state_;
  friend class Channel<T>;
  explicit Reader(internal::ChannelState<T>* absl_nonnull channel_state)
      : channel_state_(channel_state) {}
};

template <class T>
class Writer {
 public:
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  void Write(const T& item) { Select({OnWrite(item)}); }
  void Write(T&& item) { Select({OnWrite(std::move(item))}); }

  // Marks the channel as closed, notifying any waiting readers. See
  // Reader::Read(). May be called at most once.
  void Close() { channel_state_->Close(); }

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
  Case OnWrite(const T& item) { return channel_state_->OnWrite(item); }

  // NOTE: "item" is not mutated (moved from) unless the returned Case is
  // selected. E.g.:
  //   // This is safe, ownership is only moved from if the case is selected.
  //   auto unique_ptr = absl::make_unique<T>();
  //   int index = thread::Select({ c_0, c_1,
  //                                writer->OnWrite(std::move(unique_ptr)) });
  //   if (index != 2) {
  //     unique_ptr->Foo();
  //   }
  Case OnWrite(T&& item) { return channel_state_->OnWrite(std::move(item)); }

  // Returns false iff the calling fiber is cancelled before the value can be
  // written.
  bool WriteUnlessCancelled(const T& item) {
    return !Cancelled() && Select({OnCancel(), OnWrite(item)}) == 1;
  }

  // NOTE: item is only moved from if this function returns true.
  bool WriteUnlessCancelled(T&& item) {
    return !Cancelled() && Select({OnCancel(), OnWrite(std::move(item))}) == 1;
  }

 private:
  internal::ChannelState<T>* absl_nonnull channel_state_;
  friend class Channel<T>;
  explicit Writer(internal::ChannelState<T>* absl_nonnull channel_state)
      : channel_state_(channel_state) {}
};

template <class T>
requires std::is_move_assignable_v<T> class Channel {
 public:
  explicit Channel(size_t buffer_capacity)
      : state_(buffer_capacity), reader_(&state_), writer_(&state_) {}

  // This type is neither copyable nor movable.
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  ~Channel() = default;

  // Accessors for the reader and writer endpoints of the channel. These
  // pointers are only valid for as long as the channel object exists.
  Reader<T>* reader() { return &reader_; }
  Writer<T>* writer() { return &writer_; }

  // Accessor for number of items currently held-up in a channel. Returns an
  // instantaneous value; client logic should not depend on it.
  [[nodiscard]] size_t length() const { return state_.Length(); }

 private:
  internal::ChannelState<T> state_;
  Reader<T> reader_;
  Writer<T> writer_;
};

}  // namespace thread

#endif  // THREAD_FIBER_CHANNEL_H_
