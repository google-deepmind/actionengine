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

#ifndef THREAD_FIBER_CHANNEL_WAITER_STATE_H_
#define THREAD_FIBER_CHANNEL_WAITER_STATE_H_

#include "thread_on_boost/cases.h"

namespace thread::internal {

// A struct that maintains readers' and writers' queues and provides methods
// to find matching readers and writers in a coordinated way, i.e. locking
// the selector mutexes of both case states if a pair is found.
struct ChannelWaiterState {
  bool GetMatchingReader(const PerSelectCaseState* writer,
                         PerSelectCaseState** reader) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, (*reader)->selector->mu,
                                      writer->selector->mu);
  bool GetMatchingWriter(const PerSelectCaseState* reader,
                         PerSelectCaseState** writer) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, reader->selector->mu,
                                      (*writer)->selector->mu);

  // Attempt to find an eligible queued writer.  There is no matching reader in
  // this case, it is used for when space becomes available in the queue due to
  // a read completing, allowing a writer to complete without partner.
  //
  // Returns true, and updates *writer, if a suitable waiter exists.  *writer is
  // returned with selector mutex held and guaranteed pickable.
  // Returns false with no side effects otherwise.
  bool GetWaitingWriter(PerSelectCaseState** writer) const
      ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, (*writer)->selector->mu);

  // Unlock (and mark selected) the passed reader/writer respectively.
  // REQUIRES: selector mutex is held, picked == kNonePicked
  void UnlockAndReleaseReader(PerSelectCaseState* reader)
      ABSL_UNLOCK_FUNCTION(reader->selector->mu);
  void UnlockAndReleaseWriter(PerSelectCaseState* writer)
      ABSL_UNLOCK_FUNCTION(writer->selector->mu);

  // Releases all waiting readers.  Unselected readers are picked and marked to
  // return that this channel was closed.
  void CloseAndReleaseReaders();

  internal::PerSelectCaseState* readers = nullptr;
  internal::PerSelectCaseState* writers = nullptr;
};

}  // namespace thread::internal

#endif  // THREAD_FIBER_CHANNEL_WAITER_STATE_H_