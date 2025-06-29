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

#include "thread_on_boost/channel/waiter_state.h"
#include "thread_on_boost/cases.h"

namespace thread::internal {

static void Notify(PerSelectCaseState** head, PerSelectCaseState* c)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(c->selector->mu) {
  CHECK_EQ(c->selector->picked_case_index, -1) << "Double-notifying selector";
  if (c->prev) {  // Synchronized by owning ChannelState
    internal::UnlinkFromList(head, c);
  }
  c->TryPick();
}

// Signal all waiting readers that this channel has been closed and no future
// writes are possible.
void ChannelWaiterState::CloseAndReleaseReaders() {
  // Wake up all waiting readers
  PerSelectCaseState* reader = readers;
  while (reader != nullptr) {
    PerSelectCaseState* next_reader = reader->next;
    if (reader->next == readers) {
      // We must be careful to only traverse the list once as there may be
      // waiters who are non-selectable due to being picked by another case, but
      // need the state mutex we hold to Unregister().
      next_reader = nullptr;
    }

    eglt::concurrency::impl::MutexLock l2(&reader->selector->mu);
    if (reader->selector->picked_case_index == Selector::kNonePicked) {
      // We know there was no data previously -- otherwise the reader would not
      // have been waiting -- so we return that the channel was closed.
      bool* ok = reader->GetCase()->GetArgPtr<bool>(1);
      *ok = false;

      Notify(&readers, reader);
    }

    reader = next_reader;
  }
}

// Attempt to start a direct transfer between "reader" and "writer". Returns
// true with both selectors held if both cases are eligible for selection and
// belong to different Select statements. Returns false with no locks held
// otherwise.
static bool LockReaderAndWriterIffBothAreWaiting(
    const PerSelectCaseState* reader, const PerSelectCaseState* writer)
    ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, reader->selector->mu,
                                    writer->selector->mu) {
  if (writer->selector == reader->selector) {
    // Cannot transfer if both reader and writer are part of
    // the same select statement.
    return false;
  }

  // Order the selector locks by address and grab both.
  internal::Selector* s1 =
      (reader->selector < writer->selector ? reader->selector
                                           : writer->selector);
  internal::Selector* s2 =
      (reader->selector < writer->selector ? writer->selector
                                           : reader->selector);

  s1->mu.Lock();
  if (s1->picked_case_index == Selector::kNonePicked) {
    s2->mu.Lock();
    if (s2->picked_case_index == Selector::kNonePicked) {
      return true;
    }
    s2->mu.Unlock();
  }
  s1->mu.Unlock();
  return false;
}

void ChannelWaiterState::UnlockAndReleaseReader(PerSelectCaseState* reader)
    ABSL_UNLOCK_FUNCTION(reader->selector->mu) {
  Notify(&readers, reader);
  reader->selector->mu.Unlock();
}

void ChannelWaiterState::UnlockAndReleaseWriter(PerSelectCaseState* writer)
    ABSL_UNLOCK_FUNCTION(writer->selector->mu) {
  Notify(&writers, writer);
  writer->selector->mu.Unlock();
}

bool ChannelWaiterState::GetMatchingReader(const PerSelectCaseState* writer,
                                           PerSelectCaseState** reader) const {
  if (PerSelectCaseState* current_reader = readers; current_reader != nullptr) {
    do {
      if (LockReaderAndWriterIffBothAreWaiting(current_reader, writer)) {
        // Both current_reader->selector->mu and writer->selector->mu are now locked.
        *reader = current_reader;
        return true;
      }
      current_reader = current_reader->next;
    } while (current_reader != readers);
  }
  return false;
}

bool ChannelWaiterState::GetMatchingWriter(const PerSelectCaseState* reader,
                                           PerSelectCaseState** writer) const {
  if (PerSelectCaseState* current_writer = writers; current_writer != nullptr) {
    do {
      if (LockReaderAndWriterIffBothAreWaiting(reader, current_writer)) {
        // Both reader->selector->mu and current_writer->selector->mu are now locked.
        *writer = current_writer;
        return true;
      }
      current_writer = current_writer->next;
    } while (current_writer != writers);
  }
  return false;
}

bool ChannelWaiterState::GetWaitingWriter(PerSelectCaseState** writer) const {
  if (PerSelectCaseState* current_writer = writers; current_writer != nullptr) {
    do {
      current_writer->selector->mu.Lock();
      if (current_writer->selector->picked_case_index ==
          Selector::kNonePicked) {
        *writer = current_writer;
        return true;  // Returns with *writer->selector->mu held.
      }
      current_writer->selector->mu.Unlock();
      current_writer = current_writer->next;
    } while (current_writer != writers);
  }
  return false;
}

}  // namespace thread::internal