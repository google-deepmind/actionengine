// Copyright 2012 Google Inc. All Rights Reserved.
// Author: sanjay@google.com (Sanjay Ghemawat)

#include <eglt/absl_headers.h>

#include "thread_on_boost/boost_primitives.h"
#include "thread_on_boost/channel-internal.h"
#include "thread_on_boost/select.h"

namespace thread::internal {

static void Notify(CaseState** head, CaseState* c)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(c->sel->mu) {
  CHECK_EQ(c->sel->picked, -1) << "Double-notifying selector";
  if (c->prev) {  // Synchronized by owning ChannelState
    internal::RemoveFromList(head, c);
  }
  c->Pick();
}

// Signal all waiting readers that this channel has been closed and no future
// writes are possible.
void ChannelWaiterState::CloseAndReleaseReaders() {
  // Wake up all waiting readers
  CaseState* reader = waiting_readers_;
  while (reader != nullptr) {
    CaseState* next_reader = reader->next;
    if (reader->next == waiting_readers_) {
      // We must be careful to only traverse the list once as there may be
      // waiters who are non-selectable due to being picked by another case, but
      // need the state mutex we hold to Unregister().
      next_reader = nullptr;
    }

    MutexLock l2(&reader->sel->mu);
    if (reader->sel->picked == Selector::kNonePicked) {
      // We know there was no data previously -- otherwise the reader would not
      // have been waiting -- so we return that the channel was closed.
      *reinterpret_cast<bool*>(reader->params->arg2) = false;
      Notify(&waiting_readers_, reader);
    }

    reader = next_reader;
  }
}

// Attempt to start a direct transfer between "reader" and "writer".  Returns
// true with both selectors held if both cases are eligible for selection and
// belong to different Select statements.  Returns false with no locks held
// otherwise.
static bool StartTransfer(CaseState* reader, CaseState* writer)
    ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true, reader->sel->mu, writer->sel->mu) {
  if (writer->sel == reader->sel) {
    // Cannot transfer if both reader and writer are part of
    // the same select statement.
    return false;
  }

  // Order the selector locks by address and grab both.
  internal::Selector* s1;
  internal::Selector* s2;
  s1 = (reader->sel < writer->sel ? reader->sel : writer->sel);
  s2 = (reader->sel < writer->sel ? writer->sel : reader->sel);

  s1->mu.Lock();
  if (s1->picked == Selector::kNonePicked) {
    s2->mu.Lock();
    if (s2->picked == Selector::kNonePicked) {
      return true;
    } else {
      s2->mu.Unlock();
    }
  }
  s1->mu.Unlock();
  return false;
}

void ChannelWaiterState::UnlockAndReleaseReader(CaseState* reader)
    ABSL_UNLOCK_FUNCTION(reader->sel->mu) {
  Notify(&waiting_readers_, reader);
  reader->sel->mu.Unlock();
}

void ChannelWaiterState::UnlockAndReleaseWriter(CaseState* writer)
    ABSL_UNLOCK_FUNCTION(writer->sel->mu) {
  Notify(&waiting_writers_, writer);
  writer->sel->mu.Unlock();
}

bool ChannelWaiterState::GetMatchingReader(CaseState* writer,
                                           CaseState** reader) {
  CaseState* current_reader = waiting_readers_;
  if (current_reader != nullptr) {
    do {
      if (StartTransfer(current_reader, writer)) {
        // Both current_reader->sel->mu and writer->sel->mu are now locked.
        *reader = current_reader;
        return true;
      }
      current_reader = current_reader->next;
    } while (current_reader != waiting_readers_);
  }
  return false;
}

bool ChannelWaiterState::GetMatchingWriter(CaseState* reader,
                                           CaseState** writer) {
  CaseState* current_writer = waiting_writers_;
  if (current_writer != nullptr) {
    do {
      if (StartTransfer(reader, current_writer)) {
        // Both reader->sel->mu and current_writer->sel->mu are now locked.
        *writer = current_writer;
        return true;
      }
      current_writer = current_writer->next;
    } while (current_writer != waiting_writers_);
  }
  return false;
}

bool ChannelWaiterState::GetWaitingWriter(CaseState** writer) {
  CaseState* current_writer = waiting_writers_;
  if (current_writer != nullptr) {
    do {
      current_writer->sel->mu.Lock();
      if (current_writer->sel->picked == Selector::kNonePicked) {
        *writer = current_writer;
        return true;  // Returns with *writer->sel->mu held.
      }
      current_writer->sel->mu.Unlock();
      current_writer = current_writer->next;
    } while (current_writer != waiting_writers_);
  }
  return false;
}

}  // namespace thread::internal