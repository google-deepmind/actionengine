#ifndef EGLT_CONCURRENCY_IMPLEMENTATION_H_
#define EGLT_CONCURRENCY_IMPLEMENTATION_H_

#if defined(__EGLT_CONCURRENCY_IMPLEMENTATION_GOOGLE_THREAD__)
#include "learning/deepmind/evergreen/eglt/cpp/concurrency/google_thread.h"  // IWYU pragma: export
#else
#include "eglt/concurrency/not_implemented.h"
#endif  // defined(__EGLT_CONCURRENCY_IMPLEMENTATION_GOOGLE_THREAD__)

#endif  // EGLT_CONCURRENCY_IMPLEMENTATION_H_

