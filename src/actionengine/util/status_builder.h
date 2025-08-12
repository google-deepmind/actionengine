/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ACTIONENGINE_UTIL_STATUS_BUILDER_H_
#define ACTIONENGINE_UTIL_STATUS_BUILDER_H_

#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include <absl/base/attributes.h>
#include <absl/status/status.h>

#include "actionengine/util/source_location.h"

namespace act::util {

class ABSL_MUST_USE_RESULT StatusBuilder {
 public:
  StatusBuilder(const StatusBuilder& sb);
  StatusBuilder& operator=(const StatusBuilder& sb);

  StatusBuilder(StatusBuilder&&) = default;
  StatusBuilder& operator=(StatusBuilder&&) = default;

  // Creates a `StatusBuilder` based on an original status.  If logging is
  // enabled, it will use `location` as the location from which the log message
  // occurs.  A typical user will call this with `ACTIONENGINE_LOC`.
  StatusBuilder(const absl::Status& original_status,
                act::util::source_location location)
      : impl_(original_status.ok()
                  ? nullptr
                  : std::make_unique<Impl>(original_status, location)) {}

  StatusBuilder(absl::Status&& original_status,
                act::util::source_location location)
      : impl_(original_status.ok()
                  ? nullptr
                  : std::make_unique<Impl>(std::move(original_status),
                                           location)) {}

  // Creates a `StatusBuilder` from a status code. If logging is
  // enabled, it will use `location` as the location from which the log message
  // occurs.  A typical user will call this with `ACTIONENGINE_LOC`.
  StatusBuilder(absl::StatusCode code, act::util::source_location location)
      : impl_(code == absl::StatusCode::kOk
                  ? nullptr
                  : std::make_unique<Impl>(absl::Status(code, ""), location)) {}

  bool ok() const { return !impl_; }

  StatusBuilder& SetAppend() &;
  StatusBuilder&& SetAppend() &&;

  StatusBuilder& SetPrepend() &;
  StatusBuilder&& SetPrepend() &&;

  StatusBuilder& SetNoLogging() &;
  StatusBuilder&& SetNoLogging() &&;

  template <typename T>
  StatusBuilder& operator<<(const T& msg) & {
    if (!impl_)
      return *this;
    impl_->stream << msg;
    return *this;
  }

  template <typename T>
  StatusBuilder&& operator<<(const T& msg) && {
    return std::move(*this << msg);
  }

  operator absl::Status() const&;
  operator absl::Status() &&;

  absl::Status JoinMessageToStatus();

 private:
  struct Impl {
    // Specifies how to join the error message in the original status and any
    // additional message that has been streamed into the builder.
    enum class MessageJoinStyle {
      kAnnotate,
      kAppend,
      kPrepend,
    };

    Impl(const absl::Status& status, act::util::source_location location);
    Impl(absl::Status&& status, act::util::source_location location);
    Impl(const Impl&);
    Impl& operator=(const Impl&);

    absl::Status JoinMessageToStatus();

    // The status that the result will be based on.
    absl::Status status;
    // The source location to record if this file is logged.
    act::util::source_location location;
    // Logging disabled if true.
    bool no_logging = false;
    // The additional messages added with `<<`.  This is nullptr when status_ is
    // ok.
    std::ostringstream stream;
    // Specifies how to join the message in `status_` and `stream_`.
    MessageJoinStyle join_style = MessageJoinStyle::kAnnotate;
  };

  // Internal store of data for the class.  An invariant of the class is that
  // this is null when the original status is okay, and not-null otherwise.
  std::unique_ptr<Impl> impl_;
};

inline StatusBuilder AlreadyExistsErrorBuilder(
    act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kAlreadyExists, location);
}

inline StatusBuilder FailedPreconditionErrorBuilder(
    act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kFailedPrecondition, location);
}

inline StatusBuilder InternalErrorBuilder(act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kInternal, location);
}

inline StatusBuilder InvalidArgumentErrorBuilder(
    act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kInvalidArgument, location);
}

inline StatusBuilder NotFoundErrorBuilder(act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kNotFound, location);
}

inline StatusBuilder UnavailableErrorBuilder(
    act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kUnavailable, location);
}

inline StatusBuilder UnimplementedErrorBuilder(
    act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kUnimplemented, location);
}

inline StatusBuilder UnknownErrorBuilder(act::util::source_location location) {
  return StatusBuilder(absl::StatusCode::kUnknown, location);
}

}  // namespace act::util

#endif  // ACTIONENGINE_REDIS_STATUS_BUILDER_H_