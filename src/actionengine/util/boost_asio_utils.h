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

#ifndef ACTIONENGINE_UTIL_BOOST_ASIO_UTILS_H_
#define ACTIONENGINE_UTIL_BOOST_ASIO_UTILS_H_

#define BOOST_ASIO_NO_DEPRECATED

#include <optional>
#include <type_traits>
#include <utility>

#include <absl/container/inlined_vector.h>
#include <absl/functional/any_invocable.h>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>

#include "actionengine/concurrency/concurrency.h"

namespace act::util {

boost::asio::thread_pool* GetDefaultAsioExecutionContext();

}  // namespace act::util

#endif  // ACTIONENGINE_UTIL_BOOST_ASIO_UTILS_H_