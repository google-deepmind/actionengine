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

#include "actionengine/util/boost_asio_utils.h"

#include <thread>

#include <boost/asio/detail/bind_handler.hpp>
#include <boost/asio/impl/thread_pool.hpp>
#include <boost/asio/impl/thread_pool.ipp>
#include <boost/asio/thread_pool.hpp>

namespace act::util {

boost::asio::thread_pool* GetDefaultAsioExecutionContext() {
  static auto context =
      boost::asio::thread_pool(std::thread::hardware_concurrency() * 2);
  return &context;
}

}  // namespace act::util