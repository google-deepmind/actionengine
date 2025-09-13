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

#ifndef ACTIONENGINE_NET_WEBSOCKETS_SERVER_H_
#define ACTIONENGINE_NET_WEBSOCKETS_SERVER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#define BOOST_ASIO_NO_DEPRECATED

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_format.h>
#include <boost/asio/ip/tcp.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/net/websockets/fiber_aware_websocket_stream.h"
#include "actionengine/service/service.h"

namespace act::net {

class WebsocketServer {
 public:
  explicit WebsocketServer(act::Service* absl_nonnull service,
                           std::string_view address = "0.0.0.0",
                           uint16_t port = 20000);

  ~WebsocketServer();

  void Run();

  absl::Status Cancel();

  absl::Status Join();

 private:
  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  act::Service* absl_nonnull const service_;
  std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;

  mutable act::Mutex mu_;
  std::unique_ptr<thread::Fiber> main_loop_;
  bool cancelled_ ABSL_GUARDED_BY(mu_) = false;
  act::CondVar join_cv_ ABSL_GUARDED_BY(mu_);
  bool joining_ ABSL_GUARDED_BY(mu_) = false;
  absl::Status status_;
};

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBSOCKETS_SERVER_H_