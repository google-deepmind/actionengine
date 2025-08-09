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

#ifndef ACTIONENGINE_NET_WEBRTC_SIGNALLING_H_
#define ACTIONENGINE_NET_WEBRTC_SIGNALLING_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <absl/base/thread_annotations.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <boost/json/value.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/websockets/fiber_aware_websocket_stream.h"

/**
 * @file
 * Provides the `SignallingClient` class for WebRTC signalling over WebSocket.
 */
namespace act::net {

// void(peer_id, message)
using PeerJsonHandler =
    std::function<void(std::string_view, boost::json::value)>;

/**
 * A client for WebRTC signalling using WebSocket.
 *
 * This class handles the connection to a WebSocket server for WebRTC signalling,
 * allowing the client to send and receive offers, answers, and ICE candidates.
 * It provides methods to set callbacks for handling these messages and to
 * connect with a specific identity.
 *
 * @headerfile actionengine/net/webrtc/signalling.h
 */
class SignallingClient {
 public:
  explicit SignallingClient(std::string_view address = "localhost",
                            uint16_t port = 80);

  // This class is not copyable or movable
  SignallingClient(const SignallingClient&) = delete;
  SignallingClient& operator=(const SignallingClient&) = delete;

  ~SignallingClient();

  void ResetCallbacks();

  void OnOffer(PeerJsonHandler on_offer) { on_offer_ = std::move(on_offer); }

  void OnCandidate(PeerJsonHandler on_candidate) {
    on_candidate_ = std::move(on_candidate);
  }

  void OnAnswer(PeerJsonHandler on_answer) {
    on_answer_ = std::move(on_answer);
  }

  thread::Case OnError() const { return error_event_.OnEvent(); }

  absl::Status GetStatus() const {
    act::MutexLock lock(&mu_);
    return loop_status_;
  }

  absl::Status ConnectWithIdentity(std::string_view identity);

  absl::Status Send(const std::string& message) {
    return stream_.WriteText(message);
  }

  void Cancel() {
    act::MutexLock lock(&mu_);
    CancelInternal();
  }

  void Join() {
    act::MutexLock lock(&mu_);
    JoinInternal();
  }

 private:
  void CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (const absl::Status status = stream_.Close(); !status.ok()) {
      LOG(ERROR) << "SignallingClient::Cancel failed: " << status;
    }
    loop_->Cancel();
    loop_status_ =
        absl::CancelledError("WebsocketActionEngineServer cancelled");
  }

  void JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (loop_ != nullptr) {
      mu_.Unlock();
      loop_->Join();
      mu_.Lock();
      loop_ = nullptr;
    }
  }

  void RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CloseStreamAndJoinLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::string identity_;
  const std::string address_;
  const uint16_t port_;

  PeerJsonHandler on_offer_;
  PeerJsonHandler on_candidate_;
  PeerJsonHandler on_answer_;

  FiberAwareWebsocketStream stream_;
  std::unique_ptr<thread::Fiber> loop_;
  absl::Status loop_status_ ABSL_GUARDED_BY(mu_);
  mutable act::Mutex mu_;
  thread::PermanentEvent error_event_;
};

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBRTC_SIGNALLING_H_