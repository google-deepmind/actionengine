#ifndef EGLT_NET_WEBRTC_SIGNALLING_H_
#define EGLT_NET_WEBRTC_SIGNALLING_H_

#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

#include "eglt/net/websockets/fiber_aware_websocket_stream.h"
#include "eglt/util/boost_asio_utils.h"

namespace eglt::net {

// void(peer_id, message)
using PeerJsonHandler =
    std::function<void(std::string_view, boost::json::value)>;

class SignallingClient {
 public:
  explicit SignallingClient(std::string_view address = "localhost",
                            uint16_t port = 80);

  // This class is not copyable or movable
  SignallingClient(const SignallingClient&) = delete;
  SignallingClient& operator=(const SignallingClient&) = delete;

  ~SignallingClient();

  void OnOffer(PeerJsonHandler on_offer) { on_offer_ = std::move(on_offer); }

  void OnCandidate(PeerJsonHandler on_candidate) {
    on_candidate_ = std::move(on_candidate);
  }

  void OnAnswer(PeerJsonHandler on_answer) {
    on_answer_ = std::move(on_answer);
  }

  concurrency::Case OnError() const { return error_event_.OnEvent(); }

  absl::Status GetStatus() const {
    concurrency::MutexLock lock(&mu_);
    return loop_status_;
  }

  absl::Status ConnectWithIdentity(std::string_view identity);

  absl::Status Send(const std::string& message) {
    return stream_.WriteText(message);
  }

  void Cancel() {
    concurrency::MutexLock lock(&mu_);
    stream_.Close().IgnoreError();
    loop_->Cancel();
    loop_status_ = absl::CancelledError("WebsocketEvergreenServer cancelled");
  }

  void Join() {
    if (loop_ != nullptr) {
      loop_->Join();
      loop_ = nullptr;
    }
  }

 private:
  void RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CloseStreamAndJoinLoop() ABSL_LOCKS_EXCLUDED(mu_);

  std::string identity_;
  const std::string address_;
  const uint16_t port_;

  PeerJsonHandler on_offer_;
  PeerJsonHandler on_candidate_;
  PeerJsonHandler on_answer_;

  FiberAwareWebsocketStream stream_;
  std::unique_ptr<thread::Fiber> loop_;
  absl::Status loop_status_ ABSL_GUARDED_BY(mu_);
  mutable concurrency::Mutex mu_;
  concurrency::PermanentEvent error_event_;
};

}  // namespace eglt::net

#endif  // EGLT_NET_WEBRTC_SIGNALLING_H_