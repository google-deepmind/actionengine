#include "eglt/net/webrtc/signalling.h"

namespace eglt::net {

SignallingClient::SignallingClient(std::string_view address, uint16_t port)
    : address_(address), port_(port) {}

SignallingClient::~SignallingClient() {
  CloseStreamAndJoinLoop();
}

absl::Status SignallingClient::ConnectWithIdentity(std::string_view identity) {
  concurrency::MutexLock l(&mu_);

  if (!on_answer_ && !on_offer_ && !on_candidate_) {
    return absl::FailedPreconditionError(
        "WebsocketEvergreenServer no handlers set: connecting in this "
        "state would lose messages");
  }

  identity_ = std::string(identity);

  auto boost_stream = std::make_unique<BoostWebsocketStream>(
      *util::GetDefaultAsioExecutionContext());

  boost_stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                "Action Engine / Evergreen Light 0.1.0 "
                "WebsocketWireStream client");
      }));
  boost_stream->set_option(boost::beast::websocket::stream_base::timeout(
      boost::beast::websocket::stream_base::timeout::suggested(
          boost::beast::role_type::server)));

  {
    concurrency::ScopedUnlock unlock(&mu_);

    if (absl::Status status =
            ResolveAndConnect(boost_stream.get(), address_, port_);
        !status.ok()) {
      return status;
    }

    stream_ = FiberAwareWebsocketStream(
        std::move(boost_stream), [this](BoostWebsocketStream* stream) {
          return DoHandshake(stream, absl::StrFormat("%s:%d", address_, port_),
                             absl::StrFormat("/%s", identity_));
        });
  }

  loop_status_ = stream_.Start();
  if (!loop_status_.ok()) {
    return loop_status_;
  }

  loop_ = std::make_unique<thread::Fiber>([this]() {
    concurrency::MutexLock lock(&mu_);
    RunLoop();
  });

  return absl::OkStatus();
}

void SignallingClient::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  std::string message;
  boost::json::value parsed_message;
  absl::Status status;

  while (!thread::Cancelled()) {
    message.clear();

    {
      concurrency::ScopedUnlock unlock(&mu_);
      status = stream_.ReadText(&message);
      if (!status.ok()) {
        break;
      }
    }

    boost::system::error_code error;
    parsed_message = boost::json::parse(message, error);
    if (error) {
      LOG(ERROR) << "WebsocketEvergreenServer parse() failed: "
                 << error.message();
      continue;
    }

    std::string client_id;
    if (auto id_ptr = parsed_message.find_pointer("/id", error);
        id_ptr == nullptr || error) {
      LOG(ERROR) << "WebsocketEvergreenServer no 'id' field in message: "
                 << message;
      continue;
    } else {
      client_id = id_ptr->as_string().c_str();
    }

    std::string type;
    if (auto type_ptr = parsed_message.find_pointer("/type", error);
        type_ptr == nullptr || error) {
      LOG(ERROR) << "WebsocketEvergreenServer no 'type' field in message: "
                 << message;
      continue;
    } else {
      type = type_ptr->as_string().c_str();
    }

    if (type != "offer" && type != "candidate" && type != "answer") {
      LOG(ERROR) << "WebsocketEvergreenServer unknown message type: " << type
                 << " in message: " << message;
      continue;
    }

    {
      concurrency::ScopedUnlock unlock(&mu_);

      if (type == "offer" && on_offer_) {
        on_offer_(client_id, std::move(parsed_message));
        continue;
      }

      if (type == "candidate" && on_candidate_) {
        on_candidate_(client_id, std::move(parsed_message));
        continue;
      }

      if (type == "answer" && on_answer_) {
        on_answer_(client_id, std::move(parsed_message));
        continue;
      }
    }
  }

  loop_status_ = status;
  if (!loop_status_.ok()) {
    error_event_.Notify();
  }
}

void SignallingClient::CloseStreamAndJoinLoop() {
  stream_.Close().IgnoreError();
  if (loop_ != nullptr) {
    loop_->Cancel();
    loop_->Join();
    loop_ = nullptr;
  }
}
}  // namespace eglt::net