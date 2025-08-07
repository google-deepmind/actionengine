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

#ifndef EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_
#define EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_

#include <memory>
#include <optional>

#define BOOST_ASIO_NO_DEPRECATED

#include <utility>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/net/websockets/fiber_aware_websocket_stream.h"
#include "eglt/service/service.h"
#include "eglt/util/boost_asio_utils.h"
#include "eglt/util/random.h"

namespace eglt::net {

class WebsocketWireStream final : public WireStream {
 public:
  explicit WebsocketWireStream(std::unique_ptr<BoostWebsocketStream> stream,
                               std::string_view id = "");

  explicit WebsocketWireStream(FiberAwareWebsocketStream stream,
                               std::string_view id = "");

  absl::Status Send(SessionMessage message) override;

  absl::StatusOr<std::optional<SessionMessage>> Receive(
      absl::Duration timeout) override;

  thread::Case OnReceive(std::optional<SessionMessage>* absl_nonnull message,
                         absl::Status* absl_nonnull status) override {
    return thread::NonSelectableCase();
  }

  absl::Status Start() override;

  absl::Status Accept() override;

  absl::Status HalfClose() override;

  absl::Status GetStatus() const override { return status_; }

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* absl_nonnull GetImpl() const override {
    return &stream_;
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const WebsocketWireStream& stream) {
    absl::Format(&sink, "WebsocketWireStream(id: %s, status: %v)", stream.id_,
                 stream.status_);
  }

 private:
  absl::Status SendInternal(SessionMessage message)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable eglt::Mutex mu_;

  FiberAwareWebsocketStream stream_;
  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  std::string id_;

  absl::Status status_;
};

class WebsocketServer {
 public:
  explicit WebsocketServer(eglt::Service* absl_nonnull service,
                           std::string_view address = "0.0.0.0",
                           uint16_t port = 20000)
      : service_(service),
        acceptor_(std::make_unique<boost::asio::ip::tcp::acceptor>(
            *util::GetDefaultAsioExecutionContext())) {
    boost::system::error_code error;

    acceptor_->open(boost::asio::ip::tcp::v4(), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketServer open() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->set_option(boost::asio::ip::tcp::no_delay(true));
    acceptor_->set_option(boost::asio::socket_base::reuse_address(true), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketServer set_option() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->bind(boost::asio::ip::tcp::endpoint(
                        boost::asio::ip::make_address(address), port),
                    error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketServer bind() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketServer listen() failed: " << status_;
      ABSL_ASSUME(false);
    }

    DLOG(INFO) << "WebsocketServer created at " << address << ":" << port;
  }

  ~WebsocketServer() {
    eglt::MutexLock lock(&mu_);
    CancelInternal().IgnoreError();
    JoinInternal().IgnoreError();
    DLOG(INFO) << "WebsocketServer::~WebsocketServer()";
  }

  void Run() {
    eglt::MutexLock l(&mu_);

    main_loop_ = thread::NewTree({}, [this]() {
      eglt::MutexLock lock(&mu_);

      while (!thread::Cancelled()) {
        boost::asio::ip::tcp::socket socket{
            boost::asio::make_strand(*util::GetDefaultAsioExecutionContext())};

        DLOG(INFO) << "WES waiting for connection.";
        boost::system::error_code error;
        thread::PermanentEvent accepted;
        acceptor_->async_accept(
            socket, [&error, &accepted](const boost::system::error_code& ec) {
              error = ec;
              accepted.Notify();
            });

        mu_.Unlock();
        thread::Select({accepted.OnEvent(),
                        thread::OnCancel()});  // Wait for accept to complete.
        mu_.Lock();

        cancelled_ = thread::Cancelled() ||
                     error == boost::system::errc::operation_canceled ||
                     cancelled_;
        if (cancelled_) {
          DLOG(INFO) << "WebsocketServer canceled and is exiting "
                        "its main loop";
          break;
        }

        if (error) {
          DLOG(ERROR) << "WebsocketServer accept() failed: " << error.message();
          switch (error.value()) {
            case boost::system::errc::operation_canceled:
              status_ = absl::OkStatus();
              break;
            default:
              DLOG(ERROR) << "WebsocketServer accept() failed.";
              status_ = absl::InternalError(error.message());
              break;
          }
          // Any code reaching here means the service is shutting down.
          break;
        }

        mu_.Unlock();
        auto stream = std::make_unique<BoostWebsocketStream>(std::move(socket));
        PrepareServerStream(stream.get()).IgnoreError();
        auto connection = service_->EstablishConnection(
            std::make_shared<WebsocketWireStream>(std::move(stream)));
        mu_.Lock();

        if (!connection.ok()) {
          status_ = connection.status();
          DLOG(ERROR) << "WebsocketServer EstablishConnection failed: "
                      << status_;
          // continuing here
        }
      }
      acceptor_->close();
    });
  }

  absl::Status Cancel() {
    eglt::MutexLock lock(&mu_);
    return CancelInternal();
  }

  absl::Status Join() {
    eglt::MutexLock lock(&mu_);
    return JoinInternal();
  }

 private:
  absl::Status CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (cancelled_) {
      return absl::OkStatus();
    }
    cancelled_ = true;
    DLOG(INFO) << "WebsocketServer Cancel()";
    acceptor_->close();
    // util::GetDefaultAsioExecutionContext()->stop();
    main_loop_->Cancel();

    if (boost::system::error_code error; error) {
      status_ = absl::InternalError(error.message());
      return status_;
    }

    return absl::OkStatus();
  }

  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    while (joining_) {
      join_cv_.Wait(&mu_);
    }
    if (main_loop_ == nullptr) {
      return status_;
    }
    joining_ = true;

    mu_.Unlock();
    main_loop_->Join();
    mu_.Lock();

    main_loop_ = nullptr;
    joining_ = false;
    join_cv_.SignalAll();

    DLOG(INFO) << "WebsocketServer main_loop_ joined";
    return status_;
  }

  eglt::Service* absl_nonnull const service_;
  std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;

  mutable eglt::Mutex mu_;
  std::unique_ptr<thread::Fiber> main_loop_;
  bool cancelled_ ABSL_GUARDED_BY(mu_) = false;
  eglt::CondVar join_cv_ ABSL_GUARDED_BY(mu_);
  bool joining_ ABSL_GUARDED_BY(mu_) = false;
  absl::Status status_;
};

inline absl::StatusOr<std::unique_ptr<WebsocketWireStream>>
MakeWebsocketWireStream(std::string_view address = "127.0.0.1",
                        uint16_t port = 20000, std::string_view target = "/",
                        std::string_view id = "",
                        PrepareStreamFn prepare_stream = PrepareClientStream) {

  absl::StatusOr<FiberAwareWebsocketStream> ws_stream =
      FiberAwareWebsocketStream::Connect(
          *util::GetDefaultAsioExecutionContext(), address, port, target,
          std::move(prepare_stream));

  if (!ws_stream.ok()) {
    return ws_stream.status();
  }

  if (absl::Status handshake_status = ws_stream->Start();
      !handshake_status.ok()) {
    return handshake_status;
  }

  std::string session_id = id.empty() ? GenerateUUID4() : std::string(id);
  return std::make_unique<WebsocketWireStream>(*std::move(ws_stream),
                                               session_id);
}

}  // namespace eglt::net

#endif  // EGLT_NET_WEBSOCKETS_WEBSOCKETS_H_