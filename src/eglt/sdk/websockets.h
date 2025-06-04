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

#ifndef EGLT_SDK_WEBSOCKETS_H_
#define EGLT_SDK_WEBSOCKETS_H_

#include <memory>
#include <optional>

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <utility>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/sdk/boost_asio_utils.h"
#include "eglt/sdk/fiber_aware_websocket_stream.h"
#include "eglt/service/service.h"
#include "eglt/util/random.h"

namespace eglt::sdk {

class WebsocketEvergreenWireStream final : public EvergreenWireStream {
 public:
  explicit WebsocketEvergreenWireStream(
      std::unique_ptr<BoostWebsocketStream> stream, std::string_view id = "");

  explicit WebsocketEvergreenWireStream(FiberAwareWebsocketStream stream,
                                        std::string_view id = "");

  absl::Status Send(SessionMessage message) override;

  std::optional<SessionMessage> Receive() override;

  absl::Status Start() override;

  absl::Status Accept() override;

  void HalfClose() override;

  absl::Status GetStatus() const override { return status_; }

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override { return &stream_; }

 private:
  FiberAwareWebsocketStream stream_;
  std::string id_;

  absl::Status status_;
};

class WebsocketEvergreenServer {
 public:
  explicit WebsocketEvergreenServer(eglt::Service* absl_nonnull service,
                                    std::string_view address = "0.0.0.0",
                                    uint16_t port = 20000)
      : service_(service),
        acceptor_(std::make_unique<boost::asio::ip::tcp::acceptor>(
            *GetDefaultAsioExecutionContext())) {
    boost::system::error_code error;

    acceptor_->open(boost::asio::ip::tcp::v4(), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer open() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->set_option(boost::asio::ip::tcp::no_delay(true));
    acceptor_->set_option(boost::asio::socket_base::reuse_address(true), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer set_option() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->bind(boost::asio::ip::tcp::endpoint(
                        boost::asio::ip::make_address(address), port),
                    error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer bind() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer listen() failed: " << status_;
      ABSL_ASSUME(false);
    }

    DLOG(INFO) << "WebsocketEvergreenServer created at " << address << ":"
               << port;
  }

  ~WebsocketEvergreenServer() {
    Cancel().IgnoreError();
    Join().IgnoreError();
    DLOG(INFO) << "WebsocketEvergreenServer::~WebsocketEvergreenServer()";
  }

  void Run() {
    concurrency::MutexLock lock(&mutex_);
    main_loop_ = concurrency::NewTree({}, [this]() {
      while (!concurrency::Cancelled()) {
        boost::asio::ip::tcp::socket socket{
            boost::asio::make_strand(*GetDefaultAsioExecutionContext())};

        DLOG(INFO) << "WES waiting for connection.";
        boost::system::error_code error;

        RunInAsioContext(
            [this, &socket, &error]() { acceptor_->accept(socket, error); },
            {concurrency::OnCancel()});
        {
          concurrency::MutexLock cancellation_lock(&mutex_);
          cancelled_ = concurrency::Cancelled() ||
                       error == boost::system::errc::operation_canceled ||
                       cancelled_;
          if (cancelled_) {
            DLOG(INFO) << "WebsocketEvergreenServer canceled and is exiting "
                          "its main loop";
            break;
          }
        }

        if (!error) {
          auto stream =
              std::make_unique<BoostWebsocketStream>(std::move(socket));
          stream->write_buffer_bytes(16);
          boost::beast::websocket::permessage_deflate permessage_deflate_option;
          permessage_deflate_option.msg_size_threshold = 1024;  // 1 KiB
          permessage_deflate_option.server_enable = true;
          permessage_deflate_option.client_enable = true;
          stream->set_option(permessage_deflate_option);
          auto connection = service_->EstablishConnection(
              std::make_shared<WebsocketEvergreenWireStream>(
                  std::move(stream)));
          if (!connection.ok()) {
            status_ = connection.status();
            DLOG(ERROR)
                << "WebsocketEvergreenServer EstablishConnection failed: "
                << status_;
            // continuing here
          }
        } else {
          DLOG(ERROR) << "WebsocketEvergreenServer accept() failed: "
                      << error.message();
          switch (error.value()) {
            case boost::system::errc::operation_canceled:
              status_ = absl::OkStatus();
              break;
            default:
              DLOG(ERROR) << "WebsocketEvergreenServer accept() failed.";
              status_ = absl::InternalError(error.message());
              break;
          }
          // Any code reaching here means the service is shutting down.
          break;
        }
      }
      acceptor_->close();
    });
  }

  absl::Status Cancel() {
    concurrency::MutexLock lock(&mutex_);
    if (cancelled_) {
      return absl::OkStatus();
    }
    cancelled_ = true;
    DLOG(INFO) << "WebsocketEvergreenServer Cancel()";
    boost::system::error_code error;
    acceptor_->close();
    GetDefaultAsioExecutionContext()->stop();
    main_loop_->Cancel();

    if (error) {
      status_ = absl::InternalError(error.message());
      return status_;
    }

    return absl::OkStatus();
  }

  absl::Status Join() {
    {
      concurrency::MutexLock lock(&mutex_);
      while (joining_) {
        join_cv_.Wait(&mutex_);
      }
      if (main_loop_ == nullptr) {
        return status_;
      }
      joining_ = true;
    }
    main_loop_->Join();
    {
      concurrency::MutexLock lock(&mutex_);
      main_loop_ = nullptr;
      joining_ = false;
      join_cv_.SignalAll();
    }

    DLOG(INFO) << "WebsocketEvergreenServer main_loop_ joined";
    return status_;
  }

 private:
  eglt::Service* absl_nonnull const service_;
  boost::asio::thread_pool thread_pool_;
  std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;

  mutable concurrency::Mutex mutex_;
  std::unique_ptr<concurrency::Fiber> main_loop_;
  bool cancelled_ ABSL_GUARDED_BY(mutex_) = false;
  concurrency::CondVar join_cv_ ABSL_GUARDED_BY(mutex_);
  bool joining_ ABSL_GUARDED_BY(mutex_) = false;
  absl::Status status_;
};

inline absl::StatusOr<std::unique_ptr<WebsocketEvergreenWireStream>>
MakeWebsocketEvergreenWireStream(
    std::string_view address = "127.0.0.1", uint16_t port = 20000,
    std::string_view target = "/", std::string_view id = "",
    PrepareStreamFn prepare_stream = PrepareClientStream) {

  absl::StatusOr<FiberAwareWebsocketStream> ws_stream =
      FiberAwareWebsocketStream::Connect(*GetDefaultAsioExecutionContext(),
                                         address, port, target,
                                         std::move(prepare_stream));

  if (!ws_stream.ok()) {
    return ws_stream.status();
  }

  if (const absl::Status handshake_status = ws_stream->Start();
      !handshake_status.ok()) {
    return handshake_status;
  }

  std::string session_id = id.empty() ? GenerateUUID4() : std::string(id);
  return std::make_unique<WebsocketEvergreenWireStream>(*std::move(ws_stream),
                                                        session_id);
}

class WebsocketEvergreenClient {
 public:
  explicit WebsocketEvergreenClient(
      EvergreenConnectionHandler connection_handler = RunSimpleEvergreenSession,
      ActionRegistry action_registry = {},
      const ChunkStoreFactory& chunk_store_factory = {})
      : connection_handler_(std::move(connection_handler)),
        action_registry_(std::move(action_registry)),
        node_map_(std::make_unique<NodeMap>(chunk_store_factory)) {}

  ~WebsocketEvergreenClient() { Cancel(); }

  std::shared_ptr<StreamToSessionConnection> Connect(std::string_view address,
                                                     int32_t port) {
    if (absl::StatusOr<std::unique_ptr<WebsocketEvergreenWireStream>> stream =
            MakeWebsocketEvergreenWireStream(address, port);
        !stream.ok()) {
      DLOG(ERROR) << absl::StrFormat("WESt %s Connect failed: %v", address,
                                     stream.status());
      return nullptr;
    } else {
      eg_stream_ = std::move(stream).value();
    }
    session_ = std::make_unique<Session>(node_map_.get(), &action_registry_);

    if (const absl::Status status = eg_stream_->Start(); !status.ok()) {
      DLOG(ERROR) << absl::StrFormat("WESt %s Start failed: %v",
                                     eg_stream_->GetId(), status);
      return nullptr;
    }

    fiber_ = concurrency::NewTree(concurrency::TreeOptions(), [this]() {
      auto handler_fiber = concurrency::Fiber([this]() {
        status_ = connection_handler_(eg_stream_, session_.get());
      });

      const auto selected = concurrency::Select(
          {handler_fiber.OnJoinable(), concurrency::OnCancel()});

      if (selected == 1) {
        handler_fiber.Cancel();
        eg_stream_->HalfClose();
        eg_stream_.reset();
      }

      handler_fiber.Join();
    });

    return std::make_shared<StreamToSessionConnection>(
        StreamToSessionConnection{
            .stream = eg_stream_,
            .session = session_.get(),
            .session_id = std::string(eg_stream_->GetId()),
            .stream_id = std::string(eg_stream_->GetId()),
        });
  }

  void Cancel() const {
    if (fiber_ != nullptr) {
      fiber_->Cancel();
    }
  }

  absl::Status GetStatus() { return status_; }

  absl::Status Join() {
    const int selected =
        concurrency::Select({fiber_->OnJoinable(), concurrency::OnCancel()});
    if (selected == 1) {
      fiber_->Cancel();
    }
    fiber_->Join();
    return GetStatus();
  }

 private:
  std::unique_ptr<concurrency::Fiber> fiber_;

  EvergreenConnectionHandler connection_handler_;
  ActionRegistry action_registry_;

  absl::Status status_;

  std::shared_ptr<WebsocketEvergreenWireStream> eg_stream_;
  std::unique_ptr<NodeMap> node_map_;
  std::unique_ptr<Session> session_;

  StreamToSessionConnection connection_;
};

}  // namespace eglt::sdk

#endif  // EGLT_SDK_WEBSOCKETS_H_