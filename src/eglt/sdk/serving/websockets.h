#ifndef EGLT_SDK_SERVING_WEBSOCKETS_H_
#define EGLT_SDK_SERVING_WEBSOCKETS_H_

#include <memory>
#include <optional>

#define BOOST_ASIO_NO_DEPRECATED

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <utility>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"
#include "eglt/util/random.h"

namespace eglt::sdk {

namespace asio = boost::asio;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

static asio::thread_pool* GetDefaultAsioExecutionContext() {
  static auto* context =
      new asio::thread_pool(std::thread::hardware_concurrency() * 2);
  return context;
}

// If fn() is void, result holder cannot be created
template <typename Invocable, typename = std::enable_if_t<!std::is_void_v<
                                  std::invoke_result_t<Invocable>>>>
auto RunInAsioContext(Invocable&& fn,
                      concurrency::impl::CaseArray additional_cases = {}) {
  std::optional<decltype(fn())> result;
  concurrency::PermanentEvent done;
  asio::post(*GetDefaultAsioExecutionContext(),
             [&done, &result, fn = std::forward<Invocable>(fn)]() {
               result = fn();
               done.Notify();
             });

  auto cases = std::move(additional_cases);
  cases.push_back(done.OnEvent());
  concurrency::Select(cases);

  return *result;
}

inline auto RunInAsioContext(
    absl::AnyInvocable<void()>&& fn,
    concurrency::impl::CaseArray additional_cases = {}) {
  concurrency::PermanentEvent done;
  asio::post(*GetDefaultAsioExecutionContext(), [&done, &fn]() {
    fn();
    done.Notify();
  });

  auto cases = std::move(additional_cases);
  cases.push_back(done.OnEvent());
  concurrency::Select(cases);
}

class WebsocketEvergreenWireStream final : public EvergreenWireStream {
 public:
  static constexpr size_t kSwapBufferOnCapacity = 1 * 1024 * 1024;  // 1MB

  explicit WebsocketEvergreenWireStream(
      beast::websocket::stream<tcp::socket> stream, std::string_view id = "")
      : stream_(std::move(stream)),
        id_(id.empty() ? GenerateUUID4() : std::string(id)) {
    DLOG(INFO) << absl::StrFormat("WESt %s created", id_);
  }

  ~WebsocketEvergreenWireStream() override {
    DLOG(INFO) << absl::StrFormat(
        "WebsocketEvergreenWireStream::~WebsocketEvergreenWireStream(), id=%s",
        id_);
    if (stream_.is_open()) {
      HalfClose();
    }
    concurrency::MutexLock lock(&mutex_);
    while (send_pending_ || recv_pending_) {
      cv_.Wait(&mutex_);
    }
  }

  absl::Status Send(SessionMessage message) override {
    auto message_bytes = cppack::Pack(std::move(message));

    concurrency::MutexLock lock(&mutex_);
    if (!status_.ok()) {
      return status_;
    }

    send_pending_ = true;
    mutex_.Unlock();
    boost::system::error_code error;
    stream_.binary(true);
    RunInAsioContext(
        [this, &error, &message_bytes]() {
          stream_.write(asio::buffer(message_bytes), error);
        },
        {concurrency::OnCancel()});
    mutex_.Lock();
    send_pending_ = false;
    cv_.SignalAll();

    last_send_status_ = absl::OkStatus();

    if (concurrency::Cancelled()) {
      stream_.next_layer().shutdown(asio::socket_base::shutdown_send);
      DLOG(INFO) << absl::StrFormat("WESt %s Send cancelled", id_);
      last_send_status_ = absl::CancelledError("Send cancelled");
    }

    if (error) {
      last_send_status_ = absl::InternalError(error.message());
      DLOG(INFO) << absl::StrFormat("WESt %s Send failed: %v", id_,
                                    last_send_status_);
    }

    return last_send_status_;
  }

  std::optional<SessionMessage> Receive() override {
    concurrency::MutexLock lock(&mutex_);
    if (!status_.ok()) {
      DLOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_,
                                     status_);
      return std::nullopt;
    }

    std::vector<uint8_t> buffer;

    recv_pending_ = true;
    mutex_.Unlock();
    boost::system::error_code error;
    RunInAsioContext(
        [this, &buffer, &error]() {
          auto dynamic_buffer = asio::dynamic_buffer(buffer);
          stream_.read(dynamic_buffer, error);
        },
        {concurrency::OnCancel()});
    mutex_.Lock();
    recv_pending_ = false;
    cv_.SignalAll();

    if (concurrency::Cancelled()) {
      stream_.next_layer().shutdown(asio::socket_base::shutdown_receive);
      DLOG(INFO) << absl::StrFormat("WESt %s Receive cancelled", id_);
      return std::nullopt;
    }

    if (error) {
      LOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_,
                                    error.message());
      return std::nullopt;
    }

    if (auto unpacked = cppack::Unpack<SessionMessage>(buffer); unpacked.ok()) {
      return *std::move(unpacked);
    } else {
      LOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_,
                                    unpacked.status());
    }

    return std::nullopt;
  }

  absl::Status Start() override {
    // In this case, the client EG stream is not responsible for handshaking.
    return absl::OkStatus();
  }

  absl::Status Accept() override {
    DLOG(INFO) << absl::StrFormat("WESt %s Accept()", id_);
    stream_.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::response_type& res) {
          res.set(
              beast::http::field::server,
              "Action Engine / Evergreen Light 0.1.0 WebsocketEvergreenServer");
        }));

    boost::system::error_code error;

    RunInAsioContext([this, &error]() { stream_.accept(error); });
    if (error) {
      status_ = absl::InternalError(error.message());
      return status_;
    }
    DLOG(INFO) << absl::StrFormat("WESt %s Accept(): %v.", id_, status_);
    return absl::OkStatus();
  }

  void HalfClose() override {
    concurrency::MutexLock lock(&mutex_);

    if (!stream_.is_open()) {
      DLOG(INFO) << absl::StrFormat(
          "WESt %s HalfClose: stream is already closed", id_);
      return;
    }

    boost::system::error_code error;
    stream_.next_layer().cancel(error);
    if (error) {
      LOG(ERROR) << absl::StrFormat("WESt %s HalfClose cancel failed: %v", id_,
                                    error.message());
      status_ = absl::InternalError(error.message());
    }
    status_ = absl::CancelledError("Cancelled");
    while (send_pending_ || recv_pending_) {
      cv_.Wait(&mutex_);
    }
    stream_.next_layer().wait(tcp::socket::wait_error, error);
    if (stream_.is_open()) {
      DLOG(WARNING) << absl::StrFormat(
          "WESt %s HalfClose: stream is still open after cancelling and "
          "waiting for error",
          id_);
      RunInAsioContext([this, &error]() {
        stream_.close(beast::websocket::close_code::normal, error);
      });
    }

    if (error) {
      LOG(ERROR) << absl::StrFormat("WESt %s HalfClose failed: %v", id_,
                                    error.message());
      status_ = absl::InternalError(error.message());
    }
  }

  absl::Status GetStatus() const override { return last_send_status_; }

  [[nodiscard]] std::string_view GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override { return &stream_; }

  template <typename Sink>
  friend void AbslStringify(Sink& sink,
                            const WebsocketEvergreenWireStream& stream) {
    const auto endpoint = stream.stream_.next_layer().remote_endpoint();
    sink.Append(absl::StrFormat("WebsocketEvergreenWireStream %s %s:%d",
                                stream.id_, endpoint.address().to_string(),
                                endpoint.port()));
  }

 private:
  beast::websocket::stream<tcp::socket> stream_;
  std::string id_;

  absl::Status status_;
  absl::Status last_send_status_;

  mutable concurrency::Mutex mutex_;
  mutable concurrency::CondVar cv_ ABSL_GUARDED_BY(mutex_);
  bool send_pending_ ABSL_GUARDED_BY(mutex_) = false;
  bool recv_pending_ ABSL_GUARDED_BY(mutex_) = false;
};

class WebsocketEvergreenServer {
 public:
  explicit WebsocketEvergreenServer(eglt::Service* absl_nonnull service,
                                    std::string_view address = "0.0.0.0",
                                    uint16_t port = 20000)
      : service_(service),
        acceptor_(std::make_unique<tcp::acceptor>(
            *GetDefaultAsioExecutionContext())) {
    boost::system::error_code error;

    acceptor_->open(tcp::v4(), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer open() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->set_option(boost::asio::socket_base::reuse_address(true), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer set_option() failed: " << status_;
      ABSL_ASSUME(false);
    }

    acceptor_->bind(tcp::endpoint(boost::asio::ip::make_address(address), port),
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
        tcp::socket socket{*GetDefaultAsioExecutionContext()};

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
          beast::websocket::stream<tcp::socket> stream(std::move(socket));
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
  asio::thread_pool thread_pool_;
  std::unique_ptr<tcp::acceptor> acceptor_;

  mutable concurrency::Mutex mutex_;
  std::unique_ptr<concurrency::Fiber> main_loop_;
  bool cancelled_ ABSL_GUARDED_BY(mutex_) = false;
  concurrency::CondVar join_cv_ ABSL_GUARDED_BY(mutex_);
  bool joining_ ABSL_GUARDED_BY(mutex_) = false;
  absl::Status status_;
};

using PrepareStreamFn =
    absl::AnyInvocable<absl::Status(beast::websocket::stream<tcp::socket>*) &&>;

inline void SetClientStreamOptions(
    beast::websocket::stream<tcp::socket>* absl_nonnull ws_stream) {
  ws_stream->set_option(beast::websocket::stream_base::decorator(
      [](beast::websocket::request_type& req) {
        req.set(beast::http::field::user_agent,
                "Action Engine / Evergreen Light 0.1.0 "
                "WebsocketEvergreenWireStream client");
      }));
}

inline absl::Status PerformHandshake(
    beast::websocket::stream<tcp::socket>* absl_nonnull ws_stream,
    std::string_view address, uint16_t port, std::string_view target) {
  boost::system::error_code error;
  RunInAsioContext(
      [ws_stream, &error, address, port, target]() {
        ws_stream->handshake(absl::StrFormat("%s:%d", address, port), target,
                             error);
      },
      {concurrency::OnCancel()});
  if (error) {
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

inline absl::StatusOr<std::unique_ptr<WebsocketEvergreenWireStream>>
MakeWebsocketEvergreenWireStream(std::string_view address = "127.0.0.1",
                                 uint16_t port = 20000,
                                 std::string_view target = "/",
                                 std::string_view id = "",
                                 PrepareStreamFn prepare_stream = {}) {
  beast::websocket::stream<tcp::socket> ws_stream(
      *GetDefaultAsioExecutionContext());
  boost::system::error_code error;

  tcp::resolver resolver(*GetDefaultAsioExecutionContext());
  auto endpoints = RunInAsioContext(
      [&resolver, &error, address, port]() {
        return resolver.resolve(address, std::to_string(port),
                                boost::asio::ip::resolver_query_base::flags(),
                                error);
      },
      {concurrency::OnCancel()});
  if (error) {
    return absl::InternalError(error.message());
  }
  if (concurrency::Cancelled()) {
    resolver.cancel();
    ws_stream.next_layer().cancel();
    ws_stream.next_layer().shutdown(tcp::socket::shutdown_both, error);
    return absl::CancelledError("Cancelled");
  }

  const tcp::endpoint endpoint = RunInAsioContext(
      [&ws_stream, &error, &endpoints]() {
        return asio::connect(beast::get_lowest_layer(ws_stream), endpoints,
                             error);
      },
      {concurrency::OnCancel()});
  if (error) {
    return absl::InternalError(error.message());
  }
  if (concurrency::Cancelled()) {
    resolver.cancel();
    ws_stream.next_layer().cancel();
    ws_stream.next_layer().shutdown(tcp::socket::shutdown_both, error);
    return absl::CancelledError("Cancelled");
  }

  if (prepare_stream) {
    if (auto status = std::move(prepare_stream)(&ws_stream); !status.ok()) {
      return status;
    }
  } else {
    SetClientStreamOptions(&ws_stream);
    if (auto handshake_status =
            PerformHandshake(&ws_stream, address, endpoint.port(), target);
        !handshake_status.ok()) {
      return handshake_status;
    }
  }

  std::string session_id = id.empty() ? GenerateUUID4() : std::string(id);
  return std::make_unique<WebsocketEvergreenWireStream>(std::move(ws_stream),
                                                        session_id);
}

inline absl::StatusOr<std::unique_ptr<WebsocketEvergreenWireStream>>
MakeWebsocketClientEvergreenWireStream(std::string_view address = "0.0.0.0",
                                       uint16_t port = 20000,
                                       std::string_view target = "/") {

  return MakeWebsocketEvergreenWireStream(
      address, port, target, /*id=*/GenerateUUID4(),
      /*prepare_stream=*/
      [address, port,
       target](beast::websocket::stream<tcp::socket>* absl_nonnull ws_stream) {
        SetClientStreamOptions(ws_stream);
        return PerformHandshake(ws_stream, address, port, target);
      });
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
            MakeWebsocketClientEvergreenWireStream(address, port);
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

#endif  // EGLT_SDK_SERVING_WEBSOCKETS_H_