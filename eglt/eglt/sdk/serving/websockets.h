#ifndef EGLT_SDK_SERVING_WEBSOCKETS_H_
#define EGLT_SDK_SERVING_WEBSOCKETS_H_

#include <memory>
#include <optional>

#include <boost/asio/ip/tcp.hpp>
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

static asio::system_context& GetSystemContext() {
  static auto context = asio::system_context();
  return context;
}

// If fn() is void, result holder cannot be created
template <typename Invocable, typename = std::enable_if_t<!std::is_void_v<
                                  std::invoke_result_t<Invocable>>>>
auto RunInAsioContext(Invocable&& fn) {
  std::optional<decltype(fn())> result;
  concurrency::PermanentEvent done;
  asio::post(GetSystemContext(),
             [&done, &result, fn = std::forward<Invocable>(fn)]() {
               result = fn();
               done.Notify();
             });
  concurrency::Select({done.OnEvent()});
  return *result;
}

inline auto RunInAsioContext(absl::AnyInvocable<void()>&& fn) {
  concurrency::PermanentEvent done;
  asio::post(GetSystemContext(), [&done, &fn]() {
    fn();
    done.Notify();
  });
  concurrency::Select({done.OnEvent()});
}

class WebsocketEvergreenStream final : public EvergreenStream {
 public:
  static constexpr size_t kSwapBufferOnCapacity = 1 * 1024 * 1024;  // 1MB

  explicit WebsocketEvergreenStream(
      beast::websocket::stream<tcp::socket> stream, std::string_view id = "")
      : stream_(std::move(stream)),
        id_(id.empty() ? GenerateUUID4() : std::string(id)) {
    DLOG(INFO) << absl::StrFormat("WESt %s created", id_);
  }

  ~WebsocketEvergreenStream() override {
    if (stream_.is_open()) {
      HalfClose();
    }
    DLOG(INFO) << absl::StrFormat("WESt %s destroyed", id_);
  }

  absl::Status Send(SessionMessage message) override {
    if (!status_.ok()) {
      return status_;
    }

    auto message_bytes = cppack::Pack(std::move(message));

    boost::system::error_code error;

    stream_.binary(true);
    RunInAsioContext([this, &error, &message_bytes]() {
      stream_.write(asio::buffer(message_bytes), error);
    });

    if (error) {
      last_send_status_ = absl::InternalError(error.message());
      DLOG(INFO) << absl::StrFormat("WESt %s Send failed: %v", id_,
                                    last_send_status_);
      return last_send_status_;
    }

    last_send_status_ = absl::OkStatus();
    return absl::OkStatus();
  }

  std::optional<SessionMessage> Receive() override {
    if (!status_.ok()) {
      DLOG(ERROR) << absl::StrFormat("WESt %s Receive failed: %v", id_,
                                     status_);
      return std::nullopt;
    }

    boost::system::error_code error;
    RunInAsioContext([this, &error]() {
      auto dynamic_buffer = asio::dynamic_buffer(buffer_);
      stream_.read(dynamic_buffer, error);
    });

    if (error) {
      return std::nullopt;
    }

    std::optional<SessionMessage> message;

    if (auto unpacked = cppack::Unpack<SessionMessage>(buffer_);
        unpacked.ok()) {
      message = std::move(unpacked).value();
    }

    if (buffer_.capacity() > kSwapBufferOnCapacity) {
      std::vector<uint8_t>().swap(buffer_);
    } else {
      buffer_.clear();
    }

    return message;
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
    if (!status_.ok()) {
      LOG(ERROR) << absl::StrFormat("WESt %s HalfClose failed: %v", id_,
                                    status_);
      return;
    }

    boost::system::error_code error;
    RunInAsioContext([this, &error]() {
      stream_.close(beast::websocket::close_code::normal, error);
    });

    if (error) {
      LOG(ERROR) << absl::StrFormat("WESt %s HalfClose failed: %v", id_,
                                    error.message());
      last_send_status_ = absl::InternalError(error.message());
    }
  }

  absl::Status GetStatus() const override { return last_send_status_; }

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* GetImpl() const override { return &stream_; }

  template <typename Sink>
  friend void AbslStringify(Sink& sink,
                            const WebsocketEvergreenStream& stream) {
    const auto endpoint = stream.stream_.next_layer().remote_endpoint();
    sink.Append(absl::StrFormat("WebsocketEvergreenStream %s %s:%d", stream.id_,
                                endpoint.address().to_string(),
                                endpoint.port()));
  }

 private:
  beast::websocket::stream<tcp::socket> stream_;
  std::string id_;

  std::vector<uint8_t> buffer_;
  absl::Status status_;
  absl::Status last_send_status_;
};

class WebsocketEvergreenServer {
 public:
  explicit WebsocketEvergreenServer(eglt::Service* absl_nonnull service,
                                    std::string_view address = "0.0.0.0",
                                    uint16_t port = 20000)
      : service_(service),
        acceptor_(std::make_unique<tcp::acceptor>(GetSystemContext())) {
    boost::system::error_code error;

    acceptor_->open(tcp::v4(), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer open() failed: " << status_;
      return;
    }

    acceptor_->set_option(boost::asio::socket_base::reuse_address(true), error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer set_option() failed: " << status_;
      return;
    }

    acceptor_->bind(tcp::endpoint(boost::asio::ip::make_address(address), port),
                    error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer bind() failed: " << status_;
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, error);
    if (error) {
      status_ = absl::InternalError(error.message());
      LOG(FATAL) << "WebsocketEvergreenServer listen() failed: " << status_;
    }

    DLOG(INFO) << "WebsocketEvergreenServer created at " << address << ":"
               << port;
  }

  ~WebsocketEvergreenServer() {
    if (main_loop_ != nullptr) {
      Cancel().IgnoreError();
      Join().IgnoreError();
    }
    DLOG(INFO) << "WebsocketEvergreenServer destroyed";
  }

  void Run() {
    main_loop_ = concurrency::NewTree({}, [this]() {
      while (!concurrency::Cancelled()) {
        tcp::socket socket{GetSystemContext()};

        DLOG(INFO) << "WES waiting for connection.";
        boost::system::error_code error;

        RunInAsioContext(
            [this, &socket, &error]() { acceptor_->accept(socket, error); });

        if (!error) {
          beast::websocket::stream<tcp::socket> stream(std::move(socket));
          auto connection = service_->EstablishConnection(
              std::make_shared<WebsocketEvergreenStream>(std::move(stream)));
          if (!connection.ok()) {
            status_ = connection.status();
            DLOG(ERROR)
                << "WebsocketEvergreenServer EstablishConnection failed: "
                << status_;
            break;
          }
        } else {
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
    boost::system::error_code error;
    acceptor_->cancel(error);

    if (error) {
      status_ = absl::InternalError(error.message());
      return status_;
    }

    return absl::OkStatus();
  }

  absl::Status Join() {
    main_loop_->Join();
    DLOG(INFO) << "WebsocketEvergreenServer joined";
    return status_;
  }

 private:
  eglt::Service* absl_nonnull service_;
  std::unique_ptr<tcp::acceptor> acceptor_;

  std::unique_ptr<concurrency::Fiber> main_loop_;
  absl::Status status_;
};

using PrepareStreamFn =
    absl::AnyInvocable<absl::Status(beast::websocket::stream<tcp::socket>*) &&>;

inline void SetClientStreamOptions(
    beast::websocket::stream<tcp::socket>* ws_stream) {
  ws_stream->set_option(beast::websocket::stream_base::decorator(
      [](beast::websocket::request_type& req) {
        req.set(beast::http::field::user_agent,
                "Action Engine / Evergreen Light 0.1.0 "
                "WebsocketEvergreenStream client");
      }));
}

inline absl::Status PerformHandshake(
    beast::websocket::stream<tcp::socket>* ws_stream, std::string_view address,
    uint16_t port, std::string_view target) {
  boost::system::error_code error;
  RunInAsioContext([ws_stream, &error, address, port, target]() {
    ws_stream->handshake(absl::StrFormat("%s:%d", address, port), target,
                         error);
  });
  if (error) {
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

inline std::unique_ptr<WebsocketEvergreenStream> MakeWebsocketEvergreenStream(
    std::string_view address = "127.0.0.1", uint16_t port = 20000,
    std::string_view target = "/", std::string_view id = "",
    PrepareStreamFn prepare_stream = {}) {
  beast::websocket::stream<tcp::socket> ws_stream(GetSystemContext());
  boost::system::error_code error;

  tcp::resolver resolver(GetSystemContext());
  auto endpoints = RunInAsioContext([&resolver, &error, address, port]() {
    return resolver.resolve(address, std::to_string(port),
                            boost::asio::ip::resolver_query_base::flags(),
                            error);
  });
  if (error) {
    LOG(ERROR) << error.message();
    return nullptr;
  }

  tcp::endpoint endpoint = RunInAsioContext([&ws_stream, &error, &endpoints]() {
    return asio::connect(beast::get_lowest_layer(ws_stream), endpoints, error);
  });
  if (error) {
    LOG(ERROR) << error.message();
    return nullptr;
  }

  absl::Status status = absl::OkStatus();
  if (prepare_stream) {
    status = std::move(prepare_stream)(&ws_stream);
  } else {
    SetClientStreamOptions(&ws_stream);
    status = PerformHandshake(&ws_stream, address, endpoint.port(), target);
  }
  if (!status.ok()) {
    return nullptr;
  }

  std::string session_id = id.empty() ? GenerateUUID4() : std::string(id);
  return std::make_unique<WebsocketEvergreenStream>(std::move(ws_stream),
                                                    session_id);
}

inline std::unique_ptr<WebsocketEvergreenStream>
MakeWebsocketClientEvergreenStream(std::string_view address = "0.0.0.0",
                                   uint16_t port = 20000,
                                   std::string_view target = "/") {

  return MakeWebsocketEvergreenStream(
      address, port, target, /*id=*/GenerateUUID4(),
      /*prepare_stream=*/
      [address, port,
       target](beast::websocket::stream<tcp::socket>* ws_stream) {
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
    eg_stream_ = eglt::sdk::MakeWebsocketClientEvergreenStream(address, port);
    session_ = std::make_unique<Session>(node_map_.get(), &action_registry_);

    if (const absl::Status status = eg_stream_->Start(); !status.ok()) {
      DLOG(ERROR) << absl::StrFormat("WESt %s Start failed: %v",
                                     eg_stream_->GetId(), status);
      return nullptr;
    }

    fiber_ = concurrency::NewTree(concurrency::TreeOptions(), [this]() {
      auto handler_fiber = concurrency::Fiber([this]() {
        status_ = connection_handler_(eg_stream_.get(), session_.get());
      });

      const auto selected = concurrency::Select(
          {handler_fiber.OnJoinable(), concurrency::OnCancel()});

      if (selected == 1) {
        eg_stream_->HalfClose();
        eg_stream_.reset();
      }

      handler_fiber.Join();
    });

    return std::make_shared<StreamToSessionConnection>(
        StreamToSessionConnection{
            .stream = eg_stream_.get(),
            .session = session_.get(),
            .session_id = eg_stream_->GetId(),
            .stream_id = eg_stream_->GetId(),
        });
  }

  void Cancel() const {
    if (fiber_ != nullptr) {
      fiber_->Cancel();
    }
  }

  absl::Status GetStatus() { return status_; }
  absl::Status Join() {
    concurrency::Select({fiber_->OnJoinable(), concurrency::OnCancel()});
    fiber_->Join();
    return GetStatus();
  }

 private:
  std::unique_ptr<concurrency::Fiber> fiber_;

  EvergreenConnectionHandler connection_handler_;
  ActionRegistry action_registry_;

  absl::Status status_;

  std::unique_ptr<WebsocketEvergreenStream> eg_stream_;
  std::unique_ptr<NodeMap> node_map_;
  std::unique_ptr<Session> session_;

  StreamToSessionConnection connection_;
};

}  // namespace eglt::sdk

#endif  // EGLT_SDK_SERVING_WEBSOCKETS_H_