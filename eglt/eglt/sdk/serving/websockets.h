#ifndef EGLT_SDK_SERVING_WEBSOCKETS_H_
#define EGLT_SDK_SERVING_WEBSOCKETS_H_

#include <memory>
#include <optional>

#include <thread_on_boost/boost_primitives.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "eglt/data/eg_structs.h"
#include "eglt/data/msgpack.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"
#include "eglt/util/random.h"

namespace eglt::sdk {

namespace asio = boost::asio;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

class WebsocketEvergreenStream final : public base::EvergreenStream {
 public:
  static constexpr size_t kSwapBufferOnCapacity = 4 * 1024 * 1024;  // 4MB

  explicit WebsocketEvergreenStream(
      beast::websocket::stream<tcp::socket> stream, std::string_view id = "")
      : stream_(std::move(stream)), id_(id.empty() ? GenerateUUID4() : id) {
    DLOG(INFO) << absl::StrFormat("WES %s created", id_);
  }

  absl::Status Send(SessionMessage message) override {
    if (!status_.ok()) {
      return status_;
    }

    DLOG(INFO) << absl::StrFormat("WES %s sending message: \n%v", id_, message);

    boost::system::error_code error_code;
    auto message_bytes = cppack::Pack(std::move(message));
    stream_.write(asio::buffer(message_bytes), error_code);
    if (error_code) {
      last_send_status_ = absl::InternalError(error_code.message());
      return last_send_status_;
    }

    last_send_status_ = absl::OkStatus();
    return absl::OkStatus();
  }

  std::optional<SessionMessage> Receive() override {
    DLOG(INFO) << absl::StrFormat("WES %s Receive called", id_);
    if (!status_.ok()) {
      DLOG(INFO) << absl::StrFormat("WES %s Receive failed: %v", id_, status_);
      return std::nullopt;
    }

    boost::system::error_code error_code;
    concurrency::PermanentEvent read;
    concurrency::RunInFiber([&error_code, this, &read]() {
      auto dynamic_buffer = asio::dynamic_buffer(buffer_);
      LOG(INFO) << "reading";
      stream_.read(dynamic_buffer, error_code);
      read.Notify();
    });
    concurrency::SelectUntil(absl::Now() + absl::Seconds(2), {read.OnEvent()});
    LOG(INFO) << "selected";

    if (error_code) {
      return std::nullopt;
    }

    std::optional<SessionMessage> message;

    if (auto unpacked = cppack::Unpack<SessionMessage>(buffer_);
        unpacked.ok()) {
      message = std::move(unpacked).value();
    }

    DLOG(INFO) << absl::StrFormat("WES %s received message:\n%v", id_,
                                  *message);

    if (buffer_.capacity() > kSwapBufferOnCapacity) {
      std::vector<uint8_t>().swap(buffer_);
    } else {
      buffer_.clear();
    }

    return message;
  }

  void Start() override {
    // In this case, the client EG stream is not responsible for handshaking.
  }

  void Accept() override {
    DLOG(INFO) << absl::StrFormat("WES %s Accept()", id_);
    stream_.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::response_type& res) {
          res.set(
              beast::http::field::server,
              "Action Engine / Evergreen Light 0.1.0 WebsocketEvergreenServer");
        }));

    boost::system::error_code error_code;
    concurrency::RunInFiber(
        [&error_code, this]() { stream_.accept(error_code); });
    if (error_code) {
      status_ = absl::InternalError(error_code.message());
    }
    DLOG(INFO) << absl::StrFormat("WES %s Accept(): %v.", id_, status_);
  }

  void HalfClose() override {
    if (!status_.ok()) {
      return;
    }

    boost::system::error_code error_code;
    stream_.close(beast::websocket::close_code::normal, error_code);
    if (error_code) {
      last_send_status_ = absl::InternalError(error_code.message());
    }
  }

  absl::Status GetLastSendStatus() const override { return last_send_status_; }

  [[nodiscard]] std::string GetId() const override { return id_; }

 private:
  std::shared_ptr<asio::io_context> io_context_;
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
                                    uint16_t port = 20000,
                                    asio::io_context* io_context = nullptr)
      : service_(service),
        io_context_(io_context != nullptr ? io_context
                                          : &thread::GetThreadLocalIOContext()),
        acceptor_(std::make_unique<tcp::acceptor>(
            *io_context_,
            tcp::endpoint{asio::ip::make_address(address), port})) {
    LOG(INFO) << "WebsocketEvergreenServer created at " << address << ":"
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
    main_loop_ = std::make_unique<std::thread>([this]() {
      while (!concurrency::Cancelled()) {
        tcp::socket socket{*io_context_};

        DLOG(INFO) << "WebsocketEvergreenServer waiting for connection.";
        boost::system::error_code error_code;
        acceptor_->accept(socket, error_code);
        if (!error_code) {
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
          switch (error_code.value()) {
            case boost::system::errc::operation_canceled:
              status_ = absl::OkStatus();
              break;
            default:
              DLOG(ERROR) << "WebsocketEvergreenServer accept() failed.";
              status_ = absl::InternalError(error_code.message());
              break;
          }
          // Any code reaching here means the service is shutting down.
          break;
        }
      }
    });
  }

  absl::Status Cancel() {
    // main_loop_->Cancel();

    boost::system::error_code error_code;
    concurrency::RunInFiber(
        [&error_code, this]() { acceptor_->cancel(error_code); });

    if (error_code) {
      status_ = absl::InternalError(error_code.message());
      return status_;
    }

    return absl::OkStatus();
  }

  absl::Status Join() {
    main_loop_->join();
    DLOG(INFO) << "WebsocketEvergreenServer joined";
    return status_;
  }

 private:
  eglt::Service* absl_nonnull service_;

  asio::io_context* io_context_;
  std::unique_ptr<tcp::acceptor> acceptor_;

  std::unique_ptr<std::thread> main_loop_;
  absl::Status status_;
};

inline std::unique_ptr<WebsocketEvergreenStream>
MakeWebsocketClientEvergreenStream(std::string_view address = "0.0.0.0",
                                   uint16_t port = 20000,
                                   std::string_view target = "/",
                                   asio::io_context* io_context = nullptr) {

  if (io_context == nullptr) {
    io_context = &thread::GetThreadLocalIOContext();
  }
  beast::websocket::stream<tcp::socket> ws_stream(*io_context);

  tcp::resolver resolver(*io_context);
  const auto endpoints = resolver.resolve(address, std::to_string(port));

  boost::system::error_code error_code;

  const auto endpoint = asio::connect(
      beast::get_lowest_layer(ws_stream),
      resolver.resolve(address, std::to_string(port)), error_code);
  if (error_code) {
    return nullptr;
  }

  // Set a decorator to change the User-Agent of the handshake
  ws_stream.set_option(beast::websocket::stream_base::decorator(
      [](beast::websocket::request_type& req) {
        req.set(beast::http::field::user_agent,
                "Action Engine / Evergreen Light 0.1.0 "
                "WebsocketEvergreenStream client");
      }));

  ws_stream.handshake(absl::StrFormat("%s:%d", address, endpoint.port()),
                      target, error_code);

  if (error_code) {
    return nullptr;
  }

  std::string session_id = GenerateUUID4();
  return std::make_unique<WebsocketEvergreenStream>(std::move(ws_stream),
                                                    session_id);
}

}  // namespace eglt::sdk

#endif  // EGLT_SDK_SERVING_WEBSOCKETS_H_