#ifndef EGLT_SDK_SERVING_WEBSOCKETS_H_
#define EGLT_SDK_SERVING_WEBSOCKETS_H_

#include <memory>
#include <optional>

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

inline asio::io_context& GetGlobalIOContext() {
  static auto* io_context = new asio::io_context(1);
  return *io_context;
}

class WebsocketEvergreenStream final : public base::EvergreenStream {
 public:
  static constexpr size_t kSwapBufferOnCapacity = 4 * 1024 * 1024;  // 4MB

  explicit WebsocketEvergreenStream(
      beast::websocket::stream<tcp::socket> stream, std::string_view id = "")
      : stream_(std::move(stream)), id_(id.empty() ? GenerateUUID4() : id) {}

  absl::Status Send(SessionMessage message) override {
    if (!status_.ok()) {
      return status_;
    }

    auto buffer = cppack::Pack(std::move(message));

    boost::system::error_code ec;
    stream_.write(boost::asio::buffer(buffer), ec);
    if (ec) {
      last_send_status_ = absl::InternalError(ec.message());
      return last_send_status_;
    }

    return absl::OkStatus();
  }

  std::optional<SessionMessage> Receive() override {
    if (!status_.ok()) {
      return std::nullopt;
    }

    boost::system::error_code ec;
    stream_.read(buffer_, ec);
    if (ec) {
      return std::nullopt;
    }

    std::optional<SessionMessage> msg = std::nullopt;
    if (auto unpacked = cppack::Unpack<SessionMessage>(buffer_);
        unpacked.ok()) {
      msg = std::move(*unpacked);
    }

    if (buffer_.capacity() > kSwapBufferOnCapacity) {
      std::vector<uint8_t>().swap(buffer_);
    } else {
      buffer_.clear();
    }

    return msg;
  }

  void Start() override {
    // In this case, the client EG stream is not responsible for handshaking.
  }

  void Accept() override {
    stream_.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::response_type& res) {
          res.set(beast::http::field::server,
                  std::string(BOOST_BEAST_VERSION_STRING) +
                      " WebsocketEvergreenServer");
        }));

    boost::system::error_code ec;
    stream_.accept(ec);
    if (ec) {
      status_ = absl::InternalError(ec.message());
    }
  }

  void HalfClose() override {
    if (!status_.ok()) {
      return;
    }

    boost::system::error_code ec;
    stream_.close(beast::websocket::close_code::normal, ec);
    if (ec) {
      last_send_status_ = absl::InternalError(ec.message());
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
        io_context_(io_context != nullptr ? io_context : &GetGlobalIOContext()),
        acceptor_(std::make_unique<tcp::acceptor>(
            *io_context_,
            tcp::endpoint{asio::ip::make_address(address), port})) {}

  ~WebsocketEvergreenServer() {
    if (main_loop_ != nullptr) {
      Cancel().IgnoreError();
      Join().IgnoreError();
    }
  }

  void Run() {
    main_loop_ = concurrency::NewTree({}, [this]() {
      while (!concurrency::Cancelled()) {
        tcp::socket socket{*io_context_};

        if (boost::system::error_code ec = acceptor_->accept(socket, ec); !ec) {
          beast::websocket::stream<tcp::socket> stream(socket);
          auto connection = service_->EstablishConnection(
              std::make_shared<WebsocketEvergreenStream>(std::move(stream)));
          if (!connection.ok()) {
            status_ = connection.status();
            break;
          }
        } else {
          switch (ec.value()) {
            case boost::system::errc::operation_canceled:
              status_ = absl::OkStatus();
              break;
            default:
              status_ = absl::InternalError(ec.message());
              break;
          }
          // Any code reaching here means the service is shutting down.
          break;
        }
      }
    });
  }

  absl::Status Cancel() {
    main_loop_->Cancel();

    if (boost::system::error_code ec = acceptor_->close(ec); ec) {
      status_ = absl::InternalError(ec.message());
      return status_;
    }

    return absl::OkStatus();
  }

  absl::Status Join() {
    main_loop_->Join();
    return status_;
  }

 private:
  eglt::Service* absl_nonnull service_;

  asio::io_context* io_context_;
  std::unique_ptr<tcp::acceptor> acceptor_;

  std::unique_ptr<concurrency::Fiber> main_loop_;
  absl::Status status_;
};

inline std::unique_ptr<WebsocketEvergreenStream> MakeWebsocketEvergreenStream(
    std::string_view address = "0.0.0.0", uint16_t port = 20000,
    std::string_view target = "/", asio::io_context* io_context = nullptr) {
  if (io_context == nullptr) {
    io_context = &GetGlobalIOContext();
  }

  tcp::resolver resolver(*io_context);
  const auto endpoints = resolver.resolve(address, std::to_string(port));

  beast::websocket::stream<tcp::socket> ws_stream(*io_context);

  boost::system::error_code ec;
  const auto endpoint =
      boost::asio::connect(beast::get_lowest_layer(ws_stream), endpoints, ec);
  if (ec) {
    return nullptr;
  }

  // Set a decorator to change the User-Agent of the handshake
  ws_stream.set_option(beast::websocket::stream_base::decorator(
      [](beast::websocket::request_type& req) {
        req.set(beast::http::field::user_agent,
                std::string(BOOST_BEAST_VERSION_STRING) +
                    " WebsocketEvergreenClient");
      }));

  ws_stream.handshake(absl::StrFormat("%s:%d", address, endpoint.port()),
                      target, ec);
  if (ec) {
    return nullptr;
  }

  std::string session_id = GenerateUUID4();
  return std::make_unique<WebsocketEvergreenStream>(std::move(ws_stream),
                                                    session_id);
}

}  // namespace eglt::sdk

#endif  // EGLT_SDK_SERVING_WEBSOCKETS_H_