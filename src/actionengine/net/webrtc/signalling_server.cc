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

// #include <algorithm>
// #include <cstdlib>
// #include <iostream>
// #include <memory>
// #include <string>
// #include <thread>
// #include <vector>
//
// #include <boost/asio/dispatch.hpp>
// #include <boost/asio/ssl.hpp>
// #include <boost/asio/strand.hpp>
// #include <boost/beast/core.hpp>
// #include <boost/beast/websocket.hpp>
// #include <boost/beast/websocket/ssl.hpp>
//
// // Report a failure
// void fail(boost::beast::error_code ec, char const* what) {
//   std::cerr << what << ": " << ec.message() << "\n";
// }
//
// // Echoes back all received WebSocket messages
// class session : public std::enable_shared_from_this<session> {
//   boost::beast::websocket::stream<
//       boost::asio::ssl::stream<boost::beast::tcp_stream>>
//       ws_;
//   boost::beast::flat_buffer buffer_;
//
//  public:
//   // Take ownership of the socket
//   session(boost::asio::ip::tcp::socket&& socket, boost::asio::ssl::context& ctx)
//       : ws_(std::move(socket), ctx) {}
//
//   // Get on the correct executor
//   void run() {
//     // We need to be executing within a strand to perform async operations
//     // on the I/O objects in this session. Although not strictly necessary
//     // for single-threaded contexts, this example code is written to be
//     // thread-safe by default.
//     boost::asio::dispatch(
//         ws_.get_executor(),
//         boost::beast::bind_front_handler(&session::on_run, shared_from_this()));
//   }
//
//   // Start the asynchronous operation
//   void on_run() {
//     // Set the timeout.
//     boost::beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
//
//     // Perform the SSL handshake
//     ws_.next_layer().async_handshake(
//         boost::asio::ssl::stream_base::server,
//         boost::beast::bind_front_handler(&session::on_handshake,
//                                          shared_from_this()));
//   }
//
//   void on_handshake(boost::beast::error_code ec) {
//     if (ec)
//       return fail(ec, "handshake");
//
//     // Turn off the timeout on the tcp_stream, because
//     // the websocket stream has its own timeout system.
//     boost::beast::get_lowest_layer(ws_).expires_never();
//
//     // Set suggested timeout settings for the websocket
//     ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(
//         boost::beast::role_type::server));
//
//     // Set a decorator to change the Server of the handshake
//     ws_.set_option(boost::beast::websocket::stream_base::decorator(
//         [](boost::beast::websocket::response_type& res) {
//           res.set(boost::beast::http::field::server,
//                   std::string(BOOST_BEAST_VERSION_STRING) +
//                       " websocket-server-async-ssl");
//         }));
//
//     // Accept the websocket handshake
//     ws_.async_accept(boost::beast::bind_front_handler(&session::on_accept,
//                                                       shared_from_this()));
//   }
//
//   void on_accept(boost::beast::error_code ec) {
//     if (ec)
//       return fail(ec, "accept");
//
//     // Read a message
//     do_read();
//   }
//
//   void do_read() {
//     // Read a message into our buffer
//     ws_.async_read(buffer_, boost::beast::bind_front_handler(
//                                 &session::on_read, shared_from_this()));
//   }
//
//   void on_read(boost::beast::error_code ec, std::size_t bytes_transferred) {
//     boost::ignore_unused(bytes_transferred);
//
//     // This indicates that the session was closed
//     if (ec == boost::beast::websocket::error::closed)
//       return;
//
//     if (ec)
//       return fail(ec, "read");
//
//     // Echo the message
//     ws_.text(ws_.got_text());
//     ws_.async_write(buffer_.data(),
//                     boost::beast::bind_front_handler(&session::on_write,
//                                                      shared_from_this()));
//   }
//
//   void on_write(boost::beast::error_code ec, std::size_t bytes_transferred) {
//     boost::ignore_unused(bytes_transferred);
//
//     if (ec)
//       return fail(ec, "write");
//
//     // Clear the buffer
//     buffer_.consume(buffer_.size());
//
//     // Do another read
//     do_read();
//   }
// };
//
// //------------------------------------------------------------------------------
//
// // Accepts incoming connections and launches the sessions
// class listener : public std::enable_shared_from_this<listener> {
//   boost::asio::io_context& ioc_;
//   boost::asio::ssl::context& ctx_;
//   boost::asio::ip::tcp::acceptor acceptor_;
//
//  public:
//   listener(boost::asio::io_context& ioc, boost::asio::ssl::context& ctx,
//            boost::asio::ip::tcp::endpoint endpoint)
//       : ioc_(ioc), ctx_(ctx), acceptor_(boost::asio::make_strand(ioc)) {
//     boost::beast::error_code ec;
//
//     // Open the acceptor
//     acceptor_.open(endpoint.protocol(), ec);
//     if (ec) {
//       fail(ec, "open");
//       return;
//     }
//
//     // Allow address reuse
//     acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
//     if (ec) {
//       fail(ec, "set_option");
//       return;
//     }
//
//     // Bind to the server address
//     acceptor_.bind(endpoint, ec);
//     if (ec) {
//       fail(ec, "bind");
//       return;
//     }
//
//     // Start listening for connections
//     acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
//     if (ec) {
//       fail(ec, "listen");
//       return;
//     }
//   }
//
//   // Start accepting incoming connections
//   void run() { do_accept(); }
//
//  private:
//   void do_accept() {
//     // The new connection gets its own strand
//     acceptor_.async_accept(boost::asio::make_strand(ioc_),
//                            boost::beast::bind_front_handler(
//                                &listener::on_accept, shared_from_this()));
//   }
//
//   void on_accept(boost::beast::error_code ec,
//                  boost::asio::ip::tcp::socket socket) {
//     if (ec) {
//       fail(ec, "accept");
//     } else {
//       // Create the session and run it
//       std::make_shared<session>(std::move(socket), ctx_)->run();
//     }
//
//     // Accept another connection
//     do_accept();
//   }
// };
//
// //------------------------------------------------------------------------------
//
// int main(int argc, char* argv[]) {
//   // Check command line arguments.
//   if (argc != 4) {
//     std::cerr
//         << "Usage: websocket-server-async-ssl <address> <port> <threads>\n"
//         << "Example:\n"
//         << "    websocket-server-async-ssl 0.0.0.0 8080 1\n";
//     return EXIT_FAILURE;
//   }
//   auto const address = boost::asio::ip::make_address(argv[1]);
//   auto const port = static_cast<unsigned short>(std::atoi(argv[2]));
//   auto const threads = std::max<int>(1, std::atoi(argv[3]));
//
//   // The io_context is required for all I/O
//   boost::asio::io_context ioc{threads};
//
//   // The SSL context is required, and holds certificates
//   boost::asio::ssl::context ctx{boost::asio::ssl::context::tlsv13};
//
//   // This holds the self-signed certificate used by the server
//   load_server_certificate(ctx);
//
//   // Create and launch a listening port
//   std::make_shared<listener>(ioc, ctx,
//                              boost::asio::ip::tcp::endpoint{address, port})
//       ->run();
//
//   // Run the I/O service on the requested number of threads
//   std::vector<std::thread> v;
//   v.reserve(threads - 1);
//   for (auto i = threads - 1; i > 0; --i)
//     v.emplace_back([&ioc] { ioc.run(); });
//   ioc.run();
//
//   return EXIT_SUCCESS;
// }