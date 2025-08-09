#include <iostream>
#include <memory>

#include <absl/debugging/failure_signal_handler.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <actionengine/net/webrtc/server.h>
#include <actionengine/net/webrtc/wire_stream.h>

ABSL_FLAG(std::string, webrtc_signalling_address, "demos.helena.direct",
          "Signalling address for WebRTC connections.");

ABSL_FLAG(uint16_t, webrtc_signalling_port, 19000,
          "Signalling port for WebRTC connections.");

ABSL_FLAG(std::string, webrtc_signalling_identity, "server",
          "Signalling identity for WebRTC connections. "
          "This is used to identify the server in the signalling process.");

ABSL_FLAG(std::string, webrtc_bind_address, "0.0.0.0",
          "Address for WebRTC connections. This is the address on which the "
          "server will listen for incoming WebRTC connections.");

ABSL_FLAG(uint16_t, webrtc_port, 20000,
          "Port for WebRTC connections. This is the port on which the server "
          "will listen for incoming WebRTC connections.");

ABSL_FLAG(
    std::vector<act::net::TurnServer>, webrtc_turn_servers, {},
    "List of TURN servers to use for WebRTC connections. Format: "
    "username1:password1@hostname1:port1,username2:password2@hostname2:port2");

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  act::Service service;

  act::net::RtcConfig rtc_config;
  rtc_config.turn_servers = absl::GetFlag(FLAGS_webrtc_turn_servers);
  act::net::WebRtcServer server(
      &service, /*address=*/absl::GetFlag(FLAGS_webrtc_bind_address),
      /*port=*/absl::GetFlag(FLAGS_webrtc_port),
      /*signalling_address=*/absl::GetFlag(FLAGS_webrtc_signalling_address),
      /*signalling_port=*/absl::GetFlag(FLAGS_webrtc_signalling_port),
      /*signalling_identity=*/absl::GetFlag(FLAGS_webrtc_signalling_identity),
      /*rtc_config=*/std::move(rtc_config));
  server.Run();

  act::SleepFor(absl::Milliseconds(200));

  for (int i = 0; i < 10; ++i) {
    auto status_or_stream = act::net::StartStreamWithSignalling(
        /*identity=*/act::GenerateUUID4(),
        /*peer_identity=*/
        absl::GetFlag(FLAGS_webrtc_signalling_identity),
        /*address=*/absl::GetFlag(FLAGS_webrtc_signalling_address),
        /*port=*/absl::GetFlag(FLAGS_webrtc_signalling_port));
    if (!status_or_stream.ok()) {
      LOG(ERROR) << "Failed to start WebRTC stream: "
                 << status_or_stream.status().message();
      return 1;
    }

    const auto stream = std::move(*std::move(status_or_stream));

    act::SessionMessage session_message;
    session_message.node_fragments.push_back({
        .id = "test",
        .chunk =
            act::Chunk{.metadata = act::ChunkMetadata{.mimetype = "text/plain",
                                                      .timestamp = absl::Now()},
                       .data = absl::StrFormat(
                           "Hello, Action Engine from client %v!", i)},
        .seq = 0,
        .continued = false,
    });

    absl::Status send_status = stream->Send(std::move(session_message));
    if (!send_status.ok()) {
      LOG(ERROR) << "Failed to send message over WebRTC stream: "
                 << send_status.message();
      return 1;
    }
    // act::SleepFor(absl::Seconds(0.5));
  }

  return 0;
}
