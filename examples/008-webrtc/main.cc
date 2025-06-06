#include <iostream>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/sdk/webrtc.h"

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  eglt::Service service;
  eglt::sdk::WebRtcEvergreenServer server(
      &service, "0.0.0.0", 20000,
      /*signalling_address=*/"demos.helena.direct", /*signalling_port=*/19000);
  server.Run();
  eglt::concurrency::SleepFor(absl::Milliseconds(200));

  for (int i = 0; i < 10; ++i) {
    auto status_or_stream = eglt::sdk::StartStreamWithSignalling(
        eglt::GenerateUUID4(), "server", "demos.helena.direct", 19000);
    if (!status_or_stream.ok()) {
      LOG(ERROR) << "Failed to start WebRTC stream: "
                 << status_or_stream.status().message();
      return 1;
    }

    const auto stream = std::move(*std::move(status_or_stream));
    LOG(INFO) << "Client started a WebRTC stream with ID: " << stream->GetId();

    eglt::SessionMessage session_message;
    session_message.node_fragments.push_back({
        .id = "test",
        .chunk =
            eglt::Chunk{
                .metadata = eglt::ChunkMetadata{.mimetype = "text/plain",
                                                .timestamp = absl::Now()},
                .data = absl::StrFormat("Hello, Evergreen from client %v!", i)},
        .seq = 0,
        .continued = false,
    });

    stream->Send(std::move(session_message)).IgnoreError();
    // eglt::concurrency::SleepFor(absl::Seconds(0.5));
  }

  return 0;
}
