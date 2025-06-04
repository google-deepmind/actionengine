#include <iostream>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/sdk/webrtc.h"

void ServeHelloEvergreen() {
  while (true) {
    auto status_or_stream = eglt::sdk::AcceptStreamFromSignalling(
        "server", "demos.helena.direct", 19000);
    if (!status_or_stream.ok()) {
      LOG(ERROR) << "Failed to accept WebRTC stream: "
                 << status_or_stream.status().message();
      break;
    }

    const auto stream = *std::move(status_or_stream);
    LOG(INFO) << "Server accepted a new WebRTC stream with ID: "
              << stream->GetId();

    eglt::SessionMessage message;
    message.node_fragments.push_back({
        .id = "test",
        .chunk = eglt::Chunk{.metadata =
                                 eglt::ChunkMetadata{.mimetype = "text/plain",
                                                     .timestamp = absl::Now()},
                             .data = "Hello, Evergreen!"},
        .seq = 0,
        .continued = false,
    });

    if (auto status = stream->Send(std::move(message)); !status.ok()) {
      LOG(ERROR) << "Failed to send message: " << status.message();
    }
  }
}

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  eglt::concurrency::Fiber server_fiber([]() { ServeHelloEvergreen(); });

  eglt::concurrency::SleepFor(absl::Milliseconds(200));

  while (true) {
    auto status_or_stream = eglt::sdk::StartStreamWithSignalling(
        eglt::GenerateUUID4(), "server", "demos.helena.direct", 19000);
    if (!status_or_stream.ok()) {
      LOG(ERROR) << "Failed to start WebRTC stream: "
                 << status_or_stream.status().message();
      break;
    }

    const auto stream = std::move(*std::move(status_or_stream));
    LOG(INFO) << "Client started a WebRTC stream with ID: " << stream->GetId();

    const auto message = stream->Receive();
    if (!message) {
      LOG(ERROR) << "Failed to receive message";
      break;
    }

    LOG(INFO) << "Received message: \n" << *message;
  }

  server_fiber.Join();
  return 0;
}