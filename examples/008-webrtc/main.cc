#include <iostream>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/sdk/webrtc.h"

void ServeHelloEvergreen() {
  while (true) {
    auto stream = eglt::sdk::AcceptStreamFromSignalling();
    if (!stream) {
      LOG(ERROR) << "Failed to accept WebRTC stream.";
      break;
    }
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
    const auto stream =
        eglt::sdk::StartStreamWithSignalling(eglt::GenerateUUID4());
    if (!stream) {
      LOG(ERROR) << "Failed to start WebRTC stream.";
      break;
    }
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