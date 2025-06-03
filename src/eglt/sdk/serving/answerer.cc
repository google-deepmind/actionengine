#include <iostream>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/sdk/serving/webrtc.h"

void ServeHelloEvergreen() {
  while (true) {
    auto stream = eglt::sdk::AcceptStreamFromSignalling();
    LOG(INFO) << "Accepted new stream with ID: " << stream->GetId();

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
    eglt::concurrency::SleepFor(absl::Milliseconds(5));
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
    LOG(INFO) << "Started stream with ID: " << stream->GetId();
    const auto message = stream->Receive();
    if (!message) {
      LOG(ERROR) << "Failed to receive message";
      return 1;
    }

    LOG(INFO) << "Received message: " << *message;
    eglt::concurrency::SleepFor(absl::Milliseconds(200));
  }

  eglt::concurrency::SleepFor(absl::Milliseconds(200));

  server_fiber.Join();
  return 0;
}