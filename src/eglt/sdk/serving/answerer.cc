

#include <iostream>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/sdk/serving/webrtc.h"

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  while (true) {
    auto stream = eglt::sdk::AcceptStreamFromSignalling();

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

    stream->Send(std::move(message)).IgnoreError();
  }
}