#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <eglt/absl_headers.h>
#include <eglt/data/eg_structs.h>
#include <eglt/net/recoverable_stream.h>
#include <eglt/nodes/async_node.h>
#include <eglt/stores/local_chunk_store.h>

using namespace eglt;

struct EvergreenHandshake {
  std::string session_id;
  std::string token;
};

inline absl::Status EgltAssignInto(const EvergreenHandshake& handshake,
                                   Chunk* chunk) {
  chunk->metadata = eglt::ChunkMetadata{
      .mimetype = "x-eglt;EvergreenHandshake",
      .timestamp = absl::Now(),
  };
  chunk->data = absl::StrCat(handshake.session_id, ":::", handshake.token);
  return absl::OkStatus();
}

inline absl::Status EgltAssignInto(const Chunk& chunk,
                                   EvergreenHandshake* handshake) {
  if (chunk.metadata.mimetype != "x-eglt;EvergreenHandshake") {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid mimetype: %v", chunk.metadata.mimetype));
  }

  // this validation is application-level logic
  const std::vector<std::string> parts = absl::StrSplit(chunk.data, ":::");
  if (parts.size() != 2) {
    return absl::InvalidArgumentError("Invalid data.");
  }

  handshake->session_id = parts[0];
  handshake->token = parts[1];

  return absl::OkStatus();
}

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
}