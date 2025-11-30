#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include <absl/debugging/failure_signal_handler.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <actionengine/actions/action.h>
#include <actionengine/data/types.h>
#include <actionengine/net/webrtc/server.h>
#include <actionengine/net/webrtc/wire_stream.h>
#include <actionengine/nodes/async_node.h>
#include <actionengine/service/service.h>

ABSL_FLAG(uint16_t, port, 20000, "Port to bind to.");
ABSL_FLAG(std::string, identity, "server", "WebRTC signalling identity.");

// Simply some type aliases to make the code more readable.
using Action = act::Action;
using ActionRegistry = act::ActionRegistry;
using Chunk = act::Chunk;
using Service = act::Service;

absl::Status RunEcho(const std::shared_ptr<Action>& action) {
  const auto input_text = action->GetNode("text");
  input_text->SetReaderOptions({.ordered = true, .remove_chunks = true});
  std::optional<Chunk> chunk;
  while (true) {
    *input_text >> chunk;

    if (!chunk.has_value()) {
      break;
    }

    if (auto status = input_text->GetReaderStatus(); !status.ok()) {
      LOG(ERROR) << "Failed to read input: " << status;
      return status;
    }

    action->GetNode("response") << *chunk;
  }

  // This is necessary and indicates the end of stream.
  action->GetNode("response") << act::EndOfStream();

  return absl::OkStatus();
}

ActionRegistry MakeActionRegistry() {
  ActionRegistry registry;

  registry.Register(/*name=*/"echo",
                    /*schema=*/
                    {
                        .name = "echo",
                        .inputs = {{"text", "text/plain"}},
                        .outputs = {{"response", "text/plain"}},
                    },
                    /*handler=*/RunEcho);
  return registry;
}

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  const uint16_t port = absl::GetFlag(FLAGS_port);
  const std::string identity = absl::GetFlag(FLAGS_identity);

  ActionRegistry action_registry = MakeActionRegistry();
  act::Service service(&action_registry);
  act::net::WebRtcServer server(&service, "0.0.0.0", port,
                                /*signalling_address=*/"actionengine.dev",
                                /*signalling_port=*/19000,
                                /*signalling_identity=*/identity);
  // act::net::WebsocketServer server(&service, "0.0.0.0", port);
  server.Run();
  server.Join().IgnoreError();

  return 0;
}
