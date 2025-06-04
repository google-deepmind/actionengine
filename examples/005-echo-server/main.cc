#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include <eglt/absl_headers.h>
#include <eglt/actions/action.h>
#include <eglt/data/eg_structs.h>
#include <eglt/nodes/async_node.h>
#include <eglt/sdk/websockets.h>
#include <eglt/service/service.h>

ABSL_FLAG(uint16_t, port, 20000, "Port to bind to.");

// Simply some type aliases to make the code more readable.
using Action = eglt::Action;
using ActionRegistry = eglt::ActionRegistry;
using Chunk = eglt::Chunk;
using Service = eglt::Service;

absl::Status RunEcho(const std::shared_ptr<Action>& action) {
  auto input_text = action->GetNode("text");
  input_text->SetReaderOptions(/*ordered=*/true,
                               /*remove_chunks=*/true);
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
  action->GetNode("response") << eglt::EndOfStream();

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
  ActionRegistry action_registry = MakeActionRegistry();
  eglt::Service service(&action_registry);
  eglt::sdk::WebsocketEvergreenServer server(&service, "0.0.0.0", port);
  server.Run();
  server.Join().IgnoreError();

  return 0;
}
