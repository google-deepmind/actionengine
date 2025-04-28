#include <thread>

#include <eglt/actions/action.h>
#include <eglt/sdk/serving/websockets.h>
#include <eglt/service/service.h>

int main() {
  eglt::ActionRegistry registry;

  eglt::Service service(&registry);
  eglt::sdk::WebsocketEvergreenServer server(&service, /*address=*/"0.0.0.0",
                                             /*port=*/20000);
  server.Run();

  const auto client_stream =
      eglt::sdk::MakeWebsocketClientEvergreenStream("localhost", 20000);
  client_stream->Start();

  eglt::NodeMap node_map;
  eglt::AsyncNode* node = node_map.Get("prompts");
  node->BindWriterStream(client_stream.get());

  while (true) {
    *node << "hello!";
    eglt::concurrency::SleepFor(absl::Seconds(5));
  }

  server.Join().IgnoreError();
}