#include <thread>

#include <eglt/actions/action.h>
#include <eglt/sdk/serving/websockets.h>
#include <eglt/service/service.h>

void ServerThread() {
  eglt::ActionRegistry registry;
  eglt::Service service(&registry);
  eglt::sdk::WebsocketEvergreenServer server(&service);
  server.Run();
  DLOG(INFO) << "server running";
  DLOG(INFO) << server.Join();
}

int main() {
  std::thread server(ServerThread);
  eglt::ActionRegistry registry;

  const auto client_stream =
      eglt::sdk::MakeWebsocketClientEvergreenStream("localhost", 20000);

  eglt::NodeMap node_map;
  eglt::AsyncNode* node = node_map.Get("prompts");
  node->BindWriterStream(client_stream.get());

  while (true) {
    *node << "hello!";
    eglt::concurrency::SleepFor(absl::Seconds(5));
  }

  *node << "hello!" << eglt::EndOfStream();

  // eglt::concurrency::SleepFor(absl::Seconds(5));
  server.join();
}