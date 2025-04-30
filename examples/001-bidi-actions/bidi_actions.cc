// ------------------------------------------------------------------------------
// This example will run an echo server and an echo client in separate threads.
// The client will connect to the server and send a text message. The server
// will NOT send it back to the client. Instead, it will call a second action,
// which will make the client print the text of the message. This example
// demonstrates how to use bidirectional actions.
//
// You can run this example with:
// blaze run //third_party/eglt/examples:bidi_actions_cc
//
// There is a similar example with a single-turn action, where you can read
// more details about action usage in general.
// ------------------------------------------------------------------------------

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

#include <eglt/absl_headers.h>
#include <eglt/actions/action.h>
#include <eglt/data/eg_structs.h>
#include <eglt/sdk/serving/websockets.h>
#include <eglt/service/service.h>

ABSL_FLAG(int32_t, port, 20000, "Port to bind to.");

double kDelayBetweenWords = 0.1;

// Simply some type aliases to make the code more readable.
using Action = eglt::Action;
using ActionRegistry = eglt::ActionRegistry;
using Chunk = eglt::Chunk;
using Service = eglt::Service;

std::string ToLower(std::string_view text);

absl::Status RunPrint(const std::shared_ptr<Action>& action) {
  auto text = action->GetInput("text");
  text->SetReaderOptions(/*ordered=*/true,
                         /*remove_chunks=*/true);

  while (true) {
    std::optional<std::string> word = text->Next<std::string>();
    if (!word.has_value()) {
      break;
    }
    std::cout << *word << std::flush;
  }

  return absl::OkStatus();
}

absl::Status RunBidiEcho(const std::shared_ptr<Action>& action) {
  const auto print_action = action->MakeActionInSameSession("print_text");
  if (auto status = print_action->Call(); !status.ok()) {
    return status;
  }

  const auto echo_input = action->GetInput("text");
  echo_input->SetReaderOptions(/*ordered=*/true,
                               /*remove_chunks=*/true);

  const auto print_input = print_action->GetInput("text");

  absl::BitGen generator;
  while (true) {
    std::optional<std::string> word = echo_input->Next<std::string>();
    if (!word.has_value()) {
      break;
    }
    print_input->Put(*word).IgnoreError();

    double jitter = absl::Uniform(generator, -kDelayBetweenWords / 2,
                                  kDelayBetweenWords / 2);
    absl::SleepFor(absl::Seconds(kDelayBetweenWords + jitter));
  }
  print_input->Put(eglt::EndOfStream()).IgnoreError();

  return absl::OkStatus();
}

ActionRegistry MakeActionRegistry() {
  ActionRegistry registry;

  registry.Register(/*name=*/"bidi_echo",
                    /*def=*/
                    {
                        .name = "bidi_echo",
                        .inputs = {{"text", "text/plain"}},
                        .outputs = {},
                    },
                    /*handler=*/RunBidiEcho);

  registry.Register(/*name=*/"print_text",
                    /*def=*/
                    {
                        .name = "print_text",
                        .inputs = {{"text", "text/plain"}},
                        .outputs = {},
                    },
                    /*handler=*/RunPrint);
  return registry;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  const uint16_t port = absl::GetFlag(FLAGS_port);
  auto action_registry = MakeActionRegistry();

  eglt::Service service(&action_registry);
  eglt::sdk::WebsocketEvergreenServer server(&service, "0.0.0.0", port);
  server.Run();

  eglt::sdk::WebsocketEvergreenClient client(eglt::RunSimpleEvergreenSession,
                                             action_registry);
  const auto connection = client.Connect("localhost", port);

  std::cout << absl::StrFormat(
      "Bidi actions. Enter a prompt, and the server will print it back with a "
      "delay of %.1f seconds between each word. For a fun experience, try "
      "copying and pasting a long text.\n",
      kDelayBetweenWords);

  while (true) {
    std::string prompt;
    std::cout << "Enter a prompt: ";
    std::getline(std::cin, prompt);

    if (absl::StartsWith(ToLower(prompt), "/q")) {
      break;
    }

    const auto action =
        eglt::MakeActionInConnection(*connection,
                                     /*action_name=*/"bidi_echo");

    if (const auto status = action->Call(); !status.ok()) {
      LOG(ERROR) << "Error: " << status << "\n";
      continue;
    }

    const auto text_input = action->GetInput("text");
    std::vector<std::string> words = absl::StrSplit(prompt, ' ');
    for (auto word : words) {
      if (const auto status = text_input->Put(absl::StrCat(word, " "));
          !status.ok()) {
        LOG(FATAL) << "Error: " << status;
      }
    }
    text_input->Put(eglt::EndOfStream()).IgnoreError();

    absl::SleepFor(absl::Seconds(kDelayBetweenWords * (words.size() + 2)));
    std::cout << std::endl;
  }

  client.Cancel();
  server.Cancel().IgnoreError();

  client.Join().IgnoreError();
  server.Join().IgnoreError();

  return 0;
}

std::string ToLower(std::string_view text) {
  std::string lower(text);
  std::transform(lower.begin(), lower.end(), lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return lower;
}
