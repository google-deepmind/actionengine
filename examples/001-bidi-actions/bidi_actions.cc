// ------------------------------------------------------------------------------
// This example will run an echo server and an echo client in separate threads.
// The client will connect to the server and send a text message. The server
// will NOT send it back to the client. Instead, it will call a second action,
// which will make the client print the text of the message. This example
// demonstrates how to use bidirectional actions.
//
// You can run this example with:
// blaze run //third_party/actionengine/examples:bidi_actions_cc
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

#include <absl/debugging/failure_signal_handler.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <actionengine/actions/action.h>
#include <actionengine/data/types.h>
#include <actionengine/net/websockets/server.h>
#include <actionengine/net/websockets/wire_stream.h>
#include <actionengine/service/service.h>
#include <actionengine/util/status_macros.h>

ABSL_FLAG(int32_t, port, 20000, "Port to bind to.");

double kDelayBetweenWords = 0.1;

// Simply some type aliases to make the code more readable.
using Action = act::Action;
using ActionRegistry = act::ActionRegistry;
using Chunk = act::Chunk;
using Service = act::Service;
using Session = act::Session;
using WireStream = act::WireStream;

std::string ToLower(std::string_view text);

absl::Status RunPrint(const std::shared_ptr<Action>& action) {
  const auto text = action->GetInput("text");
  text->SetReaderOptions({.ordered = true, .remove_chunks = true});

  while (true) {
    ASSIGN_OR_RETURN(std::optional<std::string> word,
                     text->Next<std::string>(absl::Seconds(5)));
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
  echo_input->SetReaderOptions({.ordered = true, .remove_chunks = true});

  const auto print_input = print_action->GetInput("text");

  absl::BitGen generator;
  while (true) {
    ASSIGN_OR_RETURN(std::optional<std::string> word,
                     echo_input->Next<std::string>(absl::Seconds(5)));
    if (!word.has_value()) {
      break;
    }
    RETURN_IF_ERROR(print_input->Put(*word));

    const double jitter = absl::Uniform(generator, -kDelayBetweenWords / 2,
                                        kDelayBetweenWords / 2);
    act::SleepFor(absl::Seconds(kDelayBetweenWords + jitter));
  }
  RETURN_IF_ERROR(print_input->Put(act::EndOfStream()));

  return absl::OkStatus();
}

ActionRegistry MakeActionRegistry() {
  ActionRegistry registry;

  registry.Register(/*name=*/"bidi_echo",
                    /*schema=*/
                    {
                        .name = "bidi_echo",
                        .inputs = {{"text", "text/plain"}},
                        .outputs = {},
                    },
                    /*handler=*/RunBidiEcho);

  registry.Register(/*name=*/"print_text",
                    /*schema=*/
                    {
                        .name = "print_text",
                        .inputs = {{"text", "text/plain"}},
                        .outputs = {},
                    },
                    /*handler=*/RunPrint);
  return registry;
}

absl::Status Main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);
  const uint16_t port = absl::GetFlag(FLAGS_port);
  auto action_registry = MakeActionRegistry();

  act::Service service(&action_registry);
  act::net::WebsocketServer server(&service, "0.0.0.0", port);
  server.Run();

  act::NodeMap node_map;
  act::Session session(&node_map, &action_registry);
  ASSIGN_OR_RETURN(std::shared_ptr<act::WireStream> stream,
                   act::net::MakeWebsocketWireStream("localhost", port));

  session.DispatchFrom(stream);

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

    const auto action = action_registry.MakeAction("bidi_echo");
    action->BindNodeMap(&node_map);
    action->BindSession(&session);
    action->BindStream(stream.get());

    RETURN_IF_ERROR(action->Call());

    const auto text_input = action->GetInput("text");
    std::vector<std::string> words = absl::StrSplit(prompt, ' ');
    for (auto& word : words) {
      RETURN_IF_ERROR(text_input->Put(absl::StrCat(word, " ")));
    }
    RETURN_IF_ERROR(text_input->Put(act::EndOfStream()));

    act::SleepFor(absl::Seconds(kDelayBetweenWords *
                                (static_cast<double>(words.size()) + 2.0)));
    std::cout << std::endl;
  }

  stream->HalfClose();

  RETURN_IF_ERROR(server.Cancel());
  RETURN_IF_ERROR(server.Join());

  return absl::OkStatus();
}

int main(int argc, char** argv) {
  CHECK_OK(Main(argc, argv))
      << "Failed to run the bidi actions example. See logs for details.";
  return 0;
}

std::string ToLower(std::string_view text) {
  std::string lower(text);
  std::transform(lower.begin(), lower.end(), lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return lower;
}
