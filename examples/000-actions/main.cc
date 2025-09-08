// ------------------------------------------------------------------------------
// This example will run an echo server and an echo client in separate threads.
// The client will connect to the server and send a message. The server will
// echo the message back to the client. Subsequently, you will be able to type
// messages into the client and see them echoed back.
// ------------------------------------------------------------------------------

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
#include <absl/log/check.h>
#include <actionengine/actions/action.h>
#include <actionengine/data/types.h>
#include <actionengine/net/webrtc/server.h>
#include <actionengine/net/webrtc/wire_stream.h>
#include <actionengine/net/websockets/websockets.h>
#include <actionengine/nodes/async_node.h>
#include <actionengine/service/service.h>
#include <actionengine/util/random.h>

ABSL_FLAG(uint16_t, port, 20000, "Port to bind to.");

// Simply some type aliases to make the code more readable.
using Action = act::Action;
using ActionRegistry = act::ActionRegistry;
using Chunk = act::Chunk;
using Service = act::Service;
using Session = act::Session;
using WireStream = act::WireStream;

// Server side implementation of the echo action. This is a simple example of
// how to implement an action handler. Every handler must take a shared_ptr to
// the Action object as an argument. This object provides accessors to input and
// output nodes (as streams), as well as to underlying Session and
// transport-level ActionEngine stream.
absl::Status RunEcho(const std::shared_ptr<Action>& action) {
  // ----------------------------------------------------------------------------
  // ActionEngine actions are asynchronous, so to read inputs, we need a streaming
  // reader. Conversely, to write outputs, we need a streaming writer. The
  // .GetNode() method return an instance of the
  // AsyncNode class, which combines a reader and a writer into a single object.
  // ----------------------------------------------------------------------------
  // The call to .SetReaderOptions() is optional. It allows to control how the
  // reader behaves:
  // - if ordered is set to true, the reader will sort chunks by sequence number
  //   in their respective NodeFragment. Default is false.
  // - if remove_chunks is set to true, chunks will be removed from the
  //   underlying store as they are read. Default is true.
  // ----------------------------------------------------------------------------
  auto input_text = action->GetNode("text");
  input_text->SetReaderOptions({.ordered = true, .remove_chunks = true});

  // ----------------------------------------------------------------------------
  // The while loop below reads all chunks from the input stream and writes
  // them to the output stream.
  // ----------------------------------------------------------------------------
  std::optional<Chunk> chunk;
  while (true) {
    // Read the next chunk from the input stream. If we reach the end of the
    // stream, the chunk will be nullopt. If we did not need to control the
    // order of chunks, we could have used the >> operator directly like this :
    // *action->GetNode("text") >> chunk;
    // Equivalent operations are supported as AsyncNode methods, in this case:
    // std::optional<Chunk> chunk = action->GetNode("text")->NextOrDie<Chunk>();
    *input_text >> chunk;

    // End of stream (everything was read successfully)
    if (!chunk.has_value()) {
      break;
    }

    // Check if there was an error reading the input stream. If so, we can
    // terminate the action and return the error status. This status is updated
    // after every read.
    if (auto status = input_text->GetReaderStatus(); !status.ok()) {
      LOG(ERROR) << "Failed to read input: " << status;
      return status;
    }

    // Write the chunk to the output stream. In this case, we are writing
    // directly into the temporary variable for convenience.
    action->GetNode("response") << *chunk;
  }

  // This is necessary and indicates the end of stream.
  action->GetNode("response") << act::EndOfStream();

  return absl::OkStatus();
}

// ----------------------------------------------------------------------------
// The ActionEngine service takes care of the dispatching of nodes and actions.
// Specifically how it does this is customizable and is shown in other examples.
// The default implementation only manages nodes in the lifetime of a single
// connection, and runs actions from the action registry. Therefore, we need to
// create an action registry for the server.
// ----------------------------------------------------------------------------
ActionRegistry MakeActionRegistry() {
  ActionRegistry registry;

  // This is hopefully self-explanatory. The name of the action is arbitrary,
  // but it must be unique within the registry. The definition of the action
  // consists of the name, input and output nodes. The handler is a function
  // that takes a shared_ptr to the Action object as an argument and implements
  // the action logic. There must be no two nodes with the same name within the
  // same action, even if they are an input and an output.
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

// ----------------------------------------------------------------------------
// This is the client side implementation of the echo action. It can be viewed
// as a wrapper, because it invokes the asynchronous action handler and
// implements the synchronous client logic which waits for the response and
// just gives it back as a string, not a stream.
// ----------------------------------------------------------------------------
absl::StatusOr<std::string> CallEcho(
    std::string_view text, Session* absl_nonnull session,
    const std::shared_ptr<WireStream>& stream) {

  const auto echo = session->GetActionRegistry()->MakeAction(
      /*name=*/"echo");
  echo->BindNodeMap(session->GetNodeMap());
  echo->BindSession(session);
  echo->BindStream(stream.get());

  if (const auto status = echo->Call(); !status.ok()) {
    LOG(ERROR) << "Failed to call action: " << status;
    return "";
  }

  echo->GetInput("text") << Chunk{.metadata =
                                      act::ChunkMetadata{.mimetype =
                                                             "text/plain"},
                                  .data = std::string(text)}
                         << act::EndOfStream();

  std::ostringstream response;
  while (true) {
    const auto response_node = echo->GetOutput("response");
    ASSIGN_OR_RETURN(std::optional<Chunk> response_chunk,
                     response_node->Next(absl::Seconds(5)));
    if (!response_chunk.has_value()) {
      // End of stream, we are done.
      break;
    }
    if (response_chunk->GetMimetype() == "text/plain") {
      response << response_chunk->data;
    }
  }

  RETURN_IF_ERROR(echo->Await());

  return response.str();
}

absl::Status Main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  const uint16_t port = absl::GetFlag(FLAGS_port);
  // We will use the same action registry for the server and the client.
  ActionRegistry action_registry = MakeActionRegistry();
  // This is enough to run the server. Notice how Service is decoupled from
  // the server implementation. We could have used any other implementation,
  // such as gRPC or WebSockets, if they provide an implementation of
  // WireStream and (de)serialization of base types (types.h) from
  // and into their transport-level messages. There is an example of using
  // zmq streams and msgpack messages in one of the showcases.
  act::Service service(&action_registry);
  act::net::WebRtcServer server(&service, "127.0.0.1", port,
                                /*signalling_address=*/"demos.helena.direct",
                                /*signalling_port=*/19000,
                                /*signalling_identity=*/"server");
  server.Run();
  act::SleepFor(absl::Seconds(0.2));

  act::NodeMap node_map;
  act::Session session(&node_map, &action_registry);
  std::string identity = act::GenerateUUID4();
  LOG(INFO) << "Identity: " << identity;
  ASSIGN_OR_RETURN(std::shared_ptr<act::WireStream> stream,
                   act::net::StartStreamWithSignalling(
                       identity, "server", "demos.helena.direct", 19000));

  session.DispatchFrom(stream);

  std::string text = "test text to skip the long startup logs";
  std::cout << "Sending: " << text << std::endl;
  ASSIGN_OR_RETURN(std::string response, CallEcho(text, &session, stream));
  std::cout << "Received: " << response << std::endl;

  std::cout << "This is an example with an ActionEngine server and a client "
               "performing an echo action. You can type some text and it will "
               "be echoed back. Type /quit to exit.\n"
            << std::endl;

  while (text != "/quit") {
    std::cout << "Enter text: ";
    std::getline(std::cin, text);
    ASSIGN_OR_RETURN(response, CallEcho(text, &session, stream));
    std::cout << "Received: " << response << "\n" << std::endl;
  }

  stream->HalfClose();

  RETURN_IF_ERROR(server.Cancel());
  RETURN_IF_ERROR(server.Join());

  return absl::OkStatus();
}

int main(int argc, char** argv) {
  absl::Status status = Main(argc, argv);
  if (!status.ok()) {
    LOG(ERROR) << "Error: " << status;
    return 1;
  }
  return 0;
}
