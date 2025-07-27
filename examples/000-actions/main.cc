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
#include <eglt/actions/action.h>
#include <eglt/data/eg_structs.h>
#include <eglt/net/websockets/websockets.h>
#include <eglt/nodes/async_node.h>
#include <eglt/service/service.h>

ABSL_FLAG(uint16_t, port, 20000, "Port to bind to.");

// Simply some type aliases to make the code more readable.
using Action = eglt::Action;
using ActionRegistry = eglt::ActionRegistry;
using Chunk = eglt::Chunk;
using Service = eglt::Service;
using Session = eglt::Session;
using WireStream = eglt::WireStream;

// Server side implementation of the echo action. This is a simple example of
// how to implement an action handler. Every handler must take a shared_ptr to
// the Action object as an argument. This object provides accessors to input and
// output nodes (as streams), as well as to underlying Session and
// transport-level Evergreen stream.
absl::Status RunEcho(const std::shared_ptr<Action>& action) {
  // ----------------------------------------------------------------------------
  // Evergreen actions are asynchronous, so to read inputs, we need a streaming
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
  input_text->SetReaderOptions(/*ordered=*/true,
                               /*remove_chunks=*/true);

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
    // std::optional<Chunk> chunk = action->GetNode("text")->Next<Chunk>();
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
  action->GetNode("response") << eglt::EndOfStream();

  return absl::OkStatus();
}

// ----------------------------------------------------------------------------
// The Evergreen service takes care of the dispatching of nodes and actions.
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
std::string CallEcho(std::string_view text, Session* absl_nonnull session,
                     const std::shared_ptr<WireStream>& stream) {

  const auto echo = session->GetActionRegistry()->MakeAction(
      /*action_key=*/"echo");
  echo->BindNodeMap(session->GetNodeMap());
  echo->BindSession(session);
  echo->BindStream(stream);

  // Evergreen actions are asynchronous, so we can call the action even before
  // supplying all the inputs. The server will run the action handler in a
  // separate fiber, which will just block on reading an input which is still
  // not available or being streamed. This _does_ mean that if part of the
  // action's logic does not depend on the input, it will be executed before
  // the input is available, no additional effort is needed, and this applies
  // per input, not all at once.

  // This is NOT the way to check if the action finished successfully. This
  // status just indicates that the action was called (action message was sent).
  if (const auto status = echo->Call(); !status.ok()) {
    LOG(ERROR) << "Failed to call action: " << status;
    return "";
  }

  // Send the input to the action. These are not necessarily blocking calls.
  // Chunks will be buffered (to an extent) and sent in the background.
  // This makes it possible to start streaming multiple inputs at the same time
  // (though this is not the case here).
  // echo->GetNode("text")
  //     ->PutChunk(Chunk{.metadata = {.mimetype = "text/plain"},
  //                      .data = std::string(text)},
  //                /*seq=*/0,
  //                /*final=*/true)
  //     .IgnoreError();
  echo->GetNode("text") << Chunk{.metadata = {.mimetype = "text/plain"},
                                 .data = std::string(text)}
                        << eglt::EndOfStream();

  // We will read the output to a string stream.
  std::ostringstream response;

  // The action handler will write to the output stream asynchronously, so all
  // we need to do is to read the output(s) until the end of stream(s).
  // Each of the reads below will block until the next chunk is available.
  std::optional<Chunk> response_chunk;
  while (true) {
    *echo->GetNode("response") >> response_chunk;
    if (!response_chunk.has_value()) {
      break;
    }
    if (response_chunk->metadata.mimetype == "text/plain") {
      response << response_chunk->data;
    }
    // We are not checking the status here, but in general, we should.
    // This is done automatically by the AsyncNode methods, but not by the >>
    // operator.
  }

  return response.str();
}

int main(int argc, char** argv) {
  absl::InstallFailureSignalHandler({});
  absl::ParseCommandLine(argc, argv);

  const uint16_t port = absl::GetFlag(FLAGS_port);
  // We will use the same action registry for the server and the client.
  ActionRegistry action_registry = MakeActionRegistry();
  // This is enough to run the server. Notice how Service is decoupled from
  // the server implementation. We could have used any other implementation,
  // such as gRPC or WebSockets, if they provide an implementation of
  // WireStream and (de)serialization of base types (eg_structs.h) from
  // and into their transport-level messages. There is an example of using
  // zmq streams and msgpack messages in one of the showcases.
  eglt::Service service(&action_registry);
  eglt::net::WebsocketEvergreenServer server(&service, "0.0.0.0", port);
  server.Run();

  eglt::NodeMap node_map;
  eglt::Session session(&node_map, &action_registry);
  auto stream = eglt::net::MakeWebsocketWireStream();
  if (!stream.ok()) {
    LOG(ERROR) << "Failed to create WebsocketWireStream: " << stream.status();
    return 1;
  }
  auto shared_stream = std::shared_ptr(*std::move(stream));
  session.DispatchFrom(shared_stream);

  std::string text = "test text to skip the long startup logs";
  std::cout << "Sending: " << text << std::endl;
  auto response = CallEcho(text, &session, shared_stream);
  std::cout << "Received: " << response << std::endl;

  std::cout << "This is an example with an Evergreen server and a client "
               "performing an echo action. You can type some text and it will "
               "be echoed back. Type /quit to exit.\n"
            << std::endl;

  while (text != "/quit") {
    std::cout << "Enter text: ";
    std::getline(std::cin, text);
    response = CallEcho(text, &session, shared_stream);
    std::cout << "Received: " << response << "\n" << std::endl;
  }

  shared_stream->HalfClose();

  server.Cancel().IgnoreError();
  server.Join().IgnoreError();

  return 0;
}
