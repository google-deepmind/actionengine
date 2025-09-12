// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ACTIONENGINE_ACTIONS_ACTION_H_
#define ACTIONENGINE_ACTIONS_ACTION_H_

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/actions/registry.h"
#include "actionengine/actions/schema.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store_reader.h"

namespace act {
class Session;
}  // namespace act

/** @file
 *  @brief
 *    An interface for ActionEngine Action launch helper / handler context.
 *
 * This file contains the definition of the Action class, which is used to
 * call ActionEngine actions and provide context in handlers (e.g. node map,
 * session, stream).
 */

namespace act {

/** An accessor class for an ActionEngine action.
 *
 * This class provides an interface for creating and managing actions in the
 * ActionEngine V2 format. It includes methods for setting up input and output nodes,
 * calling the action, and running the action handler.
 * @headerfile actionengine/actions/action.h
 */
class Action : public std::enable_shared_from_this<Action> {
 public:
  /** @brief
   *    Constructor. Creates an action in the context given by \p node_map,
   *    \p stream, and \p session.
   *
   * Creates an action with the given schema, handler, ID, node map, stream,
   * and session. The ID is generated if not provided, and other parameters are
   * nullable, and if not provided, restrictions apply, however, it makes the
   * Action class unified for client and server-side usage.
   *
   * @param schema
   *   The action schema. Required to resolve input and output node types,
   *   as well as to create the action message on call.
   * @param id
   *   The action ID. If empty, a unique ID will be generated.
   * @param inputs
   *   A mapping of input names to node IDs. If empty, the IDs will be generated
   *   based on the schema.
   * @param outputs
   *   A mapping of output names to node IDs. If empty, the IDs will be
   *   generated based on the schema.
   */
  explicit Action(ActionSchema schema, std::string_view id = "",
                  std::vector<Port> inputs = {},
                  std::vector<Port> outputs = {});

  ~Action();

  /** @brief
   *    Makes an action message to be sent on a WireStream.
   *
   * @return
   *   The action message.
   */
  [[nodiscard]] ActionMessage GetActionMessage() const;

  /** @brief
   *    Gets an AsyncNode with the given \p id from the node map.
   *
   * The node does not have to be defined as an input or output of the action.
   * If \p id does not exist on the action's schema, returns nullptr.
   * @param id
   *   The identifier of the node to get.
   * @return
   *   A pointer to the AsyncNode with the given \p id.
   */
  AsyncNode* absl_nullable GetNode(std::string_view id);

  /** @brief
   *    Gets an AsyncNode input with the given name from the node map.
   *    If no input with the given name is found, returns nullptr.
   *
   * @param name
   *   The name of the input to get.
   * @param bind_stream
   *   If true, the stream will be bound to the input node. If unspecified,
   *   the stream will be bound in Call(), but not Run().
   * @return
   *   A pointer to the AsyncNode of the input with the given name, or nullptr
   *   if not on ActionSchema.
   */
  AsyncNode* absl_nullable GetInput(
      std::string_view name, std::optional<bool> bind_stream = std::nullopt);

  /** @brief
   *    Gets an AsyncNode output with the given name from the node map.
   *    If no output with the given name is found, returns nullptr.
   *
   * @param name
   *   The name of the output to get.
   * @param bind_stream
   *   Whether to bind the stream to the output. If not specified, the stream
   *   will be bound in Run() but not in Call().
   * @return
   *   A pointer to the AsyncNode of the output with the given name, or nullptr
   *   if not on ActionSchema.
   */
  AsyncNode* absl_nullable GetOutput(
      std::string_view name,
      const std::optional<bool> bind_stream = std::nullopt) {
    act::MutexLock lock(&mu_);
    return GetOutputInternal(name, bind_stream);
  }

  /** @brief
   *    Sets the action handler.
   *
   * @param handler
   *   The action handler. This function will be called when the action is run
   *   server-side or manually via Run().
   */
  void BindHandler(ActionHandler handler) { handler_ = std::move(handler); }

  void BindNodeMap(NodeMap* absl_nullable node_map);

  /** Returns the node map associated with the action. */
  [[nodiscard]] NodeMap* absl_nullable GetNodeMap() const;

  /** @brief
    *   Binds a given stream to the action.
    *
    * The bound stream will be used to send the action message when Call() is
    * invoked, and attach to output nodes when Run() is invoked, so that the
    * nodes can send their data to the stream for another connected party
    * over the network.
    *
    * @param stream
    *   The stream to bind the action to. If nullptr, the action will not be
    *   bound to any stream.
    */
  void BindStream(WireStream* absl_nullable stream);

  /** Returns the stream bound to the action. */
  [[nodiscard]] WireStream* absl_nullable GetStream() const;

  /** @brief
   *    Binds a session to the action.
   *
   * The session provides the action with an ActionRegistry to resolve
   * nested actions by name, and is accessible to the action handler in case
   * it needs to do custom logic based on the session.
   *
   * @param session
   *   The session to bind the action to. If nullptr, the action will not be
   *   bound to any session.
   */
  void BindSession(Session* absl_nullable session);

  /** Returns the session bound to the action. */
  [[nodiscard]] Session* absl_nullable GetSession() const;

  /** @brief
   *    Makes a different action in the same session. Should be used to create
   *    nested actions.
   *
   * An action created in this way will share the same session, node map, and
   * stream as the current action.
   * @param name
   *   The name of the action to create. It must be registered in the
   *   session's action registry.
   * @param action_id
   *   The ID of the action to create. If empty, a unique ID will be generated.
   * @return
   *   An owning pointer to the new action.
   */
  std::unique_ptr<Action> MakeActionInSameSession(
      std::string_view name, std::string_view action_id = "") const;

  /** Returns the action registry from the session. */
  [[nodiscard]] ActionRegistry* absl_nullable GetRegistry() const;

  /** Returns the action's identifier. */
  [[nodiscard]] std::string GetId() const { return id_; }

  [[nodiscard]] const ActionSchema& GetSchema() const { return schema_; }

  /** @brief
   *    Block until the action has been completed, either by being run or
   *    called.
   *
   * If previously Run(), this method waits on a condition variable until
   * the action handler has completed and returns the resulting status.
   *
   * If previously Call()-ed, this method waits until the status arrives to
   * a special reserved output node named "__status__", and returns it.
   *
   * @return
   *   The status returned by the action handler, or the error status if the
   *   handler failed.
   */
  absl::Status Await(absl::Duration timeout = absl::InfiniteDuration());

  /** @brief
   *    Calls the action by sending an ActionEngine action message to associated
   *    stream.
   *
   * This method should normally be called by the client. It only creates the
   * message and sends it to the stream. The action handler is not called
   * by this method. It makes sure that input nodes are bound to the stream.
   *
   * @return
   *   The status of sending the action call message.
   */
  absl::Status Call();

  /** @brief
   *    Run the action handler. Clients usually do not call this
   *    method directly.
   *
   * Servers may want to call this method if they implement custom Session
   * and/or Service logic. This method will call the action handler and
   * make sure output nodes are bound to the stream.
   *
   * @return
   *   The status returned by the action handler.
   */
  absl::Status Run();

  /** @brief
   *    Specifies whether the action should delete its input nodes from its
   *    bound node map after it's run.
   */
  void ClearInputsAfterRun(bool clear = true);

  /** @brief
   *    Specifies whether the action should delete its output nodes from its
   *    bound node map after it's run.
   */
  void ClearOutputsAfterRun(bool clear = true);

  /** @brief
   *    Cancels the action and all its inputs.
   *
   * This method will cancel all input nodes and notify the action handler
   * that the action has been cancelled. It is safe to call this method multiple
   * times, as it will only notify once.
   *
   * Cancelling input nodes means stopping background prefetcher fibers and
   * setting reader status to Cancelled. Any cached data will still be available
   * to the action handler, but no new data will be fetched.
   *
   * Any output nodes will not be cancelled, as they do not introduce blocking
   * behavior in the action handler. It is the handler's responsibility to
   * check for cancellation and stop processing if needed. However, if the
   * handler terminates with an error, non-OK statuses will be sent to the
   * output nodes.
   */
  void Cancel() const {
    act::MutexLock lock(&mu_);
    CancelInternal();
  }

  /** @brief
   *    Returns a thread::Case that handlers can use to synchronise with
   *    cancellation.
   *
   * @return
   *   A thread::Case that can be used to wait for cancellation in a
   *   thread::Select() or thread::SelectUntil() call.
   */
  thread::Case OnCancel() const { return cancelled_->OnEvent(); }

  /** @brief
   *    Returns whether the action has been cancelled.
   *
   * @return
   *   True if the action has been cancelled, via .Cancel(), false otherwise.
   */
  [[nodiscard]] bool Cancelled() const;

  void BindStreamsOnInputsByDefault(bool bind) {
    act::MutexLock lock(&mu_);
    bind_streams_on_inputs_default_ = bind;
  }

  void BindStreamsOnOutputsByDefault(bool bind) {
    act::MutexLock lock(&mu_);
    bind_streams_on_outputs_default_ = bind;
  }

  void SetUserData(std::shared_ptr<void> data) {
    act::MutexLock lock(&mu_);
    user_data_ = std::move(data);
  }

  [[nodiscard]] void* absl_nullable GetUserData() const {
    act::MutexLock lock(&mu_);
    return user_data_.get();
  }

 private:
  void CancelInternal() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Implementation detail: gets the input node ID for the given name, unique
  // to this particular action run/call.
  std::string GetInputId(std::string_view name) const;

  // Implementation detail: gets the input node ID for the given name, unique
  // to this particular action run/call.
  std::string GetOutputId(std::string_view name) const;

  AsyncNode* absl_nonnull GetOutputInternal(
      std::string_view name, std::optional<bool> bind_stream = std::nullopt)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void UnbindStreams() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_{};
  act::CondVar cv_ ABSL_GUARDED_BY(mu_);

  ActionSchema schema_;
  absl::flat_hash_map<std::string, std::string> input_name_to_id_;
  absl::flat_hash_map<std::string, std::string> output_name_to_id_;

  ActionHandler handler_;
  bool has_been_called_ ABSL_GUARDED_BY(mu_) = false;
  bool has_been_run_ ABSL_GUARDED_BY(mu_) = false;
  std::string id_;

  NodeMap* absl_nullable node_map_ ABSL_GUARDED_BY(mu_) = nullptr;
  WireStream* absl_nullable stream_ ABSL_GUARDED_BY(mu_) = nullptr;
  Session* absl_nullable session_ ABSL_GUARDED_BY(mu_) = nullptr;

  absl::flat_hash_set<ChunkStoreReader*> reffed_readers_ ABSL_GUARDED_BY(mu_);

  bool bind_streams_on_inputs_default_ = true;
  bool bind_streams_on_outputs_default_ = false;
  absl::flat_hash_set<AsyncNode*> nodes_with_bound_streams_
      ABSL_GUARDED_BY(mu_);

  std::unique_ptr<thread::PermanentEvent> cancelled_;

  bool clear_inputs_after_run_ ABSL_GUARDED_BY(mu_) = false;
  bool clear_outputs_after_run_ ABSL_GUARDED_BY(mu_) = false;
  std::optional<absl::Status> run_status_ ABSL_GUARDED_BY(mu_) = std::nullopt;

  std::shared_ptr<void> user_data_ ABSL_GUARDED_BY(mu_) = nullptr;
};

}  // namespace act

#endif  // ACTIONENGINE_ACTIONS_ACTION_H_
