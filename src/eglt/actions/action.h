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

#ifndef EGLT_ACTIONS_ACTION_H_
#define EGLT_ACTIONS_ACTION_H_

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/nodes/async_node.h"
#include "eglt/nodes/node_map.h"
#include "eglt/util/map_util.h"
#include "eglt/util/random.h"

/**
 * @file
 * @brief
 *   An interface for Evergreen Action launch helper / handler context.
 *
 * This file contains the definition of the Action class, which is used to
 * call Evergreen actions and provide context in handlers (e.g. node map,
 * session, stream).
 */

namespace eglt {

class Session;

class Action;
using ActionHandler =
    std::function<absl::Status(const std::shared_ptr<Action>&)>;
// A type for Python and other bindings that cannot use absl::Status
using VoidActionHandler = std::function<void(const std::shared_ptr<Action>&)>;

using NameAndMimetype = std::pair<std::string, std::string>;
using NameToMimetype = absl::flat_hash_map<std::string, std::string>;

struct ActionSchema {

  /// @private
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionSchema& schema) {
    std::vector<std::string> input_reprs;
    input_reprs.reserve(schema.inputs.size());
    for (const auto& [name, type] : schema.inputs) {
      input_reprs.push_back(absl::StrCat(name, ":", type));
    }

    std::vector<std::string> output_reprs;
    output_reprs.reserve(schema.outputs.size());
    for (const auto& [name, type] : schema.outputs) {
      output_reprs.push_back(absl::StrCat(name, ":", type));
    }

    absl::Format(&sink, "ActionSchema{name: %s, inputs: %s, outputs: %s}",
                 schema.name, absl::StrJoin(input_reprs, ", "),
                 absl::StrJoin(output_reprs, ", "));
  }

  [[nodiscard]] ActionMessage GetActionMessage(
      std::string_view action_id) const {
    CHECK(!action_id.empty())
        << "Action ID cannot be empty to create a message";

    std::vector<Port> input_parameters;
    input_parameters.reserve(inputs.size());
    for (const auto& [name, _] : inputs) {
      input_parameters.push_back(Port{
          .name = name,
          .id = absl::StrCat(action_id, "#", name),
      });
    }

    std::vector<Port> output_parameters;
    output_parameters.reserve(outputs.size());
    for (const auto& [name, _] : outputs) {
      output_parameters.push_back(Port{
          .name = name,
          .id = absl::StrCat(action_id, "#", name),
      });
    }

    return {
        .id = std::string(action_id),
        .name = name,
        .inputs = input_parameters,
        .outputs = output_parameters,
    };
  }

  std::string name;
  NameToMimetype inputs;
  NameToMimetype outputs;
};

class ActionRegistry {
 public:
  void Register(std::string_view name, const ActionSchema& schema,
                const ActionHandler& handler) {
    schemas_[name] = schema;
    handlers_[name] = handler;
  }

  [[nodiscard]] ActionMessage MakeActionMessage(std::string_view action_key,
                                                std::string_view id) const {
    return eglt::FindOrDie(schemas_, action_key).GetActionMessage(id);
  }

  [[nodiscard]] std::unique_ptr<Action> MakeAction(
      std::string_view action_key, std::string_view action_id = "",
      std::vector<Port> inputs = {}, std::vector<Port> outputs = {}) const;

  [[nodiscard]] bool IsRegistered(const std::string_view name) const {
    return schemas_.contains(name) && handlers_.contains(name);
  }

  [[nodiscard]] const ActionSchema& GetSchema(
      const std::string_view name) const {
    return eglt::FindOrDie(schemas_, name);
  }

  [[nodiscard]] const ActionHandler& GetHandler(
      const std::string_view name) const {
    return eglt::FindOrDie(handlers_, name);
  }

  absl::flat_hash_map<std::string, ActionSchema> schemas_;
  absl::flat_hash_map<std::string, ActionHandler> handlers_;
};

//! An accessor class for an Evergreen action.
/*!
 * This class provides an interface for creating and managing actions in the
 * Evergreen V2 format. It includes methods for setting up input and output nodes,
 * calling the action, and running the action handler.
 * @headerfile eglt/actions/action.h
 */
class Action : public std::enable_shared_from_this<Action> {
 public:
  /**
   * @brief
   *   Constructor. Creates an action in the context given by \p node_map,
   *   \p stream, and \p session.
   *
   * Creates an action with the given schema, handler, ID, node map, stream,
   * and session. The ID is generated if not provided, and other parameters are
   * nullable, and if not provided, restrictions apply, however, it makes the
   * Action class unified for client and server-side usage.
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
                  std::vector<Port> inputs = {}, std::vector<Port> outputs = {})
      : schema_(std::move(schema)),
        id_(id.empty() ? GenerateUUID4() : std::string(id)),
        cancelled_(std::make_unique<concurrency::PermanentEvent>()) {
    ActionMessage message = schema_.GetActionMessage(id_);

    std::vector<Port>& input_parameters =
        inputs.empty() ? message.inputs : inputs;
    for (auto& [input_name, input_id] : std::move(input_parameters)) {
      input_name_to_id_[std::move(input_name)] = std::move(input_id);
    }

    std::vector<Port>& output_parameters =
        outputs.empty() ? message.outputs : outputs;
    for (auto& [output_name, output_id] : std::move(output_parameters)) {
      output_name_to_id_[std::move(output_name)] = std::move(output_id);
    }
  }

  explicit Action(std::string_view name, std::string_view id = "",
                  std::vector<Port> inputs = {},
                  std::vector<Port> outputs = {}) {
    ActionSchema schema;
    schema.name = name;
    for (const auto& [input_name, _] : inputs) {
      schema.inputs[input_name] = "*";
    }
    for (const auto& [output_name, _] : outputs) {
      schema.outputs[output_name] = "*";
    }
    *this =
        Action(std::move(schema), id, std::move(inputs), std::move(outputs));
  }

  Action& operator=(Action&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    concurrency::TwoMutexLock lock(&mutex_, &other.mutex_);

    CHECK(!other.has_been_run_)
        << "Cannot move an action that has been run. Handlers rely on the "
           "validity of their pointers to an action.";

    schema_ = std::move(other.schema_);
    id_ = std::move(other.id_);
    input_name_to_id_ = std::move(other.input_name_to_id_);
    output_name_to_id_ = std::move(other.output_name_to_id_);
    handler_ = std::move(other.handler_);
    has_been_run_ = other.has_been_run_;
    node_map_ = other.node_map_;
    session_ = other.session_;
    cancelled_ = std::move(other.cancelled_);
    cancelled_externally_ = other.cancelled_externally_;
    nodes_with_bound_streams_ = std::move(other.nodes_with_bound_streams_);
    bind_streams_on_inputs_default_ = other.bind_streams_on_inputs_default_;
    bind_streams_on_outputs_default_ = other.bind_streams_on_outputs_default_;
    return *this;
  }

  ~Action() {
    concurrency::MutexLock lock(&mutex_);
    if (!has_been_run_ && node_map_ != nullptr) {
      ResetIoNodes();
    }
  }

  //! Makes an action message to be sent on an EvergreenWireStream.
  /**
   * @return
   *   The action message.
   */
  [[nodiscard]] ActionMessage GetActionMessage() const {
    std::vector<Port> input_parameters;
    input_parameters.reserve(input_name_to_id_.size());
    for (const auto& [name, id] : input_name_to_id_) {
      input_parameters.push_back({
          .name = name,
          .id = id,
      });
    }

    std::vector<Port> output_parameters;
    output_parameters.reserve(output_name_to_id_.size());
    for (const auto& [name, id] : output_name_to_id_) {
      output_parameters.push_back({
          .name = name,
          .id = id,
      });
    }

    return {
        .id = id_,
        .name = schema_.name,
        .inputs = input_parameters,
        .outputs = output_parameters,
    };
  }

  /**
   * @brief Gets an AsyncNode with the given \p id from the node map.
   *
   * The node does not have to be defined as an input or output of the action.
   * The \p id must exist in the action schema.
   * @param id
   *   The identifier of the node to get.
   * @return
   *   A pointer to the AsyncNode with the given \p id.
   */
  AsyncNode* GetNode(std::string_view id) {
    if (input_name_to_id_.contains(id)) {
      return GetInput(id);
    }
    if (output_name_to_id_.contains(id)) {
      return GetOutput(id);
    }

    LOG(FATAL) << absl::StrFormat(
        "Node %s not found in action schema for %s with id=%s", id,
        schema_.name, id_);
    ABSL_ASSUME(false);
  }

  /** @brief Gets an AsyncNode input with the given name from the node map.
   *         If no input with the given name is found, it will return nullptr.
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
  AsyncNode* GetInput(std::string_view name,
                      const std::optional<bool> bind_stream = std::nullopt) {
    concurrency::MutexLock lock(&mutex_);
    if (node_map_ == nullptr) {
      LOG(FATAL) << absl::StrFormat(
          "No node map is bound to action %s with id=%s. Cannot get input %s",
          schema_.name, id_, name);
      ABSL_ASSUME(false);
    }

    if (!input_name_to_id_.contains(name)) {
      LOG(FATAL) << absl::StrFormat(
          "Input %s not found in action schema for %s with id=%s", name,
          schema_.name, id_);
      ABSL_ASSUME(false);
    }

    AsyncNode* node = node_map_->Get(GetInputId(name));
    if (stream_ != nullptr &&
        bind_stream.value_or(bind_streams_on_inputs_default_)) {
      absl::flat_hash_map<std::string_view,
                          std::shared_ptr<EvergreenWireStream>>
          peers;
      peers.insert({stream_->GetId(), stream_});
      node->BindPeers(std::move(peers));
      nodes_with_bound_streams_.insert(node);
    }
    return node;
  }

  /** @brief Gets an AsyncNode output with the given name from the node map.
   *         If no output with the given name is found, it will return nullptr.
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
  AsyncNode* GetOutput(std::string_view name,
                       const std::optional<bool> bind_stream = std::nullopt) {
    concurrency::MutexLock lock(&mutex_);
    if (node_map_ == nullptr) {
      LOG(FATAL) << absl::StrFormat(
          "No node map is bound to action %s with id=%s. Cannot get input %s",
          schema_.name, id_, name);
      ABSL_ASSUME(false);
    }

    if (!output_name_to_id_.contains(name) && name != "__status__") {
      LOG(FATAL) << absl::StrFormat(
          "Output %s not found in action schema for %s with id=%s", name,
          schema_.name, id_);
      ABSL_ASSUME(false);
    }

    AsyncNode* node = node_map_->Get(GetOutputId(name));
    if (stream_ != nullptr &&
        bind_stream.value_or(bind_streams_on_outputs_default_)) {
      absl::flat_hash_map<std::string_view,
                          std::shared_ptr<EvergreenWireStream>>
          peers;
      peers.insert({stream_->GetId(), stream_});
      node->BindPeers(std::move(peers));
      nodes_with_bound_streams_.insert(node);
    }
    return node;
  }

  /** @brief Sets the action handler.
   *
   * @param handler
   *   The action handler. This function will be called when the action is run
   *   server-side or manually via Run().
   */
  void BindHandler(ActionHandler handler) { handler_ = std::move(handler); }

  void BindNodeMap(NodeMap* absl_nullable node_map)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    node_map_ = node_map;
  }

  /// Returns the node map associated with the action.
  [[nodiscard]] NodeMap* GetNodeMap() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return node_map_;
  }

  void BindStream(std::shared_ptr<EvergreenWireStream> absl_nullable stream)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    stream_ = std::move(stream);
  }

  /// Returns the stream associated with the action.
  [[nodiscard]] EvergreenWireStream* GetStream() const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return stream_.get();
  }

  void BindSession(Session* absl_nullable session) ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    session_ = session;
  }

  [[nodiscard]] Session* GetSession() const ABSL_LOCKS_EXCLUDED(mutex_) {
    concurrency::MutexLock lock(&mutex_);
    return session_;
  }

  /**
   * @brief
   *   Makes a different action in the same session. Should be used to create
   *   nested actions.
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
      const std::string_view name,
      const std::string_view action_id = "") const {
    auto action = GetRegistry()->MakeAction(name, action_id);
    concurrency::TwoMutexLock lock(&mutex_, &action->mutex_);
    action->node_map_ = node_map_;
    action->stream_ = stream_;
    action->session_ = session_;
    return action;
  }

  /// Returns the action registry from the session.
  [[nodiscard]] ActionRegistry* GetRegistry() const;

  /// Returns the action's identifier.
  [[nodiscard]] std::string GetId() const { return id_; }

  [[nodiscard]] const ActionSchema& GetSchema() const { return schema_; }

  /**
   * @brief
   *   Calls the action by sending an Evergreen action message to associated
   *   stream.
   *
   * This method should normally be called by the client. It only creates the
   * message and sends it to the stream. The action handler is not called
   * by this method. It makes sure that input nodes are bound to the stream.
   *
   * @return
   *   The status of sending the action call message.
   */
  absl::Status Call() {
    concurrency::MutexLock lock(&mutex_);
    bind_streams_on_inputs_default_ = true;
    bind_streams_on_outputs_default_ = false;

    return stream_->Send(SessionMessage{.actions = {GetActionMessage()}});
  }

  /**
   * @brief
   *   Run the action handler. Clients usually do not call this method directly.
   *
   * Servers may want to call this method if they implement custom Session
   * and/or Service logic. This method will call the action handler and
   * make sure output nodes are bound to the stream.
   *
   * @return
   *   The status returned by the action handler.
   */
  absl::Status Run(concurrency::PermanentEvent* absl_nullable
                       cancelled_externally = nullptr) {
    concurrency::MutexLock lock(&mutex_);
    bind_streams_on_inputs_default_ = false;
    bind_streams_on_outputs_default_ = true;

    cancelled_externally_ = cancelled_externally;
    has_been_run_ = true;

    mutex_.Unlock();
    auto status = handler_(shared_from_this());
    mutex_.Lock();

    UnbindStreams();
    if (node_map_ != nullptr) {
      ResetIoNodes();
    }

    return status;
  }

  void Cancel() const {
    concurrency::MutexLock lock(&mutex_);
    if (cancelled_->HasBeenNotified()) {
      return;
    }

    cancelled_->Notify();
  }

  concurrency::Case OnCancel() const { return cancelled_->OnEvent(); }

  concurrency::Case OnExternalCancel() const {
    if (cancelled_externally_ == nullptr) {
      return concurrency::NonSelectableCase();
    }
    return cancelled_externally_->OnEvent();
  }

  bool Cancelled() const {
    concurrency::MutexLock lock(&mutex_);
    bool result = cancelled_->HasBeenNotified();
    if (cancelled_externally_ != nullptr) {
      result = result || cancelled_externally_->HasBeenNotified();
    }
    return result;
  }

 private:
  std::string GetInputId(const std::string_view name) const {
    return absl::StrCat(id_, "#", name);
  }

  std::string GetOutputId(const std::string_view name) const {
    return absl::StrCat(id_, "#", name);
  }

  void UnbindStreams() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    for (const auto& node : nodes_with_bound_streams_) {
      if (node == nullptr) {
        continue;
      }
      node->BindPeers({});
    }
    nodes_with_bound_streams_.clear();
  }

  void ResetIoNodes() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    for (const auto& [input_name, input_id] : input_name_to_id_) {
      node_map_->Extract(input_id).reset();
    }
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      node_map_->Extract(output_id).reset();
    }
  }

  mutable concurrency::Mutex mutex_{};

  ActionSchema schema_;
  absl::flat_hash_map<std::string, std::string> input_name_to_id_;
  absl::flat_hash_map<std::string, std::string> output_name_to_id_;

  ActionHandler handler_;
  bool has_been_run_ ABSL_GUARDED_BY(mutex_) = false;
  std::string id_;

  NodeMap* node_map_ ABSL_GUARDED_BY(mutex_) = nullptr;
  std::shared_ptr<EvergreenWireStream> stream_ ABSL_GUARDED_BY(mutex_) =
      nullptr;
  Session* session_ ABSL_GUARDED_BY(mutex_) = nullptr;

  bool bind_streams_on_inputs_default_ = true;
  bool bind_streams_on_outputs_default_ = false;
  absl::flat_hash_set<AsyncNode*> nodes_with_bound_streams_
      ABSL_GUARDED_BY(mutex_);

  std::unique_ptr<concurrency::PermanentEvent> cancelled_;
  concurrency::PermanentEvent* absl_nullable cancelled_externally_ = nullptr;
};

inline std::unique_ptr<Action> ActionRegistry::MakeAction(
    std::string_view action_key, std::string_view action_id,
    std::vector<Port> inputs, std::vector<Port> outputs) const {

  auto action =
      std::make_unique<Action>(eglt::FindOrDie(schemas_, action_key), action_id,
                               std::move(inputs), std::move(outputs));
  action->BindHandler(eglt::FindOrDie(handlers_, action_key));

  return action;
}

}  // namespace eglt

#endif  // EGLT_ACTIONS_ACTION_H_
