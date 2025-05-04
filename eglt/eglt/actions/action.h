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

    std::vector<NamedParameter> input_parameters;
    input_parameters.reserve(inputs.size());
    for (const auto& [name, _] : inputs) {
      input_parameters.push_back(NamedParameter{
          .name = name,
          .id = absl::StrCat(action_id, "#", name),
      });
    }

    std::vector<NamedParameter> output_parameters;
    output_parameters.reserve(outputs.size());
    for (const auto& [name, _] : outputs) {
      output_parameters.push_back(NamedParameter{
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
                const ActionHandler& handler);

  [[nodiscard]] ActionMessage MakeActionMessage(std::string_view action_key,
                                                std::string_view id) const;

  [[nodiscard]] std::unique_ptr<Action> MakeAction(
      std::string_view action_key, std::string_view action_id = "",
      std::vector<NamedParameter> inputs = {},
      std::vector<NamedParameter> outputs = {}) const;

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
                  std::vector<NamedParameter> inputs = {},
                  std::vector<NamedParameter> outputs = {})
      : schema_(std::move(schema)),
        id_(id.empty() ? GenerateUUID4() : std::string(id)) {
    ActionMessage message = schema_.GetActionMessage(id_);

    std::vector<NamedParameter>& input_parameters =
        inputs.empty() ? message.inputs : inputs;
    for (auto& [input_name, input_id] : std::move(input_parameters)) {
      input_name_to_id_[std::move(input_name)] = std::move(input_id);
    }

    std::vector<NamedParameter>& output_parameters =
        outputs.empty() ? message.outputs : outputs;
    for (auto& [output_name, output_id] : std::move(output_parameters)) {
      output_name_to_id_[std::move(output_name)] = std::move(output_id);
    }
  }

  explicit Action(std::string_view name, std::string_view id = "",
                  std::vector<NamedParameter> inputs = {},
                  std::vector<NamedParameter> outputs = {}) {
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

  ~Action() {
    if (node_map_ == nullptr) {
      return;
    }
    for (const auto& [input_name, input_id] : input_name_to_id_) {
      node_map_->Extract(input_id).reset();
    }
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      node_map_->Extract(output_id).reset();
    }
  }

  //! Makes an action message to be sent on an EvergreenStream.
  /**
   * @return
   *   The action message.
   */
  [[nodiscard]] ActionMessage GetActionMessage() const {
    std::vector<NamedParameter> input_parameters;
    input_parameters.reserve(input_name_to_id_.size());
    for (const auto& [name, id] : input_name_to_id_) {
      input_parameters.push_back({
          .name = name,
          .id = id,
      });
    }

    std::vector<NamedParameter> output_parameters;
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
   * If the node is not found, it will be created with the given \p id.
   * @param id
   *   The identifier of the node to get.
   * @return
   *   A pointer to the AsyncNode with the given \p id.
   */
  AsyncNode* GetNode(std::string_view id) const;

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
    if (!input_name_to_id_.contains(name)) {
      return nullptr;
    }

    AsyncNode* node = GetNode(GetInputId(name));
    if (stream_ != nullptr &&
        bind_stream.value_or(bind_streams_on_inputs_default_)) {
      node->BindWriterStream(stream_);
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
    if (!output_name_to_id_.contains(name) && name != "__status__") {
      return nullptr;
    }

    AsyncNode* node = GetNode(GetOutputId(name));
    if (stream_ != nullptr &&
        bind_stream.value_or(bind_streams_on_outputs_default_)) {
      node->BindWriterStream(stream_);
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

  void BindNodeMap(NodeMap* node_map) { node_map_ = node_map; }

  void BindStream(base::EvergreenStream* stream) { stream_ = stream; }

  void BindSession(Session* session) { session_ = session; }

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
    action->BindNodeMap(node_map_);
    action->BindStream(stream_);
    action->BindSession(session_);
    return action;
  }

  /// Returns the action registry from the session.
  [[nodiscard]] ActionRegistry* GetRegistry() const;
  /// Returns the stream associated with the action.
  [[nodiscard]] base::EvergreenStream* GetStream() const { return stream_; }

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
  absl::Status Run() {
    bind_streams_on_inputs_default_ = false;
    bind_streams_on_outputs_default_ = true;

    auto status = handler_(shared_from_this());
    UnbindStreams();
    return status;
  }

 private:
  Session* GetSession() const { return session_; }

  std::string GetInputId(const std::string_view name) const {
    return absl::StrCat(id_, "#", name);
  }

  std::string GetOutputId(const std::string_view name) const {
    return absl::StrCat(id_, "#", name);
  }

  void UnbindStreams() {
    for (const auto& node : nodes_with_bound_streams_) {
      if (node == nullptr) {
        continue;
      }
      node->BindWriterStream(nullptr);
    }
    nodes_with_bound_streams_.clear();
  }

  ActionSchema schema_;
  absl::flat_hash_map<std::string, std::string> input_name_to_id_;
  absl::flat_hash_map<std::string, std::string> output_name_to_id_;

  ActionHandler handler_;
  std::string id_;

  NodeMap* node_map_ = nullptr;
  base::EvergreenStream* stream_ = nullptr;
  Session* session_ = nullptr;

  bool bind_streams_on_inputs_default_ = true;
  bool bind_streams_on_outputs_default_ = false;
  absl::flat_hash_set<AsyncNode*> nodes_with_bound_streams_;
};

}  // namespace eglt

#endif  // EGLT_ACTIONS_ACTION_H_
