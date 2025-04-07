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

struct ActionNode {

  /// @private
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionNode& node) {
    absl::Format(&sink, "ActionNode{name: %s, type: %s}", node.name, node.type);
  }

  //! The name of the node.
  /**
   * By default, it takes the form of "action_id#input/output-name".
   */
  std::string name;
  //! A MIME type associated with the node.
  std::string type;
};

struct ActionDefinition {

  /// @private
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionDefinition& def) {
    absl::Format(&sink, "ActionDefinition{name: %s, inputs: %s, outputs: %s}",
                 def.name, absl::StrJoin(def.inputs, ", "),
                 absl::StrJoin(def.outputs, ", "));
  }

  std::string name;
  std::vector<ActionNode> inputs;
  std::vector<ActionNode> outputs;
};

class ActionRegistry {
 public:
  void Register(std::string_view name, const ActionDefinition& def,
                const ActionHandler& handler);

  [[nodiscard]] base::ActionMessage MakeActionMessage(
      std::string_view name, std::string_view id) const;

  std::unique_ptr<Action> MakeAction(std::string_view name, std::string_view id,
                                     NodeMap* node_map,
                                     base::EvergreenStream* stream,
                                     Session* session = nullptr) const;

  ActionDefinition& GetDefinition(const std::string_view name) {
    return eglt::FindOrDie(definitions_, name);
  }

  ActionHandler& GetHandler(const std::string_view name) {
    return eglt::FindOrDie(handlers_, name);
  }

  absl::flat_hash_map<std::string, ActionDefinition> definitions_;
  absl::flat_hash_map<std::string, ActionHandler> handlers_;

 private:
  [[nodiscard]] bool IsRegistered(const std::string_view name) const {
    return definitions_.contains(name) && handlers_.contains(name);
  }
};

class Session;

//! An accessor class for an Evergreen action.
/*!
 * This class provides an interface for creating and managing actions in the
 * Evergreen V2 format. It includes methods for setting up input and output nodes,
 * calling the action, and running the action handler.
 * \headerfile eglt/actions/action.h
 */
class Action : public std::enable_shared_from_this<Action> {
 public:
  /**
   * \brief
   *   Constructor. Creates an action in the context given by \p node_map,
   *   \p stream, and \p session.
   *
   * Creates an action with the given definition, handler, ID, node map, stream,
   * and session. The ID is generated if not provided, and other parameters are
   * nullable, and if not provided, restrictions apply, however, it makes the
   * Action class unified for client and server-side usage.
   * \param def
   *   The action definition. Required to resolve input and output node types,
   *   as well as to create the action message on call.
   * \param handler
   *   The action handler. This function will be called when the action is run
   *   server-side or manually via Run().
   * \param id
   *   The action ID. If empty, a unique ID will be generated.
   * \param node_map
   *   Node map to use for node access/creation. If nullptr, only
   *   pure side effect actions are allowed.
   * \param stream
   *   A stream to use for sending messages and calling the action.
   *   If nullptr, the action cannot be called, and nodes will only
   *   be written to a local store.
   * \param session
   *   A pointer to the session.
   */
  explicit Action(ActionDefinition def, ActionHandler handler,
                  std::string_view id = "", NodeMap* node_map = nullptr,
                  base::EvergreenStream* stream = nullptr,
                  Session* session = nullptr);

  //! Makes an action message to be sent on an EvergreenStream.
  /**
   * \return
   *   The action message.
   */
  [[nodiscard]] base::ActionMessage GetActionMessage() const;

  static std::unique_ptr<Action> FromActionMessage(
      const base::ActionMessage& action, ActionRegistry* registry,
      NodeMap* node_map, base::EvergreenStream* stream,
      Session* session = nullptr);

  /**
   * \brief Gets an AsyncNode with the given \p id from the node map.
   *
   * The node does not have to be defined as an input or output of the action.
   * If the node is not found, it will be created with the given \p id.
   * \param id
   *   The identifier of the node to get.
   * \return
   *   A pointer to the AsyncNode with the given \p id.
   */
  AsyncNode* GetNode(std::string_view id) const;

  /** \brief Gets an AsyncNode input with the given name from the node map.
   *         If no input with the given name is found, it will return nullptr.
   *
   * \param name
   *   The name of the input to get.
   * \param bind_stream
   *   If true, the stream will be bound to the input node. If unspecified,
   *   the stream will be bound in Call(), but not Run().
   * \return
   *   A pointer to the AsyncNode of the input with the given name, or nullptr
   *   if not on ActionDefinition.
   */
  AsyncNode* GetInput(std::string_view name,
                      const std::optional<bool> bind_stream = std::nullopt) {
    // TODO(helenapankov): just use hash maps instead of vectors
    const auto it = std::find_if(
        def_.inputs.begin(), def_.inputs.end(),
        [name](const ActionNode& node) { return node.name == name; });
    if (it == def_.inputs.end()) {
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

  /** \brief Gets an AsyncNode output with the given name from the node map.
   *         If no output with the given name is found, it will return nullptr.
   *
   * \param name
   *   The name of the output to get.
   * \param bind_stream
   *   Whether to bind the stream to the output. If not specified, the stream
   *   will be bound in Run() but not in Call().
   * \return
   *   A pointer to the AsyncNode of the output with the given name, or nullptr
   *   if not on ActionDefinition.
   */
  AsyncNode* GetOutput(std::string_view name,
                       const std::optional<bool> bind_stream = std::nullopt) {
    const auto it = std::find_if(
        def_.outputs.begin(), def_.outputs.end(),
        [name](const ActionNode& node) { return node.name == name; });
    if (it == def_.outputs.end()) {
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

  /// Set the action handler.
  /**
   * \param handler
   *   The action handler. This function will be called when the action is run
   *   server-side or manually via Run().
   */
  void SetHandler(ActionHandler handler) { handler_ = std::move(handler); }

  /**
   * \brief
   *   Makes a different action in the same session. Should be used to create
   *   nested actions.
   *
   * An action created in this way will share the same session, node map, and
   * stream as the current action.
   * \param name
   *   The name of the action to create. It must be registered in the
   *   session's action registry.
   * \param id
   *   The ID of the action to create. If empty, a unique ID will be generated.
   * \return
   *   An owning pointer to the new action.
   */
  std::unique_ptr<Action> MakeActionInSameSession(
      const std::string_view name, const std::string_view id = "") const {
    return GetRegistry()->MakeAction(name, id, node_map_, stream_, session_);
  }

  /// Returns the action registry from the session.
  [[nodiscard]] ActionRegistry* GetRegistry() const;
  /// Returns the stream associated with the action.
  [[nodiscard]] base::EvergreenStream* GetStream() const { return stream_; }

  /// Returns the action's identifier.
  [[nodiscard]] std::string GetId() const { return id_; }

  /// Returns the action's fully qualified (inputs/outputs with IDs) definition.
  [[nodiscard]] const ActionDefinition& GetDefinition() const { return def_; }

  /**
   * \brief
   *   Calls the action by sending an Evergreen action message to associated
   *   stream.
   *
   * This method should normally be called by the client. It only creates the
   * message and sends it to the stream. The action handler is not called
   * by this method. It makes sure that input nodes are bound to the stream.
   *
   * \return
   *   The status of sending the action call message.
   */
  absl::Status Call() {
    bind_streams_on_inputs_default_ = true;
    bind_streams_on_outputs_default_ = false;

    return stream_->Send(base::SessionMessage{.actions = {GetActionMessage()}});
  }

  /**
   * \brief
   *   Run the action handler. Clients usually do not call this method directly.
   *
   * Servers may want to call this method if they implement custom Session
   * and/or Service logic. This method will call the action handler and
   * make sure output nodes are bound to the stream.
   *
   * \return
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

  ActionDefinition def_;
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
