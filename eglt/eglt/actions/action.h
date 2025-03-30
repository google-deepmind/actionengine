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

namespace eglt {

class Session;

class Action;
using ActionHandler =
std::function<absl::Status(const std::shared_ptr<Action>&)>;

struct ActionNode {
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ActionNode& node) {
    absl::Format(&sink, "ActionNode{name: %s, type: %s}", node.name, node.type);
  }

  std::string name;
  std::string type;
};

struct ActionDefinition {
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

  [[nodiscard]] base::ActionMessage MakeActionMessage(std::string_view name,
    std::string_view id) const;

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

class Action : public std::enable_shared_from_this<Action> {
public:
  explicit Action(ActionDefinition def, ActionHandler handler,
                  std::string_view id = "", NodeMap* node_map = nullptr,
                  base::EvergreenStream* stream = nullptr,
                  Session* session = nullptr);

  Action(const Action& other) = default;
  Action& operator=(const Action& other) = default;

  base::ActionMessage GetActionMessage() const;

  static std::unique_ptr<Action> FromActionMessage(
    const base::ActionMessage& action, ActionRegistry* registry,
    NodeMap* node_map, base::EvergreenStream* stream,
    Session* session = nullptr);

  AsyncNode* GetNode(std::string_view id) const;

  AsyncNode* GetInput(std::string_view name,
                      const std::optional<bool> bind_stream = std::nullopt) {
    // TODO(helenapankov): just use hash maps instead of vectors
    const auto it = std::find_if(
      def_.inputs.begin(), def_.inputs.end(),
      [name](const ActionNode& node) { return node.name == name; });
    if (it == def_.inputs.end()) { return nullptr; }

    AsyncNode* node = GetNode(GetInputId(name));
    if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_inputs_default_)) {
      node->BindWriterStream(stream_);
      nodes_with_bound_streams_.insert(node);
    }
    return node;
  }

  AsyncNode* GetOutput(std::string_view name,
                       const std::optional<bool> bind_stream = std::nullopt) {
    const auto it = std::find_if(
      def_.outputs.begin(), def_.outputs.end(),
      [name](const ActionNode& node) { return node.name == name; });
    if (it == def_.outputs.end()) { return nullptr; }

    AsyncNode* node = GetNode(GetOutputId(name));
    if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_outputs_default_)) {
      node->BindWriterStream(stream_);
      nodes_with_bound_streams_.insert(node);
    }
    return node;
  }

  void SetHandler(ActionHandler handler) { handler_ = std::move(handler); }

  std::unique_ptr<Action> MakeActionInSameSession(
    const std::string_view name,
    const std::string_view id = "") const {
    return GetRegistry()->MakeAction(name, id, node_map_, stream_, session_);
  }

  ActionRegistry* GetRegistry() const;
  base::EvergreenStream* GetStream() const { return stream_; }

  std::string GetId() const { return id_; }

  const ActionDefinition& GetDefinition() const { return def_; }

  absl::Status Call() {
    bind_streams_on_inputs_default_ = true;
    bind_streams_on_outputs_default_ = false;

    return stream_->Send(base::SessionMessage{.actions = {GetActionMessage()}});
  }

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
      if (node == nullptr) { continue; }
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

} // namespace eglt

#endif  // EGLT_ACTIONS_ACTION_H_
