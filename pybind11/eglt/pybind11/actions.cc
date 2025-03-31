#include "eglt/pybind11/actions.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <pybind11/pybind11.h>

#include "eglt/actions/action.h"
#include "eglt/nodes/node_map.h"
#include "eglt/pybind11/pybind11_headers.h"
#include "eglt/pybind11/utils.h"
#include "eglt/service/session.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindActionNode(py::handle scope, std::string_view name) {
  py::class_<ActionNode, std::shared_ptr<ActionNode>>(scope,
                                                      std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<ActionNode>())
      .def(py::init([](const std::string& node_name, const std::string& type) {
             return ActionNode{.name = node_name, .type = type};
           }),
           py::kw_only(), py::arg("name"), py::arg("type"))
      .def_readwrite("name", &ActionNode::name)
      .def_readwrite("type", &ActionNode::type)
      .def("__repr__",
           [](const ActionNode& node) { return absl::StrCat(node); })
      .doc() = "An Evergreen v2 ActionNode.";
}

void BindActionDefinition(py::handle scope, std::string_view name) {
  py::class_<ActionDefinition, std::shared_ptr<ActionDefinition>>(
      scope, std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<ActionDefinition>())
      .def(py::init([](const std::string& action_name,
                       const std::vector<ActionNode>& inputs,
                       const std::vector<ActionNode>& outputs) {
             return std::make_shared<ActionDefinition>(action_name, inputs,
                                                       outputs);
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<ActionNode>()),
           py::arg_v("outputs", std::vector<ActionNode>()))
      .def_readwrite("name", &ActionDefinition::name)
      .def_readwrite("inputs", &ActionDefinition::inputs)
      .def_readwrite("outputs", &ActionDefinition::outputs)
      .def("__repr__",
           [](const ActionDefinition& def) { return absl::StrCat(def); })
      .doc() = "An Evergreen v2 ActionDefinition.";
}

void BindActionRegistry(py::handle scope, std::string_view name) {
  py::class_<ActionRegistry, std::shared_ptr<ActionRegistry>>(
      scope, std::string(name).c_str())
      .def(py::init([]() { return std::make_shared<ActionRegistry>(); }))
      .def(MakeSameObjectRefConstructor<ActionRegistry>())
      .def("register", &ActionRegistry::Register, py::arg("name"),
           py::arg("def"), py::arg("handler"))
      .def("make_action_message", &ActionRegistry::MakeActionMessage,
           py::arg("name"), py::arg("id"))
      .def(
          "make_action",
          [](const std::shared_ptr<ActionRegistry>& self,
             const std::string& name, const std::string& id, NodeMap* node_map,
             base::EvergreenStream* stream, Session* session) {
            auto action = self->MakeAction(name, id, node_map, stream, session);
            return std::shared_ptr<Action>(std::move(action));
          },
          py::arg("name"), py::arg_v("id", ""), py::arg_v("node_map", nullptr),
          py::arg_v("stream", nullptr), py::arg_v("session", nullptr));
}

void BindAction(py::handle scope, std::string_view name) {
  py::class_<Action, std::shared_ptr<Action>>(scope, std::string(name).c_str())
      .def(MakeSameObjectRefConstructor<Action>())
      .def(py::init([](ActionDefinition def, ActionHandler handler,
                       const std::string& id = "", NodeMap* node_map = nullptr,
                       base::EvergreenStream* stream = nullptr,
                       Session* session = nullptr) {
        return std::make_shared<Action>(std::move(def), std::move(handler), id,
                                        node_map, stream, session);
      }))
      .def(
          "run",
          [](const std::shared_ptr<Action>& action) { return action->Run(); },
          py::call_guard<py::gil_scoped_release>())
      .def("call", &Action::Call)
      .def("get_registry",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetRegistry());
           })
      .def("get_stream",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetStream());
           })
      .def("get_id", &Action::GetId)
      .def("get_definition", &Action::GetDefinition,
           py::return_value_policy::reference)
      .def(
          "get_node",
          [](const std::shared_ptr<Action>& action, const std::string& id) {
            return ShareWithNoDeleter(action->GetNode(id));
          },
          py::arg("id"))
      .def(
          "get_input",
          [](const std::shared_ptr<Action>& action, const std::string& id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetInput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()))
      .def(
          "get_output",
          [](const std::shared_ptr<Action>& action, const std::string& id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetOutput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()))
      .def(
          "make_action_in_same_session",
          [](const std::shared_ptr<Action>& action, const std::string& name,
             const std::string& id) {
            auto new_action = action->MakeActionInSameSession(name, id);
            return std::shared_ptr<Action>(std::move(new_action));
          },
          py::arg("name"), py::arg_v("id", ""))
      .def("set_handler", &Action::SetHandler, py::arg("handler"));
}

py::module_ MakeActionsModule(py::module_ scope, std::string_view module_name) {
  py::module_ actions = scope.def_submodule(std::string(module_name).c_str(),
                                            "Evergreen v2 Actions interface.");

  BindActionNode(actions, "ActionNode");
  BindActionDefinition(actions, "ActionDefinition");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace eglt::pybindings
