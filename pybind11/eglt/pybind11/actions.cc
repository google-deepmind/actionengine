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

ActionHandler MakeStatusAwareActionHandler(VoidActionHandler handler) {
  return [handler = std::move(handler)](const std::shared_ptr<Action>& action) {
    try {
      std::move(handler)(action);
    } catch (py::error_already_set& e) {
      return absl::InternalError(
          absl::StrCat("Python error in action handler: ", e.what()));
    }
    return absl::OkStatus();
  };
}

/// @private
void BindActionSchema(py::handle scope, std::string_view name) {
  py::class_<ActionSchema, std::shared_ptr<ActionSchema>>(
      scope, std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<ActionSchema>())
      .def(py::init([](const std::string& action_name,
                       const std::vector<NameAndMimetype>& inputs,
                       const std::vector<NameAndMimetype>& outputs) {
             NameToMimetype input_map(inputs.begin(), inputs.end());
             NameToMimetype output_map(outputs.begin(), outputs.end());
             return std::make_shared<ActionSchema>(action_name, input_map,
                                                   output_map);
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NameAndMimetype>()),
           py::arg_v("outputs", std::vector<NameAndMimetype>()))
      .def_readwrite("name", &ActionSchema::name)
      .def_readwrite("inputs", &ActionSchema::inputs)
      .def_readwrite("outputs", &ActionSchema::outputs)
      .def("__repr__",
           [](const ActionSchema& def) { return absl::StrCat(def); })
      .doc() = "An Evergreen ActionSchema.";
}

/// @private
void BindActionRegistry(py::handle scope, std::string_view name) {
  py::class_<ActionRegistry, std::shared_ptr<ActionRegistry>>(
      scope, std::string(name).c_str())
      .def(py::init([]() { return std::make_shared<ActionRegistry>(); }))
      .def(MakeSameObjectRefConstructor<ActionRegistry>())
      .def(
          "register",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             const ActionSchema& def, VoidActionHandler handler) {
            return self->Register(
                name, def, MakeStatusAwareActionHandler(std::move(handler)));
          },
          py::arg("name"), py::arg("def"), py::arg("handler"))
      .def("make_action_message", &ActionRegistry::MakeActionMessage,
           py::arg("name"), py::arg("id"))
      .def(
          "make_action",
          [](const std::shared_ptr<ActionRegistry>& self,
             const std::string& name, const std::string& id, NodeMap* node_map,
             EvergreenStream* stream, Session* session) {
            auto action = self->MakeAction(name, id);
            action->BindNodeMap(node_map);
            action->BindStream(stream);
            action->BindSession(session);
            return std::shared_ptr(std::move(action));
          },
          py::arg("name"), py::arg_v("id", ""), py::arg_v("node_map", nullptr),
          py::arg_v("stream", nullptr), py::arg_v("session", nullptr));
}

/// @private
void BindAction(py::handle scope, std::string_view name) {
  py::class_<Action, std::shared_ptr<Action>>(scope, std::string(name).c_str())
      .def(MakeSameObjectRefConstructor<Action>())
      .def(py::init([](ActionSchema schema, const std::string& id = "") {
        return std::make_shared<Action>(std::move(schema), id);
      }))
      .def("run",
           [](const std::shared_ptr<Action>& action) {
             if (const absl::Status status = action->Run(); !status.ok()) {
               throw std::runtime_error(status.ToString());
             }
           })
      .def(
          "call",
          [](const std::shared_ptr<Action>& action) {
            if (const absl::Status status = action->Call(); !status.ok()) {
              throw py::value_error(status.ToString());
            }
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_registry",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetRegistry());
           })
      .def("get_session",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetSession());
           })
      .def("get_node_map",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetNodeMap());
           })
      .def("get_stream",
           [](const std::shared_ptr<Action>& action) {
             return ShareWithNoDeleter(action->GetStream());
           })
      .def("get_id", &Action::GetId)
      .def("get_schema", &Action::GetSchema, py::return_value_policy::reference)
      .def(
          "get_node",
          [](const std::shared_ptr<Action>& action, const std::string& id) {
            return ShareWithNoDeleter(action->GetNode(id));
          },
          py::arg("id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "get_input",
          [](const std::shared_ptr<Action>& action, const std::string& id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetInput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_output",
          [](const std::shared_ptr<Action>& action, const std::string& id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetOutput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action_in_same_session",
          [](const std::shared_ptr<Action>& action, const std::string& name,
             const std::string& id) {
            return std::shared_ptr(action->MakeActionInSameSession(name, id));
          },
          py::arg("name"), py::arg_v("id", ""))
      .def(
          "bind_handler",
          [](const std::shared_ptr<Action>& self, VoidActionHandler handler) {
            return self->BindHandler(
                MakeStatusAwareActionHandler(std::move(handler)));
          },
          py::arg("handler"));
}

/// @private
py::module_ MakeActionsModule(py::module_ scope, std::string_view module_name) {
  py::module_ actions = scope.def_submodule(std::string(module_name).c_str(),
                                            "Evergreen Actions interface.");

  BindActionSchema(actions, "ActionSchema");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace eglt::pybindings
