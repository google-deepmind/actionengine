#ifndef EGLT_PYBIND11_EGLT_ACTIONS_H_
#define EGLT_PYBIND11_EGLT_ACTIONS_H_

#include <string_view>

#include "eglt/pybind11/pybind11_headers.h"

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindActionNode(py::handle scope, std::string_view name = "ActionNode");
void BindActionDefinition(py::handle scope,
                          std::string_view name = "ActionDefinition");
void BindActionRegistry(py::handle scope,
                        std::string_view name = "ActionRegistry");
void BindAction(py::handle scope, std::string_view name = "Action");

py::module_ MakeActionsModule(py::module_ scope,
                              std::string_view module_name = "actions");

}  // namespace eglt::pybindings

#endif  // EGLT_PYBIND11_EGLT_ACTIONS_H_
