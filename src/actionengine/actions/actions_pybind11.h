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

#ifndef ACTIONENGINE_PYBIND11_ACTIONENGINE_ACTIONS_H_
#define ACTIONENGINE_PYBIND11_ACTIONENGINE_ACTIONS_H_

#include <string_view>

#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/eval.h>
#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/import_status_module.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

namespace act::pybindings {

namespace py = ::pybind11;

void BindActionSchema(py::handle scope, std::string_view name = "ActionSchema");
void BindActionRegistry(py::handle scope,
                        std::string_view name = "ActionRegistry");
void BindAction(py::handle scope, std::string_view name = "Action");

py::module_ MakeActionsModule(py::module_ scope,
                              std::string_view module_name = "actions");

}  // namespace act::pybindings

#endif  // ACTIONENGINE_PYBIND11_ACTIONENGINE_ACTIONS_H_
