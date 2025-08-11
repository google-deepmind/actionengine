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

#include "actionengine/actions/actions_pybind11.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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

#include "actionengine/actions/action.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

struct FutureOrTaskHolder {
  explicit FutureOrTaskHolder(py::object obj) {
    // Determine if the object is a Future or a Task.
    is_task = py::isinstance(obj, py::module_::import("asyncio").attr("Task"));
    future_or_task = std::move(obj);

    // // Remove the reference to the Future or Task from Python's
    // // garbage collector on completion of the action.
    // // This is to prevent memory leaks in case the action is not awaited.
    // if (is_task) {
    //   future_or_task.attr("add_done_callback")(
    //       [this](py::handle) { future_or_task = py::none(); });
    // }
  }

  ~FutureOrTaskHolder() {
    if (!is_task) {
      if (thread::Cancelled()) {
        future_or_task.attr("cancel")();
        return;
      }
      auto _ =
          future_or_task.attr("result")();  // Wait for the Future to finish.
    }
  }

  py::object future_or_task;
  bool is_task;
};

ActionHandler MakeStatusAwareActionHandler(py::handle py_handler) {
  py_handler.inc_ref();
  const py::function iscoroutinefunction =
      py::module_::import("inspect").attr("iscoroutinefunction");
  const bool is_coroutine = py::cast<bool>(iscoroutinefunction(py_handler));

  ActionHandler handler =
      [py_handler,
       is_coroutine](const std::shared_ptr<Action>& action) -> absl::Status {
    py::gil_scoped_acquire gil;

    bool await_future_result = false;
    auto from_session_tag =
        static_cast<internal::FromSessionTag*>(action->GetUserData());
    if (from_session_tag != nullptr) {
      await_future_result = true;
    }
    action->SetUserData(nullptr);

    try {
      if (is_coroutine) {
        // If the handler is a coroutine, we need to run it in the event loop.

        const py::function get_running_loop =
            py::module_::import("asyncio").attr("get_running_loop");
        py::object loop = py::none();
        try {
          loop = get_running_loop();
        } catch (py::error_already_set&) {
          // No running loop found, we will use the globally saved one.
        }

        const py::object coro = py_handler(action);
        if (!loop.is_none()) {
          action->SetUserData(std::make_shared<FutureOrTaskHolder>(
              loop.attr("create_task")(coro)));
          return absl::OkStatus();
        }

        ASSIGN_OR_RETURN(
            py::object future,
            RunThreadsafeIfCoroutine(coro, GetGloballySavedEventLoop(),
                                     /*return_future=*/true));
        if (!await_future_result) {
          action->SetUserData(
              std::make_shared<FutureOrTaskHolder>(std::move(future)));
          return absl::OkStatus();
        }
        auto _ = future.attr("result")();  // Wait for the coroutine to finish.
        return absl::OkStatus();
      } else {
        // If the handler is not a coroutine, we can call it directly.
        auto _ = py_handler(action);
      }
    } catch (py::error_already_set& e) {
      return absl::InternalError(
          absl::StrCat("Python error in action handler: ", e.what()));
    }
    return absl::OkStatus();
  };

  return handler;
}

void BindActionSchema(py::handle scope, std::string_view name) {
  py::classh<ActionSchema>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<ActionSchema>())
      .def(py::init([](std::string_view action_name,
                       const std::vector<NameAndMimetype>& inputs,
                       const std::vector<NameAndMimetype>& outputs) {
             NameToMimetype input_map(inputs.begin(), inputs.end());
             NameToMimetype output_map(outputs.begin(), outputs.end());
             return std::make_shared<ActionSchema>(std::string(action_name),
                                                   input_map, output_map);
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NameAndMimetype>()),
           py::arg_v("outputs", std::vector<NameAndMimetype>()))
      .def_readwrite("name", &ActionSchema::name)
      .def_readwrite("inputs", &ActionSchema::inputs)
      .def_readwrite("outputs", &ActionSchema::outputs)
      .def("__repr__",
           [](const ActionSchema& def) { return absl::StrCat(def); })
      .doc() = "An action schema.";
}

void BindActionRegistry(py::handle scope, std::string_view name) {
  py::classh<ActionRegistry>(scope, std::string(name).c_str())
      .def(py::init([]() { return std::make_shared<ActionRegistry>(); }))
      .def(MakeSameObjectRefConstructor<ActionRegistry>())
      .def(
          "register",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             const ActionSchema& def, py::function handler) {
            return self->Register(
                name, def, MakeStatusAwareActionHandler(std::move(handler)));
          },
          py::arg("name"), py::arg("def"), py::arg("handler"))
      .def("make_action_message", &ActionRegistry::MakeActionMessage,
           py::arg("name"), py::arg("id"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             std::string_view id, NodeMap* node_map,
             const std::shared_ptr<WireStream>& stream, Session* session) {
            auto action = self->MakeAction(name, id);
            action->BindNodeMap(node_map);
            action->BindStream(stream.get());
            action->BindSession(session);
            return std::shared_ptr(std::move(action));
          },
          py::arg("name"), py::arg_v("id", ""), py::arg_v("node_map", nullptr),
          py::arg_v("stream", nullptr), py::arg_v("session", nullptr),
          py::keep_alive<0, 4>(), py::keep_alive<0, 5>(),
          py::keep_alive<0, 6>(), pybindings::keep_event_loop_memo());
}

void BindAction(py::handle scope, std::string_view name) {
  py::classh<Action>(scope, std::string(name).c_str())
      .def(MakeSameObjectRefConstructor<Action>(), py::keep_alive<0, 1>())
      .def(py::init([](ActionSchema schema, std::string_view id = "") {
        return std::make_shared<Action>(std::move(schema), id);
      }))
      .def(
          "run",
          [](const std::shared_ptr<Action>& action)
              -> absl::StatusOr<std::shared_ptr<Action>> {
            RETURN_IF_ERROR(action->Run());
            return action;
          },
          py::call_guard<py::gil_scoped_acquire>())
      .def(
          "call",
          [](const std::shared_ptr<Action>& action) { return action->Call(); },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "wait_until_complete",
          [](const std::shared_ptr<Action>& action) { return action->Await(); },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "clear_inputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->ClearInputsAfterRun(clear);
          },
          py::arg_v("clear", true))
      .def(
          "clear_outputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->ClearOutputsAfterRun(clear);
          },
          py::arg_v("clear", true))
      .def(
          "cancel",
          [](const std::shared_ptr<Action>& self) { return self->Cancel(); },
          py::call_guard<py::gil_scoped_release>())
      .def("cancelled", &Action::Cancelled,
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
          [](const std::shared_ptr<Action>& action, std::string_view id) {
            return ShareWithNoDeleter(action->GetNode(id));
          },
          py::arg("id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "get_input",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetInput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_output",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetOutput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action_in_same_session",
          [](const std::shared_ptr<Action>& action, std::string_view name,
             std::string_view id) {
            return std::shared_ptr(action->MakeActionInSameSession(name, id));
          },
          py::arg("name"), py::arg_v("id", ""),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_handler",
          [](const std::shared_ptr<Action>& self, py::function handler) {
            return self->BindHandler(
                MakeStatusAwareActionHandler(std::move(handler)));
          },
          py::arg("handler"));
}

py::module_ MakeActionsModule(py::module_ scope, std::string_view module_name) {
  py::module_ actions = scope.def_submodule(std::string(module_name).c_str(),
                                            "ActionEngine Actions interface.");

  BindActionSchema(actions, "ActionSchema");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace act::pybindings
