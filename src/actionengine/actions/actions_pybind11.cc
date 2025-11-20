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
#include <pybind11/stl.h>
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
    future_or_task = std::move(obj);
  }

  py::object future_or_task;
};

ActionHandler MakeStatusAwareActionHandler(py::handle py_handler) {
  py_handler = py_handler.inc_ref();
  const py::function iscoroutinefunction =
      py::module_::import("inspect").attr("iscoroutinefunction");
  const bool is_coroutine = py::cast<bool>(iscoroutinefunction(py_handler));

  ActionHandler handler =
      [py_handler,
       is_coroutine](const std::shared_ptr<Action>& action) -> absl::Status {
    py::gil_scoped_acquire gil;

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
            const py::object future,
            RunThreadsafeIfCoroutine(coro, GetGloballySavedEventLoop(),
                                     /*return_future=*/true));

        thread::PermanentEvent done;
        auto future_done_callback = py::cpp_function([&done](py::handle) {
          py::gil_scoped_acquire gil;
          done.Notify();
        });
        future.attr("add_done_callback")(std::move(future_done_callback));
        {
          py::gil_scoped_release release;
          thread::Select({done.OnEvent(), thread::OnCancel()});
        }

        const bool cancelled = thread::Cancelled();
        // If we were cancelled, we need to cancel the future.
        if (cancelled) {
          auto _ = future.attr("cancel")();
        }
        // Even if we were cancelled, we still need to wait for the future to
        // finish to avoid resource leaks and no-GIL refcount change attempts.
        {
          const absl::Time deadline = !cancelled
                                          ? absl::InfiniteFuture()
                                          : absl::Now() + absl::Seconds(10);
          py::gil_scoped_release release;
          thread::SelectUntil(deadline, {done.OnEvent()});
        }
        if (thread::Cancelled()) {
          return absl::CancelledError(
              "Action handler was cancelled while waiting for the "
              "coroutine.");
        }

        // At this point, the future is done, but it might have failed.
        // If it failed, this will catch and propagate the exception.
        auto _ = future.attr("result")();

        return absl::OkStatus();
      }
      // (else), if the handler is not a coroutine, we can call it directly.
      auto _ = py_handler(action);
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
                       const std::vector<NameAndMimetype>& outputs,
                       std::string_view description = "") {
             NameToMimetype input_map(inputs.begin(), inputs.end());
             NameToMimetype output_map(outputs.begin(), outputs.end());
             return std::make_shared<ActionSchema>(std::string(action_name),
                                                   input_map, output_map,
                                                   std::string(description));
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NameAndMimetype>()),
           py::arg_v("outputs", std::vector<NameAndMimetype>()),
           py::arg_v("description", ""))
      .def_readwrite("name", &ActionSchema::name)
      .def_readwrite("inputs", &ActionSchema::inputs)
      .def_readwrite("outputs", &ActionSchema::outputs)
      .def_readwrite("description", &ActionSchema::description)
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
          py::arg("name"), py::arg("definition"), py::arg("handler"))
      .def("make_action_message", &ActionRegistry::MakeActionMessage,
           py::arg("name"), py::arg("action_id"),
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
          py::arg("name"), py::arg_v("action_id", ""),
          py::arg_v("node_map", nullptr), py::arg_v("stream", nullptr),
          py::arg_v("session", nullptr), py::keep_alive<0, 4>(),
          py::keep_alive<0, 5>(), py::keep_alive<0, 6>(),
          pybindings::keep_event_loop_memo())
      .def(
          "is_registered",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name) {
            return self->IsRegistered(action_name);
          },
          py::arg("name"))
      .def(
          "get_schema",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name)
              -> absl::StatusOr<std::shared_ptr<const ActionSchema>> {
            RETURN_IF_ERROR(
                self->IsRegistered(action_name)
                    ? absl::OkStatus()
                    : absl::NotFoundError(absl::StrCat("Action not found: '",
                                                       action_name, "'")));
            return std::shared_ptr<const ActionSchema>(
                &self->GetSchema(action_name), [](const ActionSchema*) {});
          },
          py::arg("name"))
      .def("list_registered_actions",
           [](const std::shared_ptr<ActionRegistry>& self) {
             return self->ListRegisteredActions();
           })
      .doc() = "Registry for action schemas and handlers.";
}

void BindAction(py::handle scope, std::string_view name) {
  py::classh<Action>(scope, std::string(name).c_str(),
                     py::release_gil_before_calling_cpp_dtor())
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
          pybindings::keep_event_loop_memo(),
          py::call_guard<py::gil_scoped_release>())
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
          py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
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
          py::arg("name"), py::arg_v("action_id", ""),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_handler",
          [](const std::shared_ptr<Action>& self, py::function handler) {
            return self->BindHandler(
                MakeStatusAwareActionHandler(std::move(handler)));
          },
          py::arg("handler"))
      .def(
          "bind_streams_on_inputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->BindStreamsOnInputsByDefault(bind);
          },
          py::arg("bind"))
      .def(
          "bind_streams_on_outputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->BindStreamsOnOutputsByDefault(bind);
          },
          py::arg("bind"))
      .def(
          "bind_node_map",
          [](const std::shared_ptr<Action>& self, NodeMap* node_map) {
            self->BindNodeMap(node_map);
          },
          py::arg("node_map"));
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
