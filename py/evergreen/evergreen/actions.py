"""A Pythonic wrapper for the raw pybind11 Actions bindings."""

import asyncio
import inspect
from typing import Any
from typing import Awaitable
from typing import Callable

from evergreen.evergreen import async_node
from evergreen.evergreen import node_map as eg_node_map
from evergreen.evergreen import data
from evergreen.evergreen import utils
from evergreen.evergreen_pybind11 import actions as actions_pybind11

AsyncNode = async_node.AsyncNode
NodeMap = eg_node_map.NodeMap

AsyncActionHandler = Callable[["Action"], Awaitable[None]]
SyncActionHandler = Callable[["Action"], None]
ActionHandler = SyncActionHandler | AsyncActionHandler


def wrap_async_handler(handler: AsyncActionHandler) -> SyncActionHandler:
    """Wraps the given handler to run in the event loop."""
    loop = asyncio.get_running_loop()

    def sync_handler(action: Action) -> None:
        future = asyncio.run_coroutine_threadsafe(
            handler(utils.wrap_pybind_object(Action, action)), loop
        )
        future.result()

    return sync_handler


def wrap_sync_handler(handler: SyncActionHandler) -> SyncActionHandler:
    def sync_handler(action: Action) -> None:
        return handler(utils.wrap_pybind_object(Action, action))

    return sync_handler


def wrap_handler(handler: ActionHandler) -> ActionHandler:
    if inspect.iscoroutinefunction(handler):
        return wrap_async_handler(handler)
    else:
        return wrap_sync_handler(handler)


class ActionSchema(actions_pybind11.ActionSchema):
    """A schema of an Evergreen Action."""

    # pylint: disable-next=[useless-parent-delegation]
    def __init__(
        self,
        *,
        name: str = "",
        inputs: list[tuple[str, str]],
        outputs: list[tuple[str, str]],
    ):
        """Constructor for ActionSchema.

        Args:
          name: The name of the action definition.
          inputs: The inputs of the action definition.
          outputs: The outputs of the action definition.
        """
        super().__init__(name=name, inputs=inputs, outputs=outputs)


class ActionRegistry(actions_pybind11.ActionRegistry):
    """A Pythonic wrapper for the raw pybind11 ActionRegistry bindings."""

    def register(
        self,
        name: str,
        schema: actions_pybind11.ActionSchema,
        handler: Any,
    ) -> None:
        """Registers an action schema and handler."""

        if not schema.name:
            schema.name = name
        super().register(
            name, schema, wrap_handler(handler)
        )  # pytype: disable=attribute-error

    # pylint: disable-next=[useless-parent-delegation]
    def make_action_message(
        self, name: str, action_id: str
    ) -> data.ActionMessage:
        """Creates an action message.

        Args:
          name: The name of the action. Must be registered by the time of the call.
          action_id: The id of the action.

        Returns:
          The action message.
        """
        return super().make_action_message(
            name, action_id
        )  # pytype: disable=attribute-error

    # pytype: disable=name-error
    def make_action(
        self,
        name: str,
        action_id: str = "",
        *,
        node_map: NodeMap | None = None,
        stream: "EvergreenWireStream | None" = None,
        session: "Session | None" = None,
    ) -> "Action":
        # pytype: enable=name-error
        """Creates an action."""

        action = utils.wrap_pybind_object(
            Action,
            super().make_action(  # pytype: disable=attribute-error
                name,
                action_id,
                node_map,
                stream,
                session,
            ),
        )

        action._node_map = node_map
        action._stream = stream
        action._session = session

        return action


class Action(actions_pybind11.Action):
    """A Pythonic wrapper for the raw pybind11 Action bindings."""

    # pytype: disable=name-error
    def __init__(
        self,
        schema: ActionSchema,
        handler: Any,
        action_id: str = "",
        *,
        node_map: NodeMap | None = None,
        stream: "EvergreenWireStream | None" = None,
        session: "Session | None" = None,
    ):
        # pytype: enable=name-error
        """Constructor for Action."""

        self._schema = schema
        self._node_map = node_map
        self._stream = stream
        self._session = session

        super().__init__(
            schema,
            wrap_handler(handler),
            action_id,
            node_map,
            stream,
            session,
        )

    def _add_python_specific_attributes(self):
        """Adds Python-specific attributes to the action."""
        self._schema = self.get_schema()

    async def call(self) -> None:
        """Calls the action by sending the action message to the stream."""
        await asyncio.to_thread(self.call_sync)

    def call_sync(self) -> None:
        """Calls the action by sending the action message to the stream."""
        super().call()  # pytype: disable=attribute-error

    def get_registry(self) -> ActionRegistry:
        """Returns the action registry from attached session."""
        return utils.wrap_pybind_object(
            ActionRegistry, super().get_registry()
        )  # pytype: disable=attribute-error

    # pytype: disable=name-error
    # pylint: disable-next=[useless-parent-delegation]
    def get_stream(self) -> "EvergreenWireStream | None":
        # pytype: enable=name-error
        """Returns attached stream."""
        return super().get_stream()  # pytype: disable=attribute-error

    def get_input(
        self, name: str, bind_stream: bool | None = None
    ) -> AsyncNode:
        """Returns the input node with the given name.

        Args:
          name: The name of the input node.
          bind_stream: Whether to bind the stream to the input node. Binding the
            stream to the input node means that in addition to writing chunks to the
            ChunkStore, the input node will also write them to the stream. If None,
            the default behavior is used, which is to bind streams to input nodes if
            the action is called (client-side), and to output nodes if the action is
            run (server-side).
        """
        return utils.wrap_pybind_object(
            AsyncNode,
            super().get_input(name, bind_stream),
            # pytype: disable=attribute-error
        )

    def get_output(
        self, name: str, bind_stream: bool | None = None
    ) -> AsyncNode:
        """Returns the output node with the given name.

        Args:
          name: The name of the output node.
          bind_stream: Whether to bind the stream to the output node. Binding the
            stream to the output node means that in addition to writing chunks to
            the ChunkStore, the output node will also write them to the stream. If
            None, the default behavior is used, which is to bind streams to output
            nodes if the action is run (server-side), and to input nodes if the
            action is called (client-side).
        """
        return utils.wrap_pybind_object(
            AsyncNode,
            super().get_output(name, bind_stream),
            # pytype: disable=attribute-error
        )

    def __getitem__(self, name: str) -> AsyncNode:
        """Returns the node with the given name."""
        node = None

        schema = self.get_schema()
        for param in schema.inputs:
            if param.name.endswith(name):
                node = self.get_input(name)
                break
        for param in schema.outputs:
            if param.name.endswith(name):
                node = self.get_output(name)
                break

        if node is None:
            raise KeyError(f"Node with name {name} not found.")

        return utils.wrap_pybind_object(AsyncNode, node)

    def make_action_in_same_session(self, name: str) -> "Action":
        """Creates an action in the same session."""
        return utils.wrap_pybind_object(
            Action,
            super().make_action_in_same_session(name),
            # pytype: disable=attribute-error
        )

    def run(self) -> Awaitable[None]:
        """Runs the action."""
        background_task = utils.schedule_global_task(
            asyncio.to_thread(super().run)
        )  # pytype: disable=attribute-error
        return background_task
