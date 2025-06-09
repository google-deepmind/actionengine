"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from evergreen.evergreen_pybind11 import actions as actions_pybind11
from evergreen import node_map
from evergreen import stream as eg_stream
from evergreen import data
from evergreen import utils
from evergreen.evergreen_pybind11 import service as service_pybind11

NodeMap = node_map.NodeMap


class Session(service_pybind11.Session):
    """A Pythonic wrapper for the raw pybind11 Session bindings."""

    def __init__(
        self,
        node_map: NodeMap | None = None,
        action_registry: actions_pybind11.ActionRegistry | None = None,
    ):  # pytype: disable=name-error
        """Constructor for Session."""

        super().__init__(
            node_map, action_registry
        )  # pytype: disable=attribute-error

        self._node_map = node_map
        self._action_registry = action_registry
        self._add_python_specific_attributes()

    def _add_python_specific_attributes(self):
        self._streams = set()

    def get_node_map(self) -> "NodeMap":
        """Returns the node map."""
        return utils.wrap_pybind_object(
            NodeMap,
            super().get_node_map(),  # pytype: disable=attribute-error
        )

    async def dispatch_message(
        self,
        message: data.SessionMessage,
        stream: eg_stream.WireStream,
    ):
        """Dispatches a message to the session."""
        return await asyncio.to_thread(
            super().dispatch_message, message, stream
        )  # pytype: disable=attribute-error

    def dispatch_from(self, stream: eg_stream.WireStream):
        """Dispatches messages from the stream to the session."""
        super().dispatch_from(stream)  # pytype: disable=attribute-error
        self._streams.add(stream)

    def stop_dispatching_from(self, stream: eg_stream.WireStream):
        """Stops dispatching messages from the stream to the session."""
        self._streams.discard(stream)
        super().stop_dispatching_from(stream)  # pytype: disable=attribute-error
