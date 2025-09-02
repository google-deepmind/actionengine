# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from actionengine import _C
from actionengine import node_map
from actionengine import stream as eg_stream
from actionengine import data
from actionengine import utils

NodeMap = node_map.NodeMap


def do_nothing():
    pass


class Session(_C.service.Session):
    """A Pythonic wrapper for the raw pybind11 Session bindings."""

    def __init__(
        self,
        node_map: NodeMap | None = None,
        action_registry: _C.actions.ActionRegistry | None = None,
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
        message: data.WireMessage,
        stream: eg_stream.WireStream,
    ):
        """Dispatches a message to the session."""
        return await asyncio.to_thread(
            super().dispatch_message, message, stream
        )  # pytype: disable=attribute-error

    def dispatch_from(self, stream: eg_stream.WireStream, on_done=do_nothing):
        """Dispatches messages from the stream to the session."""
        super().dispatch_from(
            stream, on_done
        )  # pytype: disable=attribute-error
        self._streams.add(stream)

    def stop_dispatching_from(self, stream: eg_stream.WireStream):
        """Stops dispatching messages from the stream to the session."""
        self._streams.discard(stream)
        super().stop_dispatching_from(stream)  # pytype: disable=attribute-error
