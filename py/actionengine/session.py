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
from actionengine import node_map as eg_node_map
from actionengine import data
from actionengine import utils


def do_nothing():
    pass


class Session(_C.service.Session):
    """A Pythonic wrapper for the raw pybind11 Session bindings."""

    def __init__(
        self,
        node_map: _C.nodes.NodeMap | None = None,
        action_registry: _C.actions.ActionRegistry | None = None,
    ):
        """Constructor for Session."""

        super().__init__(node_map, action_registry)

        self._node_map = node_map
        self._action_registry = action_registry
        self._add_python_specific_attributes()

    def _add_python_specific_attributes(self):
        self._streams = set()

    def get_node_map(self) -> eg_node_map.NodeMap:
        """Returns the node map."""
        return utils.wrap_pybind_object(
            eg_node_map.NodeMap,
            super().get_node_map(),
        )

    async def dispatch_message(
        self,
        message: data.WireMessage,
        stream: _C.service.WireStream,
    ):
        """Dispatches a message to the session."""
        return await asyncio.to_thread(
            super().dispatch_message, message, stream
        )

    def dispatch_from(self, stream: _C.service.WireStream, on_done=do_nothing):
        """Dispatches messages from the stream to the session."""
        super().dispatch_from(stream, on_done)
        self._streams.add(stream)

    def stop_dispatching_from(self, stream: _C.service.WireStream):
        """Stops dispatching messages from the stream to the session."""
        self._streams.discard(stream)
        super().stop_dispatching_from(stream)
