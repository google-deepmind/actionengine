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

"""A Pythonic wrapper for the raw pybind11 NodeMap bindings."""

from actionengine import _C
from actionengine import async_node
from actionengine import utils
from actionengine.chunk_store import ChunkStoreFactory

AsyncNode = async_node.AsyncNode


class NodeMap(_C.nodes.NodeMap):
    """An ActionEngine NodeMap.

    Simply contains AsyncNodes. Calls are thread-safe.
    """

    # pylint: disable-next=[useless-parent-delegation]
    def __init__(self, chunk_store_factory: ChunkStoreFactory | None = None):
        """Initializes the NodeMap.

        If chunk_store_factory is provided, it will be used to create ChunkStores
        for each AsyncNode.

        Args:
          chunk_store_factory: A function that takes no arguments and returns a
            ChunkStore instance.
        """
        if chunk_store_factory is None:
            super().__init__()
        else:
            super().__init__(chunk_store_factory)

    def get(self, node_id: str) -> AsyncNode:
        """Returns the AsyncNode with the given ID."""
        return utils.wrap_pybind_object(AsyncNode, super().get(node_id))

    def extract(self, node_id: str) -> AsyncNode:
        """Extracts the AsyncNode with the given ID.

        Removes it from the NodeMap and transfers ownership to the caller."""
        return utils.wrap_pybind_object(AsyncNode, super().extract(node_id))

    # pylint: disable-next=[useless-parent-delegation]
    def contains(self, node_id: str) -> bool:
        """Returns whether the NodeMap contains the given ID."""
        return super().contains(node_id)

    def __getitem__(self, node_id: str) -> AsyncNode:
        """Returns the AsyncNode with the given ID."""
        return self.get(node_id)

    def __contains__(self, node_id: str) -> bool:
        """Returns whether the NodeMap contains the given ID."""
        return self.contains(node_id)
