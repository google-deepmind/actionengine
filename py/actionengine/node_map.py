"""A Pythonic wrapper for the raw pybind11 NodeMap bindings."""

import actionengine.actionengine_pybind11 as actionengine_pybind11
from actionengine import async_node
from actionengine import data
from actionengine import utils

AsyncNode = async_node.AsyncNode
ChunkStoreFactory = data.ChunkStoreFactory


class NodeMap(actionengine_pybind11.NodeMap):
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
        return utils.wrap_pybind_object(
            AsyncNode, super().get(node_id)
        )  # pytype: disable=attribute-error

    def extract(self, node_id: str) -> AsyncNode:
        """Extracts the AsyncNode with the given ID.

        Removes it from the NodeMap and transfers ownership to the caller."""
        return utils.wrap_pybind_object(AsyncNode, super().extract(node_id))

    # pylint: disable-next=[useless-parent-delegation]
    def contains(self, node_id: str) -> bool:
        """Returns whether the NodeMap contains the given ID."""
        return super().contains(node_id)  # pytype: disable=attribute-error

    def __getitem__(self, node_id: str) -> AsyncNode:
        """Returns the AsyncNode with the given ID."""
        return self.get(node_id)

    def __contains__(self, node_id: str) -> bool:
        """Returns whether the NodeMap contains the given ID."""
        return self.contains(node_id)
