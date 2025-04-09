"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from evergreen.evergreen import node_map
from evergreen.evergreen import stream as eg_stream
from evergreen.evergreen import types
from evergreen.evergreen import utils
from evergreen.evergreen_pybind11 import \
  service as service_pybind11

NodeMap = node_map.NodeMap


class Session(service_pybind11.Session):
  """A Pythonic wrapper for the raw pybind11 Session bindings."""

  def get_node_map(self) -> "NodeMap":
    """Returns the node map."""
    return utils.wrap_pybind_object(
        NodeMap,
        super().get_node_map(),  # pytype: disable=attribute-error
    )

  async def dispatch_message(
      self,
      message: types.SessionMessage,
      stream: eg_stream.EvergreenStream,
  ):
    """Dispatches a message to the session."""
    return await asyncio.to_thread(super().dispatch_message, message,
                                   stream)  # pytype: disable=attribute-error
