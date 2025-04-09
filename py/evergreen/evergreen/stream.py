"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from evergreen.evergreen import types
from evergreen.evergreen import utils
from evergreen.evergreen_pybind11 import \
  service as service_pybind11


class EvergreenStream(service_pybind11.EvergreenStream):
  """A Pythonic wrapper for the raw pybind11 EvergreenStream bindings."""

  async def receive(self) -> types.SessionMessage | None:
    """Receives a message from the stream."""
    return await asyncio.to_thread(
        super().receive)  # pytype: disable=attribute-error

  async def send(self, message: types.SessionMessage) -> None:
    """Sends a message to the stream."""
    await asyncio.to_thread(super().send,
                            message)  # pytype: disable=attribute-error
