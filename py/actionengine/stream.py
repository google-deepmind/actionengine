"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from actionengine import data
from actionengine import utils
from actionengine.actionengine_pybind11 import service as service_pybind11


class WireStream(service_pybind11.WireStream):
    """A Pythonic wrapper for the raw pybind11 WireStream bindings."""

    async def receive(self) -> data.SessionMessage | None:
        """Receives a message from the stream."""
        return await asyncio.to_thread(
            super().receive
        )  # pytype: disable=attribute-error

    async def send(self, message: data.SessionMessage) -> None:
        """Sends a message to the stream."""
        await asyncio.to_thread(
            super().send, message
        )  # pytype: disable=attribute-error
