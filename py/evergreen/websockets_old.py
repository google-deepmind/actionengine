"""Websocket support for Evergreen."""

import asyncio
import uuid
from typing import Any
from typing import Awaitable
from typing import Callable

from evergreen import service as eg_service
from evergreen import data
from evergreen import umsgpack_extensions

import websockets

encode_well_known = umsgpack_extensions.encode_well_known
decode_well_known = umsgpack_extensions.decode_well_known

SessionMessage = data.SessionMessage

WebsocketHandler = Callable[
    [websockets.WebSocketCommonProtocol], Awaitable[Any]
]


class WebsocketEvergreenWireStream(eg_service.EvergreenWireStream):
    """A Websocket-based EvergreenWireStream."""

    def __init__(self, socket: websockets.WebSocketCommonProtocol):
        self._socket = socket
        super().__init__(str(uuid.uuid4()))

    async def send(self, message: SessionMessage):
        encoded_message = await asyncio.to_thread(encode_well_known, message)
        await self._socket.send(encoded_message)

    async def receive(self):
        try:
            message_bytes = await self._socket.recv()
            return await asyncio.to_thread(
                decode_well_known, message_bytes, SessionMessage
            )
        except websockets.ConnectionClosed:
            return None
        except asyncio.CancelledError:
            return None


class WebsocketEvergreenServer:
    """A Websocket-based EvergreenServer."""

    def __init__(self, service: eg_service.Service):
        self._service = service

    async def _ws_handler(self, socket: websockets.WebSocketCommonProtocol):
        eg_stream = WebsocketEvergreenWireStream(socket)
        connection = await asyncio.to_thread(
            self._service.establish_connection, eg_stream
        )
        await asyncio.to_thread(self._service.join_connection, connection)

    async def run(self, address: str = "::", port: int = 20000, **kwargs):
        """Serves the Evergreen service on the given port."""
        async with websockets.serve(
            self._ws_handler,
            address,
            port,
            max_size=kwargs.pop("max_size", 20 * 2**20),
            **kwargs,
        ) as server:
            await server.serve_forever()
