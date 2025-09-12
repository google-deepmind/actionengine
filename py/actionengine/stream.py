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
import inspect
import uuid
from typing import Awaitable, Callable, Optional, ParamSpec, TypeVar

from actionengine import _C
from actionengine import data


P = ParamSpec("P")
T = TypeVar("T")

SyncFn = Callable[P, T]
AsyncFn = Callable[P, Awaitable[T]]

SyncSendFn = Callable[[data.WireMessage], None]
AsyncSendFn = Callable[[data.WireMessage], Awaitable[None]]
SendFn = SyncSendFn | AsyncSendFn

SyncReceiveFn = Callable[[float], Optional[data.WireMessage]]
AsyncReceiveFn = Callable[
    [float],
    Awaitable[Optional[data.WireMessage]],
]
ReceiveFn = SyncReceiveFn | AsyncReceiveFn


def _wrap_handler(
    handler: SyncFn | AsyncFn,
) -> AsyncFn:
    if inspect.iscoroutinefunction(handler):
        return handler
    else:

        async def async_handler(*args: P.args, **kwargs: P.kwargs) -> T:
            return await asyncio.to_thread(handler, *args, **kwargs)

        return async_handler


class WireStream(_C.service.WireStream):
    """A Pythonic wrapper for the raw pybind11 WireStream bindings."""

    async def send(self, message: data.WireMessage) -> None:
        """Sends a message to the stream."""
        ...

    async def receive(
        self, timeout: float = -1.0
    ) -> Optional[data.WireMessage]:
        """Receives a message from the stream."""
        ...

    async def accept(self) -> None:
        """Accepts the stream.

        Should only be called by the server side (the one receiving the initial
        connection request).
        """
        ...

    async def start(self) -> None:
        """Starts the stream.

        Should be called by the client side (the one initiating the connection).
        """
        ...

    async def abort(self) -> None:
        """Aborts the stream.

        This indicates that the stream is being closed due to an error.
        """
        ...

    async def half_close(self) -> None:
        """(Half-)closes the stream.

        After calling this method, no more messages should be sent on the
        stream. The peer may continue to send messages until it also half-closes
        the stream.
        """
        ...

    def get_id(self) -> str:
        """Returns the unique identifier of the stream."""
        ...

    def set_exception(self, exception: Exception) -> None:
        """Sets an exception that occurred during stream processing.

        This exception must be raised when get_status() is called.
        """
        ...

    def get_status(self) -> None:
        """Checks the status of the stream.

        If an exception was set via set_exception(), it is raised here.
        """
        ...


class WireStreamAdapter(WireStream):
    _send: AsyncSendFn
    _receive: AsyncReceiveFn
    _stream_id: str

    _exception: Exception | None
    _half_closed: bool
    _recv_task: asyncio.Task | None

    def __init__(
        self,
        send: SendFn,
        receive: ReceiveFn,
        stream_id: str = "",
    ):
        super().__init__()
        self._send = _wrap_handler(send)
        self._receive = _wrap_handler(receive)
        self._stream_id = stream_id or str(uuid.uuid4())

        self._exception = None
        self._half_closed = False
        self._recv_task = None

    async def send(self, message: data.WireMessage) -> None:
        await self._send(message)

    async def receive(self, timeout: float = -1.0) -> data.WireMessage | None:
        message: data.WireMessage | None = await self._receive(timeout)

        # No more messages after an empty optional
        if message is None:
            return None

        # No more messages after an empty message (proper half-close from peer)
        if not message.actions and not message.node_fragments:
            return None

        return message

    async def accept(self) -> None:
        pass

    async def start(self) -> None:
        pass

    async def abort(self) -> None:
        # TODO: Implement abort logic (communicating an error)
        return await self.half_close()

    async def half_close(self) -> None:
        if self._half_closed:
            return

        self._half_closed = True
        await self._send(data.WireMessage())

    def get_id(self) -> str:
        return self._stream_id

    def set_exception(self, exception: Exception) -> None:
        self._exception = exception

    def get_status(self) -> None:
        if self._exception:
            raise self._exception


class InProcessWireStream(WireStream):
    """A WireStream implementation for in-process communication.

    This class uses asyncio Queues to simulate sending and receiving messages
    over a stream. It is useful for testing and in-process communication
    scenarios.
    """

    _send_queue: asyncio.Queue[data.WireMessage] | None
    _receive_queue: asyncio.Queue[data.WireMessage]
    _adapter: WireStreamAdapter
    _peer: Optional["InProcessWireStream"]

    def __init__(self, stream_id: str = "", capacity: int = 16):
        super().__init__()
        self._send_queue = None
        self._receive_queue = asyncio.Queue(capacity)
        self._adapter = WireStreamAdapter(
            send=self._send,
            receive=self._receive,
            stream_id=stream_id,
        )
        self._peer = None

    async def _send(self, message: data.WireMessage) -> None:
        if self._send_queue is None:
            raise RuntimeError("No peer is paired to this stream")
        await self._send_queue.put(message)

    async def send(self, message: data.WireMessage) -> None:
        return await self._adapter.send(message)

    async def receive(self, timeout: float = -1.0) -> data.WireMessage | None:
        message = await self._adapter.receive(timeout)
        return message

    async def accept(self) -> None:
        if self._peer is None:
            raise RuntimeError("No peer is paired to this stream")
        if self._send_queue is not None:
            raise RuntimeError("A stream is already accepted")
        self._send_queue = self._peer._receive_queue

    async def start(self) -> None:
        if self._peer is None:
            raise RuntimeError("No peer is paired to this stream")
        if self._send_queue is not None:
            raise RuntimeError("A stream is already started")
        self._send_queue = self._peer._receive_queue

    async def abort(self) -> None:
        return await self._adapter.abort()

    async def half_close(self) -> None:
        result = await self._adapter.half_close()
        self._send_queue = None
        return result

    async def pair_with(self, peer: "InProcessWireStream") -> None:
        if self._peer is not None or peer._peer is not None:
            raise RuntimeError("One of the streams is already paired")
        self._peer = peer
        peer._peer = self
        await asyncio.gather(self.start(), peer.accept())

    async def unpair(self) -> None:
        if self._peer is None:
            raise RuntimeError("No peer is paired to this stream")
        if self._peer._peer is not self:
            raise RuntimeError(
                "Peer is not paired or is paired to another stream"
            )
        await self._peer._unpair()
        await self._unpair()

    async def _unpair(self) -> None:
        if self._send_queue is not None:
            raise RuntimeError("Stream is still active")
        self._peer = None

    async def _receive(self, timeout: float = -1.0):
        try:
            return await asyncio.wait_for(
                self._receive_queue.get(), timeout if timeout >= 0 else None
            )
        except asyncio.TimeoutError:
            raise
