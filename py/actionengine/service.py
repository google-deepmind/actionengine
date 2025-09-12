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

"""A Pythonic wrapper for the raw pybind11 Service bindings."""

import asyncio
import inspect
from typing import Awaitable
from typing import Callable

from actionengine import _C
from actionengine import actions
from actionengine import session as eg_session
from actionengine import stream as eg_stream
from actionengine import utils

Session = eg_session.Session
WireStream = eg_stream.WireStream


AsyncConnectionHandler = Callable[
    [_C.service.WireStream, Session], Awaitable[None]
]
SyncConnectionHandler = Callable[[_C.service.WireStream, Session], None]
ConnectionHandler = SyncConnectionHandler | AsyncConnectionHandler


def wrap_async_handler(
    handler: AsyncConnectionHandler,
) -> SyncConnectionHandler:
    """Wraps the given handler to run in the event loop."""
    loop = asyncio.get_running_loop()

    def sync_handler(stream: _C.service.WireStream, session: Session) -> None:
        result = asyncio.run_coroutine_threadsafe(
            handler(
                stream,
                utils.wrap_pybind_object(Session, session),
            ),
            loop,
        )
        result.result()

    return sync_handler


def wrap_sync_handler(handler: SyncConnectionHandler) -> SyncConnectionHandler:
    def sync_handler(stream: _C.service.WireStream, session: Session) -> None:
        return handler(
            stream,
            utils.wrap_pybind_object(Session, session),
        )

    return sync_handler


def wrap_handler(handler: ConnectionHandler | None) -> ConnectionHandler | None:
    if handler is None:
        return handler
    if inspect.iscoroutinefunction(handler):
        return wrap_async_handler(handler)
    else:
        return wrap_sync_handler(handler)


class Service(_C.service.Service):
    """A Pythonic wrapper for the raw pybind11 Service bindings."""

    def __init__(
        self,
        action_registry: actions.ActionRegistry,
        connection_handler: ConnectionHandler | None = None,
    ):
        super().__init__(action_registry, wrap_handler(connection_handler))


class StreamToSessionConnection(_C.service.StreamToSessionConnection):
    """A Pythonic wrapper for the raw pybind11 StreamToSessionConnection bindings."""

    def get_stream(self) -> _C.service.WireStream:
        """Returns the stream."""
        return super().get_stream()

    def get_session(self) -> Session:
        """Returns the session."""
        return utils.wrap_pybind_object(
            Session,
            super().get_session(),
        )

    def make_action(
        self,
        registry: actions.ActionRegistry,
        name: str,
        action_id: str = "",
    ) -> actions.Action:
        """Creates an action."""
        session = self.get_session()
        action = registry.make_action(
            name,
            action_id,
            node_map=session.get_node_map(),
            stream=self.get_stream(),
            session=session,
        )
        return action
