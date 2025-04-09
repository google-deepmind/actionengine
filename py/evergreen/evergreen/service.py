"""A Pythonic wrapper for the raw pybind11 Service bindings."""

import asyncio
import inspect
from typing import Awaitable
from typing import Callable

from evergreen.evergreen import actions
from evergreen.evergreen import session as eg_session
from evergreen.evergreen import stream as eg_stream
from evergreen.evergreen import utils
from evergreen.evergreen_pybind11 import \
  service as service_pybind11

ActionRegistry = actions.ActionRegistry
EvergreenStream = eg_stream.EvergreenStream
Session = eg_session.Session
StreamToSessionConnection = service_pybind11.StreamToSessionConnection


class EvergreenStream(eg_stream.EvergreenStream):
  """A Pythonic wrapper for the raw pybind11 EvergreenStream bindings."""

  def __init__(self, stream_id: str):
    self._stream_id = stream_id
    super().__init__()

  def accept(self):
    """Accepts the stream."""
    pass

  def start(self):
    """Starts the stream."""
    pass

  def close(self):
    """(Half-)closes the stream."""
    pass

  def get_id(self):
    return self._stream_id


AsyncConnectionHandler = Callable[[EvergreenStream, Session], Awaitable[None]]
SyncConnectionHandler = Callable[[EvergreenStream, Session], None]
ConnectionHandler = SyncConnectionHandler | AsyncConnectionHandler


def wrap_async_handler(
    handler: AsyncConnectionHandler,
) -> SyncConnectionHandler:
  """Wraps the given handler to run in the event loop."""
  loop = asyncio.get_running_loop()

  def sync_handler(stream: "EvergreenStream", session: "Session") -> None:
    result = asyncio.run_coroutine_threadsafe(
        handler(
            utils.wrap_pybind_object(EvergreenStream, stream),
            utils.wrap_pybind_object(Session, session),
        ),
        loop,
    )
    result.result()

  return sync_handler


def wrap_sync_handler(handler: SyncConnectionHandler) -> SyncConnectionHandler:
  def sync_handler(stream: "EvergreenStream", session: "Session") -> None:
    return handler(
        utils.wrap_pybind_object(EvergreenStream, stream),
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


class Service(service_pybind11.Service):
  """A Pythonic wrapper for the raw pybind11 Service bindings."""

  def __init__(
      self,
      action_registry: actions.ActionRegistry,
      connection_handler: ConnectionHandler | None = None,
  ):
    super().__init__(action_registry, wrap_handler(connection_handler))


class StreamToSessionConnection(StreamToSessionConnection):
  """A Pythonic wrapper for the raw pybind11 StreamToSessionConnection bindings."""

  def get_stream(self) -> "EvergreenStream":
    """Returns the stream."""
    return utils.wrap_pybind_object(
        EvergreenStream,
        super().get_stream(),  # pytype: disable=attribute-error
    )

  def get_session(self) -> "Session":
    """Returns the session."""
    return utils.wrap_pybind_object(
        Session,
        super().get_session(),  # pytype: disable=attribute-error
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
