import asyncio
import uuid

import actionengine


class PersistentSession:
    _node_map: actionengine.NodeMap
    _action_registry: actionengine.ActionRegistry
    _session: actionengine.Session
    _stream: actionengine.WireStreamAdapter

    _outgoing_messages: asyncio.Queue[actionengine.WireMessage]
    _incoming_messages: asyncio.Queue[actionengine.WireMessage]

    _lock: asyncio.Lock

    def __init__(
        self,
        session_id: str = "",
        action_registry: actionengine.ActionRegistry | None = None,
    ):
        session_id = session_id or str(uuid.uuid4())

        self._node_map = actionengine.NodeMap()
        self._action_registry = action_registry
        self._session = actionengine.Session(
            node_map=self._node_map, action_registry=self._action_registry
        )

        self._lock = asyncio.Lock()

        self._incoming_messages = asyncio.Queue()
        self._outgoing_messages = asyncio.Queue()
        self._stream = actionengine.WireStreamAdapter(
            self._send, self._receive, stream_id=session_id
        )

        self._session.dispatch_from(self._stream)

    def get_session(self) -> actionengine.Session:
        return self._session

    def get_node_map(self) -> actionengine.NodeMap:
        return self._node_map

    def get_stream(self) -> actionengine.WireStreamAdapter:
        return self._stream

    def get_action_registry(self) -> actionengine.ActionRegistry:
        return self._action_registry

    def set_action_registry(self, registry: actionengine.ActionRegistry):
        self._action_registry = registry
        self._session.set_action_registry(registry)

    def make_action(self, name: str, action_id: str = ""):
        action = self._action_registry.make_action(
            name,
            action_id,
            node_map=self._node_map,
            stream=self._stream,
            session=self._session,
        )
        action.bind_streams_on_inputs_by_default(False)
        return action

    @property
    def id(self) -> str:
        return self._stream.get_id()

    @property
    def incoming_messages(self) -> asyncio.Queue[actionengine.WireMessage]:
        return self._incoming_messages

    @property
    def outgoing_messages(self) -> asyncio.Queue[actionengine.WireMessage]:
        return self._outgoing_messages

    async def _send(self, message: actionengine.WireMessage):
        return await self._outgoing_messages.put(message)

    async def _receive(
        self, timeout: float = -1.0
    ) -> actionengine.WireMessage | None:
        try:
            return await asyncio.wait_for(
                self._incoming_messages.get(),
                None if timeout < 0 else timeout,
            )
        except asyncio.TimeoutError:
            return None


class SessionRegistry:
    def __init__(self):
        self._sessions: dict[str, PersistentSession] = {}
        self._cleanup_tasks: dict[str, asyncio.Task] = {}
        self._ttl = 600  # 10 minutes

    def get(self, session_id: str) -> PersistentSession:
        if session_id not in self._sessions:
            raise KeyError(f"No session found for session_id: {session_id}")

        if current_cleanup_task := self._cleanup_tasks.pop(session_id, None):
            current_cleanup_task.cancel()

        self._cleanup_tasks[session_id] = asyncio.create_task(
            self._clear_session(session_id, self._ttl)
        )
        self._cleanup_tasks[session_id].add_done_callback(
            self._pop_if_same_task(session_id)
        )

        return self._sessions[session_id]

    def get_or_create(
        self,
        session_id: str,
        action_registry: actionengine.ActionRegistry,
    ) -> PersistentSession:
        if not session_id:
            raise ValueError("Session ID must be a non-empty string")

        if session_id not in self._sessions:
            self._sessions[session_id] = PersistentSession(
                session_id, action_registry
            )

        return self.get(session_id)

    def _pop_if_same_task(self, session_id: str):
        def callback(task: asyncio.Task):
            current_task = self._cleanup_tasks.get(session_id)
            if current_task is task:
                self._cleanup_tasks.pop(session_id, None)

        return callback

    async def _clear_session(self, session_id: str, timeout: float):
        try:
            await asyncio.sleep(timeout)
            session = self._sessions.pop(session_id, None)
            if session is not None:
                session.get_session().stop_dispatching_from(
                    session.get_stream()
                )

        except asyncio.CancelledError:
            pass


def get_session_registry() -> SessionRegistry:
    if not hasattr(get_session_registry, "registry"):
        get_session_registry.registry = SessionRegistry()
    return get_session_registry.registry
