import base64
import datetime
import time
import uuid
from typing import Any

import actionengine


def _initialize_global_settings():
    """Initializes global settings for the Action Engine client."""
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = False


class ActionEngineClient:
    """A client for interacting with the Action Engine.

    This client provides methods to interact with the Action Engine's
    stream, node map, action registry, and session.
    """

    _global_instance: "ActionEngineClient | None" = None

    def __init__(
        self,
        stream: actionengine.WireStream,
        node_map: actionengine.NodeMap | None = None,
        action_registry: actionengine.ActionRegistry | None = None,
        session: actionengine.Session | None = None,
    ):
        self._stream = stream
        self._node_map = node_map
        self._action_registry = action_registry
        self._session = session
        self._session_invalidated_at: datetime.datetime | None = None

    def set_action_registry(
        self, action_registry: actionengine.ActionRegistry
    ) -> None:
        """Sets the action registry for the client."""
        self._action_registry = action_registry

    def get_action_registry(self) -> actionengine.ActionRegistry:
        """Returns the action registry for the client."""
        return self._action_registry

    def _on_dispatch_done(self):
        self._session_invalidated_at = datetime.datetime.now()

    def _ensure_stream_and_session(self):
        if self._session_invalidated_at is None:
            return

        sleep_for = datetime.datetime.now() - self._session_invalidated_at
        if sleep_for := sleep_for.total_seconds() > 0:
            time.sleep(sleep_for)
        self._stream = actionengine.webrtc.make_webrtc_stream(
            str(uuid.uuid4()),
            "demoserver",
            "demos.helena.direct",
            19000,
        )
        self._session = None
        self._session_invalidated_at = None

    def get_session(self):
        self._ensure_stream_and_session()
        if self._session is None:
            self._session = actionengine.Session(
                self._node_map, self._action_registry
            )
            self._session.dispatch_from(self._stream, self._on_dispatch_done)
        return self._session

    def make_action(
        self, name: str, action_id: str = ""
    ) -> actionengine.Action:
        """Creates an action with the given name and action ID."""
        if self._action_registry is None:
            raise ValueError("Action registry is not set in the client.")
        self._ensure_stream_and_session()
        return self._action_registry.make_action(
            name,
            action_id=action_id,
            node_map=self._node_map,
            stream=self._stream,
            session=self.get_session(),
        )

    @staticmethod
    def global_instance() -> "ActionEngineClient":
        """Returns a global instance of ActionEngineClient."""
        if ActionEngineClient._global_instance is None:
            _initialize_global_settings()

            ActionEngineClient._global_instance = ActionEngineClient(
                stream=actionengine.webrtc.make_webrtc_stream(
                    str(uuid.uuid4()),
                    "demoserver",
                    "demos.helena.direct",
                    19000,
                ),
                node_map=actionengine.NodeMap(),
                action_registry=None,
            )
            print("Created global ActionEngineClient instance.")
        instance = ActionEngineClient._global_instance
        return instance


def make_final_node_fragment(
    node_id: str, seq: int
) -> actionengine.NodeFragment:
    """Creates a final node fragment for the given action."""
    return actionengine.NodeFragment(
        id=node_id,
        seq=seq,
        chunk=actionengine.Chunk(
            metadata=actionengine.ChunkMetadata(
                mimetype="application/octet-stream"
            ),
            data=b"",
        ),
        continued=False,
    )


def fragment_to_json(
    fragment: actionengine.NodeFragment,
    include_metadata: bool = True,
    base64_encode: bool = False,
) -> dict[str, Any]:
    """Converts a NodeFragment to a JSON-serializable dictionary."""

    data = (
        fragment.chunk.data.decode("utf-8")
        if not base64_encode
        else base64.b64encode(fragment.chunk.data)
    )
    metadata = {
        "mimetype": fragment.chunk.metadata.mimetype,
    }

    output = {
        "id": fragment.id,
        "seq": fragment.seq,
        "continued": fragment.continued,
        "chunk": {"data": data},
    }

    if include_metadata:
        output["chunk"]["metadata"] = metadata

    return output
