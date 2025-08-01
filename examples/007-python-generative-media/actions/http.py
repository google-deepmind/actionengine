import base64
import uuid
from typing import Any

import evergreen


def _initialize_global_settings():
    """Initializes global settings for the Action Engine client."""
    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


class ActionEngineClient:
    """A client for interacting with the Action Engine.

    This client provides methods to interact with the Action Engine's
    stream, node map, action registry, and session.
    """

    _global_instance: "ActionEngineClient | None" = None

    def __init__(
        self,
        stream: evergreen.WireStream,
        node_map: evergreen.NodeMap | None = None,
        action_registry: evergreen.ActionRegistry | None = None,
        session: evergreen.Session | None = None,
    ):
        self._stream = stream
        self._node_map = node_map
        self._action_registry = action_registry
        self._session = session

    def set_action_registry(
        self, action_registry: evergreen.ActionRegistry
    ) -> None:
        """Sets the action registry for the client."""
        self._action_registry = action_registry

    def get_action_registry(self) -> evergreen.ActionRegistry:
        """Returns the action registry for the client."""
        return self._action_registry

    def get_session(self):
        if self._session is None:
            self._session = evergreen.Session(
                self._node_map, self._action_registry
            )
            self._session.dispatch_from(self._stream)
        return self._session

    def make_action(self, name: str, action_id: str = "") -> evergreen.Action:
        """Creates an action with the given name and action ID."""
        if self._action_registry is None:
            raise ValueError("Action registry is not set in the client.")
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
                stream=evergreen.webrtc.make_webrtc_evergreen_stream(
                    str(uuid.uuid4()),
                    "demoserver",
                    "demos.helena.direct",
                    19000,
                ),
                node_map=evergreen.NodeMap(),
                action_registry=None,
            )
        return ActionEngineClient._global_instance


def make_final_node_fragment(node_id: str, seq: int) -> evergreen.NodeFragment:
    """Creates a final node fragment for the given action."""
    return evergreen.NodeFragment(
        id=node_id,
        seq=seq,
        chunk=evergreen.Chunk(
            metadata=evergreen.ChunkMetadata(
                mimetype="application/octet-stream"
            ),
            data=b"",
        ),
        continued=False,
    )


def fragment_to_json(
    fragment: evergreen.NodeFragment,
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
