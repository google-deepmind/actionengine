from typing import Any

from evergreen.evergreen import actions
from evergreen.evergreen import async_node
from evergreen.evergreen import global_settings
from evergreen.evergreen import node_map
from evergreen.evergreen import serialisation
from evergreen.evergreen import service as eg_service
from evergreen.evergreen import session as eg_session
from evergreen.evergreen import stream as eg_stream
from evergreen.evergreen import data
from evergreen.evergreen import umsgpack_extensions
from evergreen.evergreen import utils
from evergreen.evergreen import websockets
from evergreen.evergreen import websockets_old
from evergreen import evergreen_pybind11


def to_bytes(obj: Any, mimetype: str = "") -> bytes:
    return evergreen_pybind11.to_bytes(obj, mimetype)


def to_chunk(obj: Any, mimetype: str = "") -> data.Chunk:
    return evergreen_pybind11.to_chunk(obj, mimetype)


Action = actions.Action
ActionSchema = actions.ActionSchema
ActionMessage = data.ActionMessage
ActionRegistry = actions.ActionRegistry

AsyncNode = async_node.AsyncNode

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata
ChunkStoreFactory = data.ChunkStoreFactory

EvergreenWireStream = eg_stream.EvergreenWireStream

Port = data.Port

NodeFragment = data.NodeFragment
NodeMap = node_map.NodeMap

Service = eg_service.Service
Session = eg_session.Session
SessionMessage = data.SessionMessage

StreamToSessionConnection = eg_service.StreamToSessionConnection

is_null_chunk = utils.is_null_chunk
wrap_pybind_object = utils.wrap_pybind_object

get_global_eglt_settings = global_settings.get_global_eglt_settings
