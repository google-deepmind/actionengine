import asyncio
from typing import Any

from actionengine import actions
from actionengine import async_node
from actionengine import chunk_store
from actionengine import global_settings
from actionengine import node_map
from actionengine import service as eg_service
from actionengine import session as eg_session
from actionengine import stream as eg_stream
from actionengine import data
from actionengine import pydantic_helpers
from actionengine import redis
from actionengine import status
from actionengine import utils
from actionengine import webrtc
from actionengine import websockets
from actionengine import actionengine_pybind11


Action = actions.Action
ActionSchema = actions.ActionSchema
ActionMessage = data.ActionMessage
ActionRegistry = actions.ActionRegistry

AsyncNode = async_node.AsyncNode

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata
ChunkStoreFactory = data.ChunkStoreFactory

WireStream = eg_stream.WireStream

Port = data.Port

NodeFragment = data.NodeFragment
NodeMap = node_map.NodeMap

Service = eg_service.Service
Session = eg_session.Session
SessionMessage = data.SessionMessage

StreamToSessionConnection = eg_service.StreamToSessionConnection

is_null_chunk = utils.is_null_chunk
wrap_pybind_object = utils.wrap_pybind_object

to_bytes = data.to_bytes
to_chunk = data.to_chunk
from_chunk = data.from_chunk

get_global_act_settings = global_settings.get_global_act_settings


def run_threadsafe_if_coroutine(
    function_call_result, loop: asyncio.AbstractEventLoop | None = None
) -> Any:
    return actionengine_pybind11.run_threadsafe_if_coroutine(
        function_call_result, loop
    )
