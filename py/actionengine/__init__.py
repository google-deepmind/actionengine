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
from actionengine import _C

Action = actions.Action
ActionSchema = actions.ActionSchema
ActionMessage = data.ActionMessage
ActionRegistry = actions.ActionRegistry

AsyncNode = async_node.AsyncNode

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata
ChunkStoreFactory = chunk_store.ChunkStoreFactory

WireStreamAdapter = eg_stream.WireStreamAdapter
InProcessWireStream = eg_stream.InProcessWireStream

Port = data.Port

NodeFragment = data.NodeFragment
NodeMap = node_map.NodeMap

Service = eg_service.Service
Session = eg_session.Session
WireMessage = data.WireMessage

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
    return _C.run_threadsafe_if_coroutine(function_call_result, loop)
