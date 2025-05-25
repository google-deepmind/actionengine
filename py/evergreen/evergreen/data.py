"""Imports data (eg_structs) from the C++ bindings."""

from typing import Any, Callable

from evergreen.evergreen_pybind11 import chunk_store as chunk_store_pybind11
from evergreen.evergreen_pybind11 import data as data_pybind11
from evergreen.evergreen import pydantic_helpers
from pydantic import BaseModel

ChunkMetadata = data_pybind11.ChunkMetadata
Chunk = data_pybind11.Chunk
NodeFragment = data_pybind11.NodeFragment
Port = data_pybind11.Port
ActionMessage = data_pybind11.ActionMessage
SessionMessage = data_pybind11.SessionMessage

ChunkStoreFactory = Callable[[], chunk_store_pybind11.ChunkStore]

SerializerRegistry = data_pybind11.SerializerRegistry
get_global_serializer_registry = data_pybind11.get_global_serializer_registry


def to_bytes(
    obj: Any,
    mimetype: str = "",
    registry: SerializerRegistry = None,
) -> bytes:
    return data_pybind11.to_bytes(obj, mimetype, registry)


def to_chunk(
    obj: Any,
    mimetype: str = "",
    registry: SerializerRegistry = None,
) -> Chunk:
    return data_pybind11.to_chunk(obj, mimetype, registry)


def from_chunk(
    chunk: Chunk,
    mimetype: str = "",
    registry: SerializerRegistry | None = None,
) -> bytes:
    return data_pybind11.from_chunk(chunk, mimetype, registry)


get_global_serializer_registry().register_serializer(
    "__BaseModel__",
    pydantic_helpers.base_model_to_bytes,
    BaseModel,
)

get_global_serializer_registry().register_deserializer(
    "__BaseModel__",
    pydantic_helpers.bytes_to_base_model,
    BaseModel,
)
