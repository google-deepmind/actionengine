"""Imports data (eg_structs) from the C++ bindings."""

from typing import Callable

from evergreen.evergreen_pybind11 import chunk_store as chunk_store_pybind11
from evergreen.evergreen_pybind11 import data as data_pybind11

ChunkMetadata = data_pybind11.ChunkMetadata
Chunk = data_pybind11.Chunk
NodeFragment = data_pybind11.NodeFragment
Port = data_pybind11.Port
ActionMessage = data_pybind11.ActionMessage
SessionMessage = data_pybind11.SessionMessage

ChunkStoreFactory = Callable[[], chunk_store_pybind11.ChunkStore]

get_global_serializer_registry = data_pybind11.get_global_serializer_registry
