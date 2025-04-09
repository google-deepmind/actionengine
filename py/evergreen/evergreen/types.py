"""Imports types (eg_structs) from the C++ bindings."""

from typing import Callable

from evergreen.evergreen_pybind11 import \
  chunk_store as chunk_store_pybind11
from evergreen.evergreen_pybind11 import types as types_pybind11

ChunkMetadata = types_pybind11.ChunkMetadata
Chunk = types_pybind11.Chunk
NodeFragment = types_pybind11.NodeFragment
NamedParameter = types_pybind11.NamedParameter
ActionMessage = types_pybind11.ActionMessage
SessionMessage = types_pybind11.SessionMessage

ChunkStoreFactory = Callable[[], chunk_store_pybind11.ChunkStore]
