"""A Pythonic wrapper for the raw pybind11 ChunkStore bindings."""

from actionengine.actionengine_pybind11 import (
    chunk_store as chunk_store_pybind11,
)

ChunkStore = chunk_store_pybind11.ChunkStore


class LocalChunkStore(chunk_store_pybind11.LocalChunkStore):
    pass
