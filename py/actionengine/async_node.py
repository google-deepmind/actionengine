"""A Pythonic wrapper for the raw pybind11 AsyncNode bindings."""

import asyncio
import contextlib
import functools
from collections.abc import Awaitable
from typing import Any

import actionengine.actionengine_pybind11 as actionengine_pybind11
from actionengine import global_settings
from actionengine import data
from actionengine import utils
from actionengine.actionengine_pybind11 import (
    chunk_store as chunk_store_pybind11,
)

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata
NodeFragment = data.NodeFragment
ChunkStoreFactory = data.ChunkStoreFactory
LocalChunkStore = chunk_store_pybind11.LocalChunkStore

global_setting_if_none = global_settings.global_setting_if_none


class AsyncNode(actionengine_pybind11.AsyncNode):
    """A Pythonic wrapper for the raw pybind11 AsyncNode bindings.

    AsyncNode is an accessor class that allows to access the chunks of a node
    asynchronously, namely, to read and write chunks from/to the underlying
    chunk store.
    """

    # pytype: disable=name-error
    def __init__(
        self,
        node_id: str,
        chunk_store: chunk_store_pybind11.ChunkStore | None = None,
        node_map: "NodeMap | None" = None,
        serializer_registry: data.SerializerRegistry | None = None,
    ):
        # pytype: enable=name-error
        """Constructor for AsyncNode.

        Makes a new AsyncNode with the given id, referencing the given chunk store
        and node map. If chunk store is not provided, a local chunk store is used.
        If node map is not provided, the node will not be able to reference other
        nodes (including its children), but will otherwise function normally.

        The node, however, will NOT be added to the node map. You will need to do
        that in an outer scope.

        Args:
          node_id: The id of the node.
          chunk_store: The chunk store to use for the node.
          node_map: The node map to use for the node.
          serialiser_registry: The serialiser registry to use for the node.
        """
        if chunk_store is None:
            chunk_store = LocalChunkStore()

        self._deserialize_automatically_preference: bool | None = None
        self._serializer_registry = (
            serializer_registry or data.get_global_serializer_registry()
        )
        super().__init__(node_id, chunk_store, node_map)
        self.set_reader_options()

    def _add_python_specific_attributes(self):
        """Adds Python-specific attributes to the node."""
        self._deserialize_automatically_preference: bool | None = None
        self._serializer_registry = data.get_global_serializer_registry()

    @property
    def deserialize(self) -> bool:
        """Returns whether the node deserializes objects automatically."""
        return global_setting_if_none(
            self._deserialize_automatically_preference,
            "readers_deserialise_automatically",
        )

    def _consume_sync(self, timeout: float = -1.0):
        item = self.next_sync(timeout)
        if item is None:
            raise RuntimeError(
                "Node is empty while expecting exactly one item."
            )
        if self.next_sync(timeout) is not None:
            raise RuntimeError(
                "Node has more than one item while expecting exactly one."
            )
        return item

    def consume(self, timeout: float = -1.0) -> Any | Awaitable[Any]:
        try:
            asyncio.get_running_loop()
            return asyncio.to_thread(self._consume_sync, timeout)
        except RuntimeError:
            return self._consume_sync(timeout)

    async def next(self, timeout: float = -1.0):
        if self.deserialize:
            return await self.next_object(timeout)
        else:
            return await self.next_chunk(timeout)

    def next_sync(self, timeout: float = -1.0):
        if self.deserialize:
            return self.next_object_sync(timeout)
        else:
            return self.next_chunk_sync(timeout)

    async def next_object(self, timeout: float = -1.0) -> Any | None:
        """Returns the next object in the store, or None if the store is empty."""
        return await asyncio.to_thread(self.next_object_sync, timeout)

    def next_object_sync(self, timeout: float = -1.0) -> Any:
        """Returns the next object in the store, or None if the store is empty."""
        chunk = self.next_chunk_sync(timeout)
        if chunk is None:
            return None
        return data.from_chunk(
            chunk,
            mimetype=chunk.metadata.mimetype,
            registry=self._serializer_registry,
        )

    async def next_chunk(self, timeout: float = -1.0) -> Chunk | None:
        return await asyncio.to_thread(self.next_chunk_sync, timeout)

    def next_chunk_sync(self, timeout: float = -1.0) -> Chunk | None:
        return super().next_chunk(timeout)  # pytype: disable=attribute-error

    next_chunk = functools.wraps(next_chunk_sync)(next_chunk)

    def put_fragment(
        self, fragment: NodeFragment, seq: int = -1
    ) -> None | Awaitable[None]:
        """Puts a fragment into the node's chunk store.

        This method will only block if the node's chunk store writer's buffer is
        full. Otherwise, it will return immediately.

        Args:
          fragment: The fragment to put.
          seq: The sequence id of the fragment.

        Returns:
          None if the fragment was put synchronously with no event loop, or an
          awaitable if the fragment was put asynchronously within an event loop.
        """
        try:
            asyncio.get_running_loop()
            return asyncio.to_thread(self.put_fragment_sync, fragment, seq)
        except RuntimeError:
            super().put_fragment(
                fragment, seq
            )  # pytype: disable=attribute-error

    def put_chunk(
        self, chunk: Chunk, seq: int = -1, final: bool = False
    ) -> None | Awaitable[None]:
        """Puts a chunk into the node's chunk store.

        This method will only block if the node's chunk store writer's buffer is
        full. Otherwise, it will return immediately.

        Args:
          chunk: The chunk to put.
          seq: The sequence id of the chunk.
          final: Whether the chunk is final.

        Returns:
          None if the chunk was put synchronously with no event loop, or an
          awaitable if the chunk was put asynchronously within an event loop.
        """
        try:
            asyncio.get_running_loop()
            return asyncio.to_thread(super().put_chunk, chunk, seq, final)
        except RuntimeError:
            super().put_chunk(
                chunk, seq, final
            )  # pytype: disable=attribute-error
            return None

    def put_and_finalize(
        self,
        obj: Any,
        seq: int = -1,
        mimetype: str | None = None,
    ) -> None | Awaitable[None]:
        return self.put(obj, seq, True, mimetype)

    def put(
        self,
        obj: Any,
        seq: int = -1,
        final: bool = False,
        mimetype: str | None = None,
    ) -> None | Awaitable[None]:
        """Puts an object into the node's chunk store."""

        if isinstance(obj, Chunk):
            if mimetype is not None:
                raise ValueError(
                    "mimetype must not be specified when putting a Chunk object."
                )
            return self.put_chunk(obj, seq, final)

        if isinstance(obj, NodeFragment):
            if mimetype is not None:
                raise ValueError(
                    "mimetype must not be specified when putting a NodeFragment object."
                )
            return self.put_fragment(obj, seq)

        chunk = data.to_chunk(
            obj,
            mimetype=mimetype or "",
            registry=self._serializer_registry,
        )
        return self.put_chunk(chunk, seq, final)

    def put_text(
        self, text: str, seq: int = -1, final: bool = False
    ) -> None | Awaitable[None]:
        """Puts a text/plain chunk into the node's chunk store."""
        return self.put_chunk(
            Chunk(
                metadata=ChunkMetadata(mimetype="text/plain"),
                data=text.encode("utf-8"),
            ),
            seq=seq,
            final=final,
        )

    def finalize(self) -> None | Awaitable[None]:
        """Finalizes the node's writer stream."""
        return self.put_chunk(
            Chunk(
                metadata=ChunkMetadata(mimetype="application/octet-stream"),
                data=b"",
            ),
            final=True,
        )

    # pylint: disable-next=[useless-parent-delegation]
    def get_id(self) -> str:
        """Returns the id of the node."""
        return super().get_id()  # pytype: disable=attribute-error

    def set_reader_options(
        self,
        ordered: bool | None = None,
        remove_chunks: bool | None = None,
        n_chunks_to_buffer: int | None = None,
        timeout: float | None = None,
    ) -> "AsyncNode":
        """Sets the options for the default reader on the node.

        Args:
          ordered: Whether to read chunks in order.
          remove_chunks: Whether to remove chunks from the store after reading them.
          n_chunks_to_buffer: The number of chunks to buffer until blocking the
            internal ChunkStoreReader.
          timeout: The timeout for reading chunks, in seconds. If None, the default
            timeout is used, which is -1.0 (no timeout).

        Returns:
          The node itself.
        """

        super().set_reader_options(  # pytype: disable=attribute-error
            global_setting_if_none(ordered, "readers_read_in_order"),
            global_setting_if_none(remove_chunks, "readers_remove_read_chunks"),
            global_setting_if_none(n_chunks_to_buffer, "readers_buffer_size"),
            global_setting_if_none(timeout, "readers_timeout"),
        )
        return self

    @contextlib.contextmanager
    def write_context(self):
        """Returns the node which automatically finalizes the writer stream."""
        try:
            yield self
        finally:
            self.finalize()

    @contextlib.contextmanager
    def deserialize_automatically(self, deserialize: bool = True):
        """Returns the node which automatically deserialises objects."""
        self._deserialize_automatically_preference = deserialize
        try:
            yield self
        finally:
            self._deserialize_automatically_preference = None

    def __aiter__(self):
        return self

    def __iter__(self):
        return self

    async def __anext__(self):
        try:
            chunk = await self.next_chunk()
            while chunk is not None and utils.is_null_chunk(chunk):
                chunk = await self.next_chunk()
        except asyncio.CancelledError:
            raise StopAsyncIteration()

        if chunk is None:
            raise StopAsyncIteration()

        if self.deserialize:
            return await asyncio.to_thread(
                data.from_chunk,
                chunk,
                "",
                self._serializer_registry,
            )

        return chunk

    def __next__(self):
        chunk = self.next_chunk_sync()
        while chunk is not None and utils.is_null_chunk(chunk):
            chunk = self.next_chunk_sync()

        if chunk is None:
            raise StopIteration()

        if self.deserialize:
            return data.from_chunk(
                chunk,
                mimetype=chunk.metadata.mimetype,
                registry=self._serializer_registry,
            )

        return chunk
