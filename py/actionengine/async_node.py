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

"""A Pythonic wrapper for the raw pybind11 AsyncNode bindings."""

import asyncio
import contextlib
from collections.abc import Awaitable
from typing import Any, Optional

from actionengine import _C
from actionengine import global_settings
from actionengine import data
from actionengine import utils

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata
NodeFragment = data.NodeFragment
LocalChunkStore = _C.chunk_store.LocalChunkStore

global_setting_if_none = global_settings.global_setting_if_none


class AsyncNode(_C.nodes.AsyncNode):
    """A Pythonic wrapper for the raw pybind11 AsyncNode bindings.

    AsyncNode is an accessor class that allows to access the chunks of a node
    asynchronously, namely, to read and write chunks from/to the underlying
    chunk store.
    """

    def __init__(
        self,
        node_id: str,
        chunk_store: Optional[_C.chunk_store.ChunkStore] = None,
        node_map: "Optional[_C.nodes.NodeMap]" = None,
        serializer_registry: Optional[data.SerializerRegistry] = None,
    ):
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
          serializer_registry: The serializer registry to use for the node.
        """
        if chunk_store is None:
            chunk_store = LocalChunkStore()

        self._deserialize_automatically_preference: bool | None = None
        self._serializer_registry = (
            serializer_registry or data.get_global_serializer_registry()
        )
        self._reader_options_set = False
        super().__init__(node_id, node_map, chunk_store)

    def _add_python_specific_attributes(self):
        """Adds Python-specific attributes to the node."""
        self._deserialize_automatically_preference: bool | None = None
        self._serializer_registry = data.get_global_serializer_registry()
        self._reader_options_set = False

    def _ensure_reader_options_set(self):
        """Ensures that the reader options are set."""
        if not self._reader_options_set:
            self.set_reader_options()
            self._reader_options_set = True

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
        except RuntimeError:
            return self._consume_sync(timeout)
        return asyncio.create_task(
            asyncio.to_thread(self._consume_sync, timeout)
        )

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

    async def next_object(self, timeout: float = -1.0) -> Any:
        """Returns the next object in the store, or None if the store is empty."""
        return await asyncio.create_task(
            asyncio.to_thread(self.next_object_sync, timeout)
        )

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

    async def next_chunk(self, timeout: float = -1.0) -> Optional[Chunk]:
        return await asyncio.create_task(
            asyncio.to_thread(self.next_chunk_sync, timeout)
        )

    def next_chunk_sync(self, timeout: float = -1.0) -> Optional[Chunk]:
        self._ensure_reader_options_set()
        return super().next_chunk(timeout)

    async def next_fragment(
        self, timeout: float = -1.0
    ) -> Optional[NodeFragment]:
        """Returns the next fragment in the store, or None if the store is empty."""
        return await asyncio.create_task(
            asyncio.to_thread(self.next_fragment_sync, timeout)
        )

    def next_fragment_sync(
        self, timeout: float = -1.0
    ) -> Optional[NodeFragment]:
        """Returns the next fragment in the store, or None if the store is empty."""
        self._ensure_reader_options_set()
        return super().next_fragment(timeout)

    def put_fragment(self, fragment: NodeFragment, seq: int = -1):
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
        return super().put_fragment(fragment, seq)

    def put_chunk(self, chunk: Chunk, seq: int = -1, final: bool = False):
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
        return super().put_chunk(chunk, seq, final)

    def put_and_finalize(
        self,
        obj: Any,
        seq: int = -1,
        mimetype: str | None = None,
    ) -> None | Awaitable[None]:
        return self.put(obj, seq, True, mimetype)

    def put_sync(
        self,
        obj: Any,
        seq: int = -1,
        final: bool = False,
        mimetype: str | None = None,
    ) -> None:
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

    def put(
        self,
        obj: Any,
        seq: int = -1,
        final: bool = False,
        mimetype: str | None = None,
    ) -> None | Awaitable[None]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self.put_sync(obj, seq, final, mimetype)

        return asyncio.to_thread(
            self.put_sync,
            obj,
            seq,
            final,
            mimetype,
        )

    def put_text(self, text: str, seq: int = -1, final: bool = False):
        """Puts a text/plain chunk into the node's chunk store."""
        return self.put_chunk(
            Chunk(
                metadata=ChunkMetadata(mimetype="text/plain"),
                data=text.encode("utf-8"),
            ),
            seq=seq,
            final=final,
        )

    def finalize_sync(self) -> None:
        """Finalizes the node's writer stream."""
        return self.put_chunk(
            Chunk(
                metadata=ChunkMetadata(mimetype="application/octet-stream"),
                data=b"",
            ),
            final=True,
        )

    def finalize(self) -> None | Awaitable[None]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self.finalize_sync()
        return asyncio.to_thread(self.finalize_sync)

    # pylint: disable-next=[useless-parent-delegation]
    def get_id(self) -> str:
        """Returns the id of the node."""
        return super().get_id()

    def set_reader_options(
        self,
        ordered: bool | None = None,
        remove_chunks: bool | None = None,
        n_chunks_to_buffer: int | None = None,
        timeout: float | None = None,
        start_seq_or_offset: int | None = None,
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

        super().set_reader_options(
            global_setting_if_none(ordered, "readers_read_in_order"),
            global_setting_if_none(remove_chunks, "readers_remove_read_chunks"),
            global_setting_if_none(n_chunks_to_buffer, "readers_buffer_size"),
            global_setting_if_none(timeout, "readers_timeout"),
            start_seq_or_offset or 0,
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
            print("AsyncNode.__anext__: CancelledError")
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
