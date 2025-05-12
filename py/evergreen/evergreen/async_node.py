"""A Pythonic wrapper for the raw pybind11 AsyncNode bindings."""

import asyncio
import contextlib
import functools
from collections.abc import Awaitable
from typing import Any

import evergreen.evergreen_pybind11 as evergreen_pybind11
from evergreen.evergreen import global_settings
from evergreen.evergreen import serialisation
from evergreen.evergreen import types
from evergreen.evergreen import utils
from evergreen.evergreen_pybind11 import \
  chunk_store as chunk_store_pybind11

Chunk = types.Chunk
ChunkMetadata = types.ChunkMetadata
NodeFragment = types.NodeFragment
ChunkStoreFactory = types.ChunkStoreFactory
LocalChunkStore = chunk_store_pybind11.LocalChunkStore

global_setting_if_none = global_settings.global_setting_if_none

serialise_to_chunk = serialisation.serialise_to_chunk
deserialise_from_chunk = serialisation.deserialise_from_chunk


class AsyncNode(evergreen_pybind11.AsyncNode):
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
      serialiser_registry: serialisation.SerialiserRegistry | None = None,
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

    self._deserialise_automatically_preference: bool | None = None
    self._serialiser_registry = (
        serialiser_registry or serialisation.get_global_serialiser_registry()
    )
    super().__init__(node_id, chunk_store, node_map)
    self.set_reader_options()

  def _add_python_specific_attributes(self):
    """Adds Python-specific attributes to the node."""
    self._deserialise_automatically_preference: bool | None = None
    self._serialiser_registry = serialisation.get_global_serialiser_registry()

  @property
  def deserialise(self) -> bool:
    """Returns whether the node deserialises objects automatically."""
    return global_setting_if_none(
        self._deserialise_automatically_preference,
        "readers_deserialise_automatically",
    )

  async def next(self):
    if self.deserialise:
      return await self.next_object()
    else:
      return await self.next_chunk()

  def next_sync(self):
    if self.deserialise:
      return self.next_object_sync()
    else:
      return self.next_chunk_sync()

  async def next_object(self) -> Any | None:
    """Returns the next object in the store, or None if the store is empty."""
    return await asyncio.to_thread(self.next_object_sync)

  def next_object_sync(self) -> Any:
    """Returns the next object in the store, or None if the store is empty."""
    chunk = self.next_chunk_sync()
    if chunk is None:
      return None
    return deserialise_from_chunk(chunk)

  async def next_chunk(self) -> Chunk | None:
    return await asyncio.to_thread(self.next_chunk_sync)

  def next_chunk_sync(self) -> Chunk | None:
    """Returns the next chunk in the store, or None if the store is empty.

    Raises a status error if the reader is in an error state.
    """
    chunk = super().next_chunk()  # pytype: disable=attribute-error
    if chunk is None:
      self.raise_reader_error_if_any()
      return None
    return chunk

  next_chunk = functools.wraps(next_chunk_sync)(next_chunk)

  def put_fragment(
      self, fragment: NodeFragment, seq_id: int = -1
  ) -> None | Awaitable[None]:
    """Puts a fragment into the node's chunk store.

    This method will only block if the node's chunk store writer's buffer is
    full. Otherwise, it will return immediately.

    Args:
      fragment: The fragment to put.
      seq_id: The sequence id of the fragment.

    Returns:
      None if the fragment was put synchronously with no event loop, or an
      awaitable if the fragment was put asynchronously within an event loop.
    """
    try:
      asyncio.get_running_loop()
      return utils.schedule_global_task(
          asyncio.to_thread(super().put_fragment, fragment,
                            seq_id))  # pytype: disable=attribute-error
    except RuntimeError:
      super().put_fragment(fragment, seq_id)  # pytype: disable=attribute-error

  def put_chunk(
      self, chunk: Chunk, seq_id: int = -1, final: bool = False
  ) -> None | Awaitable[None]:
    """Puts a chunk into the node's chunk store.

    This method will only block if the node's chunk store writer's buffer is
    full. Otherwise, it will return immediately.

    Args:
      chunk: The chunk to put.
      seq_id: The sequence id of the chunk.
      final: Whether the chunk is final.

    Returns:
      None if the chunk was put synchronously with no event loop, or an
      awaitable if the chunk was put asynchronously within an event loop.
    """
    try:
      asyncio.get_running_loop()
      return utils.schedule_global_task(
          asyncio.to_thread(super().put_chunk, chunk, seq_id,
                            final))  # pytype: disable=attribute-error
    except RuntimeError:
      super().put_chunk(chunk, seq_id, final)  # pytype: disable=attribute-error

  def put(
      self,
      obj: Any,
      seq_id: int = -1,
      final: bool = False,
      mimetype: str | None = None,
  ) -> None | Awaitable[None]:
    """Puts an object into the node's chunk store."""

    if isinstance(obj, Chunk):
      if mimetype is not None:
        raise ValueError(
            "mimetype must not be specified when putting a Chunk object."
        )
      return self.put_chunk(obj, seq_id, final)

    if isinstance(obj, NodeFragment):
      if mimetype is not None:
        raise ValueError(
            "mimetype must not be specified when putting a NodeFragment object."
        )
      return self.put_fragment(obj, seq_id)

    chunk = serialise_to_chunk(obj, mimetype)
    return self.put_chunk(chunk, seq_id, final)

  def put_text(
      self, text: str, seq_id: int = -1, final: bool = False
  ) -> None | Awaitable[None]:
    """Puts a text/plain chunk into the node's chunk store."""
    return self.put_chunk(
        Chunk(
            metadata=ChunkMetadata(mimetype="text/plain"),
            data=text.encode("utf-8"),
        ),
        seq_id=seq_id,
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

  async def wait_for_completion(
      self, deserialise: bool | None = None
  ) -> list[Chunk]:
    """Waits for the node to complete and returns all its chunks, including children's chunks.

    Returned chunks will be in their sequence order, and groups of children's
    chunks will be returned grouped by child, but the order of groups is not
    guaranteed.

    Args:
      deserialise: Whether to deserialise the chunks.

    Returns:
      A list of node's chunks, including children's chunks.
    """
    chunks = await asyncio.to_thread(
        self.wait_for_completion_sync, deserialise=False
    )
    if deserialise is None:
      deserialise = self.deserialise

    if deserialise:
      tasks = [
          asyncio.to_thread(deserialise_from_chunk, chunk) for chunk in chunks
      ]
      return await asyncio.gather(*tasks)
    return chunks

  def wait_for_completion_sync(
      self, deserialise: bool | None = None
  ) -> list[Chunk]:
    """Waits for the node to complete and returns all its chunks, including children's chunks.

    Returned chunks will be in their sequence order, and groups of children's
    chunks will be returned grouped by child, but the order of groups is not
    guaranteed.

    Args:
      deserialise: Whether to deserialise the chunks.

    Returns:
      A list of node's chunks, including children's chunks.
    """
    deserialise = self.deserialise
    chunks = super().wait_for_completion()  # pytype: disable=attribute-error
    if deserialise:
      return [deserialise_from_chunk(chunk) for chunk in chunks]
    return chunks

  def set_reader_options(
      self,
      ordered: bool | None = None,
      remove_chunks: bool | None = None,
      n_chunks_to_buffer: int | None = None,
  ) -> "AsyncNode":
    """Sets the options for the default reader on the node.

    Args:
      ordered: Whether to read chunks in order.
      remove_chunks: Whether to remove chunks from the store after reading them.
      n_chunks_to_buffer: The number of chunks to buffer until blocking the
        internal ChunkStoreReader.

    Returns:
      The node itself.
    """

    super().set_reader_options(  # pytype: disable=attribute-error
        global_setting_if_none(ordered, "readers_read_in_order"),
        global_setting_if_none(remove_chunks, "readers_remove_read_chunks"),
        global_setting_if_none(n_chunks_to_buffer, "readers_buffer_size"),
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
  def deserialise_automatically(self):
    """Returns the node which automatically deserialises objects."""
    self._deserialise_automatically = True
    try:
      yield self
    finally:
      self._deserialise_automatically = None

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

    if self.deserialise:
      return await asyncio.to_thread(deserialise_from_chunk, chunk)

    return chunk

  def __next__(self):
    chunk = self.next_chunk_sync()
    while chunk is not None and utils.is_null_chunk(chunk):
      chunk = self.next_chunk_sync()

    if chunk is None:
      raise StopIteration()

    if self.deserialise:
      return deserialise_from_chunk(chunk)

    return chunk
