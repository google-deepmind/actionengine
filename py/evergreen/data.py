"""Imports data (eg_structs) from the C++ bindings."""

import io
from typing import Any, Callable

from evergreen.evergreen_pybind11 import chunk_store as chunk_store_pybind11
from evergreen.evergreen_pybind11 import data as data_pybind11
from evergreen.pybind11_abseil import status
from evergreen import pydantic_helpers
from PIL import Image
from pydantic import BaseModel

ChunkMetadata = data_pybind11.ChunkMetadata
Chunk = data_pybind11.Chunk
NodeFragment = data_pybind11.NodeFragment
Port = data_pybind11.Port
ActionMessage = data_pybind11.ActionMessage
SessionMessage = data_pybind11.SessionMessage

ChunkStoreFactory = Callable[[str], chunk_store_pybind11.ChunkStore]

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
    if isinstance(obj, NodeFragment) and mimetype in (
        "",
        "__eglt:NodeFragment__",
    ):
        return data_pybind11.to_chunk(obj)
    if isinstance(obj, status.Status) and mimetype in (
        "",
        "__status__",
    ):
        return data_pybind11.to_chunk(obj)
    return data_pybind11.to_chunk(obj, mimetype, registry)


def from_chunk(
    chunk: Chunk,
    mimetype: str = "",
    registry: SerializerRegistry | None = None,
) -> bytes:
    return data_pybind11.from_chunk(chunk, mimetype, registry)


def bytes_to_bytes(value: bytes) -> bytes:
    """Returns the bytes as-is."""
    return value


def str_to_bytes(value: str) -> bytes:
    return value.encode("utf-8")


def bytes_to_str(value: bytes) -> str:
    return value.decode("utf-8")


def pil_image_to_png_file_bytes(image: Image.Image) -> bytes:
    with io.BytesIO() as output:
        image.save(output, format="PNG")
        return output.getvalue()


def png_file_bytes_to_pil_image(png_bytes: bytes) -> Image.Image:
    with io.BytesIO(png_bytes) as input_stream:
        return Image.open(input_stream).convert("RGB")


_SERIALIZERS_REGISTERED = False
if not _SERIALIZERS_REGISTERED:
    _SERIALIZERS_REGISTERED = True

    registry = get_global_serializer_registry()

    registry.register_serializer(
        "image/png", pil_image_to_png_file_bytes, Image.Image
    )
    registry.register_deserializer(
        "image/png", png_file_bytes_to_pil_image, Image.Image
    )

    registry.register_serializer(
        "application/octet-stream", bytes_to_bytes, bytes
    )
    registry.register_deserializer(
        "application/octet-stream", bytes_to_bytes, bytes
    )

    registry.register_serializer("text/plain", str_to_bytes, str)
    registry.register_deserializer("text/plain", bytes_to_str, str)

    registry.register_serializer(
        "__BaseModel__", pydantic_helpers.base_model_to_bytes, BaseModel
    )

    registry.register_deserializer(
        "__BaseModel__", pydantic_helpers.bytes_to_base_model, BaseModel
    )
