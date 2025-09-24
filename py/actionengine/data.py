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

"""Imports data (types) from the C++ bindings."""

import io
import threading
from typing import Any, Callable

from actionengine import _C
from actionengine import status
from actionengine import pydantic_helpers
from PIL import Image
from pydantic import BaseModel

# Simply reexport "data" classes
ChunkMetadata = _C.data.ChunkMetadata
Chunk = _C.data.Chunk
NodeFragment = _C.data.NodeFragment
Port = _C.data.Port
ActionMessage = _C.data.ActionMessage
WireMessage = _C.data.WireMessage

get_global_serializer_registry = _C.data.get_global_serializer_registry


class SerializerRegistry(_C.data.SerializerRegistry):
    pass


def to_bytes(
    obj: Any,
    mimetype: str = "",
    registry: SerializerRegistry = None,
) -> bytes:
    return _C.data.to_bytes(obj, mimetype, registry)


def to_chunk(
    obj: Any,
    mimetype: str = "",
    registry: SerializerRegistry = None,
) -> Chunk:
    if isinstance(obj, NodeFragment) and mimetype in (
        "",
        "__act:NodeFragment__",
    ):
        return _C.data.to_chunk(obj)
    if isinstance(obj, status.Status) and mimetype in (
        "",
        "__status__",
    ):
        return _C.data.to_chunk(obj)
    return _C.data.to_chunk(obj, mimetype, registry)


def from_chunk(
    chunk: Chunk,
    mimetype: str = "",
    registry: SerializerRegistry | None = None,
) -> bytes:
    return _C.data.from_chunk(chunk, mimetype, registry)


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
        image = Image.open(input_stream)
        image.load()  # Make sure the image is loaded before the stream is closed
        return image


_DEFAULT_SERIALIZERS_REGISTERED = False


def _register_default_serializers():
    global _DEFAULT_SERIALIZERS_REGISTERED

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

    _DEFAULT_SERIALIZERS_REGISTERED = True


_LOCK = threading.Lock()
with _LOCK:
    if not _DEFAULT_SERIALIZERS_REGISTERED:
        _register_default_serializers()
