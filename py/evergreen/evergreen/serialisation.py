from typing import Any
from typing import Callable

from evergreen.evergreen import data

Chunk = data.Chunk
ChunkMetadata = data.ChunkMetadata

Serialiser = Callable[[Any], bytes]
Deserialiser = Callable[[bytes], Any]


class SerialiserRegistry:
    """A registry of serialisers for Evergreen v2 data structures."""

    _type_to_mimetype: dict[type[Any], str]
    _mimetype_to_type: dict[str, type[Any]]
    _serialisers: dict[str, Serialiser]
    _deserialisers: dict[str, Deserialiser]

    def __init__(self):
        self._type_to_mimetype = dict()
        self._mimetype_to_type = dict()
        self._serialisers = dict()
        self._deserialisers = dict()

    def get_type_serialiser(self, obj_type: type[Any]) -> Serialiser:
        """Returns the serialiser for the given type."""
        if obj_type not in self._serialisers:
            raise ValueError(f"No serialiser for type {obj_type}")
        return self._serialisers[obj_type]

    def get_mimetype(self, obj_type: type[Any]):
        if obj_type not in self._type_to_mimetype:
            raise ValueError(f"No mimetype for type {obj_type}")
        return self._type_to_mimetype[obj_type]

    def get_mimetype_serialiser(self, mimetype: str) -> Serialiser:
        """Returns the serialiser for the given mimetype."""
        if mimetype not in self._serialisers:
            raise ValueError(f"No serialiser for mimetype {mimetype}")
        return self._serialisers[mimetype]

    def get_type_deserialiser(self, obj_type: type[Any]) -> Deserialiser:
        """Returns the deserialiser for the given type."""
        if obj_type not in self._deserialisers:
            raise ValueError(f"No deserialiser for type {obj_type}")
        return self._deserialisers[obj_type]

    def get_mimetype_deserialiser(self, mimetype: str) -> Deserialiser:
        """Returns the deserialiser for the given mimetype."""
        if mimetype not in self._deserialisers:
            raise ValueError(f"No deserialiser for mimetype {mimetype}")
        return self._deserialisers[mimetype]

    def register_serialiser(
        self,
        fn: Serialiser,
        mimetype: str,
        obj_type: type[Any] | None = None,
    ):
        """Registers a serialiser for the given mimetype."""
        if obj_type is not None:
            self._type_to_mimetype[obj_type] = mimetype
            self._mimetype_to_type[mimetype] = obj_type
        self._serialisers[mimetype] = fn

    def register_deserialiser(
        self,
        fn: Deserialiser,
        mimetype: str,
        obj_type: type[Any] | None = None,
    ):
        """Registers a deserialiser for the given mimetype."""
        if obj_type is not None:
            self._type_to_mimetype[obj_type] = mimetype
            self._mimetype_to_type[mimetype] = obj_type
        self._deserialisers[mimetype] = fn


SERIALISER_REGISTRY = SerialiserRegistry()


def get_global_serialiser_registry() -> SerialiserRegistry:
    """Returns the global serialiser registry."""
    return SERIALISER_REGISTRY


def serialise(
    obj: Any,
    mimetype: str | None = None,
    registry: SerialiserRegistry | None = None,
) -> bytes:
    """Serialises the given object."""
    registry = registry or get_global_serialiser_registry()
    if mimetype:
        serialiser = registry.get_mimetype_serialiser(mimetype)
    else:
        serialiser = registry.get_type_serialiser(type(obj))
    return serialiser(obj)


def serialise_to_chunk(
    obj: Any,
    mimetype: str | None = None,
    registry: SerialiserRegistry | None = None,
) -> Chunk:
    """Serialises the given object to a Chunk."""
    registry = registry or get_global_serialiser_registry()

    mimetype = mimetype or registry.get_mimetype(type(obj))
    data = serialise(obj, mimetype, registry)
    return Chunk(
        metadata=ChunkMetadata(mimetype=mimetype),
        data=data,
    )


def deserialise(
    data: bytes,
    mimetype: str | None = None,
    registry: SerialiserRegistry | None = None,
) -> Any:
    """Deserialises the given data."""
    registry = registry or get_global_serialiser_registry()
    if mimetype:
        deserialiser = registry.get_mimetype_deserialiser(mimetype)
    else:
        deserialiser = registry.get_type_deserialiser(type(data))
    return deserialiser(data)


def deserialise_from_chunk(
    chunk: Chunk,
    registry: SerialiserRegistry | None = None,
) -> Any:
    """Deserialises the given chunk."""
    registry = registry or get_global_serialiser_registry()
    return deserialise(chunk.data, chunk.metadata.mimetype, registry)
