from actionengine import data


def str_to_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def bytes_to_str(data: bytes) -> str:
    return data.decode("utf-8")


def register_serialisers(
    registry: data.SerializerRegistry | None = None,
):
    registry = registry or data.get_global_serializer_registry()

    registry.register_serializer("text/plain", str_to_bytes, str)
    registry.register_deserializer("text/plain", bytes_to_str, str)
