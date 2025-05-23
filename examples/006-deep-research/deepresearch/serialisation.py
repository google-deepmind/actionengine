from evergreen.evergreen import serialisation


def str_to_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def bytes_to_str(data: bytes) -> str:
    return data.decode("utf-8")


def register_serialisers(
    registry: serialisation.SerialiserRegistry | None = None,
):
    registry = registry or serialisation.get_global_serialiser_registry()

    registry.register_serialiser(str_to_bytes, "text/plain", str)
    registry.register_deserialiser(bytes_to_str, "text/plain", str)
