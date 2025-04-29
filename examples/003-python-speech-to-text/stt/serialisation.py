from evergreen import serialisation

BYTEARRAY_MIMETYPE = "application/x-eglt;bytearray"


def bytearray_to_bytes(arr: bytearray) -> bytes:
  return bytes(arr)


def bytes_to_bytearray(data: bytes) -> bytearray:
  return bytearray(data)


def str_to_bytes(text: str) -> bytes:
  return text.encode("utf-8")


def bytes_to_str(data: bytes) -> str:
  return data.decode("utf-8")


_SERIALIZERS = {
    (bytearray, BYTEARRAY_MIMETYPE): bytearray_to_bytes,
    (str, "text/plain"): str_to_bytes,
}

_DESERIALIZERS = {
    (bytes, BYTEARRAY_MIMETYPE): bytes_to_bytearray,
    (str, "text/plain"): bytes_to_str,
}


def register_stt_serialisers(
    registry: serialisation.SerialiserRegistry | None = None,
):
  registry = registry or serialisation.get_global_serialiser_registry()

  for (python_type, mimetype), serialiser in _SERIALIZERS.items():
    registry.register_serialiser(serialiser, mimetype, python_type)

  for (python_type, mimetype), deserialiser in _DESERIALIZERS.items():
    registry.register_deserialiser(deserialiser, mimetype, python_type)
