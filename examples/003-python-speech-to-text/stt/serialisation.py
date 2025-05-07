from evergreen import serialisation

BOOL_MIMETYPE = "x-eglt;bool"
BYTEARRAY_MIMETYPE = "x-eglt;bytearray"


def bool_to_bytes(value: bool) -> bytes:
  return bytes([1 if value else 0])


def bytes_to_bool(data: bytes) -> bool:
  if len(data) != 1:
    raise ValueError("Invalid length for boolean deserialisation")
  return bool(data[0])


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
    (bool, BOOL_MIMETYPE): bool_to_bytes,
}

_DESERIALIZERS = {
    (bytes, BYTEARRAY_MIMETYPE): bytes_to_bytearray,
    (str, "text/plain"): bytes_to_str,
    (bytes, BOOL_MIMETYPE): bytes_to_bool,
}


def register_stt_serialisers(
    registry: serialisation.SerialiserRegistry | None = None,
):
  registry = registry or serialisation.get_global_serialiser_registry()

  for (python_type, mimetype), serialiser in _SERIALIZERS.items():
    registry.register_serialiser(serialiser, mimetype, python_type)

  for (python_type, mimetype), deserialiser in _DESERIALIZERS.items():
    registry.register_deserialiser(deserialiser, mimetype, python_type)
