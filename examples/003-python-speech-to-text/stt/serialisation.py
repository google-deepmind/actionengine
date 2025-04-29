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


def register_stt_serialisers(
    registry: serialisation.SerialiserRegistry | None = None,
):
  registry = registry or serialisation.get_global_serialiser_registry()
  registry.register_serialiser(
      bytearray_to_bytes,
      mimetype=BYTEARRAY_MIMETYPE,
      obj_type=bytearray,
  )
  registry.register_deserialiser(
      bytes_to_bytearray,
      mimetype=BYTEARRAY_MIMETYPE,
      obj_type=bytearray,
  )
  registry.register_serialiser(
      str_to_bytes,
      mimetype="text/plain",
      obj_type=str,
  )
  registry.register_deserialiser(
      bytes_to_str,
      mimetype="text/plain",
      obj_type=str,
  )
