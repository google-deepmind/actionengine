from typing import Any

import umsgpack
from evergreen.evergreen import types

ChunkMetadata = types.ChunkMetadata
Chunk = types.Chunk
NodeFragment = types.NodeFragment
NamedParameter = types.NamedParameter
ActionMessage = types.ActionMessage
SessionMessage = types.SessionMessage

WellKnownType = (
    ChunkMetadata
    | Chunk
    | NodeFragment
    | NamedParameter
    | ActionMessage
    | SessionMessage
)


def encode_chunk_metadata(metadata: ChunkMetadata):
  return umsgpack.packb([
      metadata.mimetype,
      metadata.role,
      metadata.channel,
      metadata.environment,
      metadata.original_file_name,
  ])


def encode_chunk(chunk: Chunk | None):
  if chunk is None:
    return umsgpack.packb(None)
  return umsgpack.packb(
      [encode_chunk_metadata(chunk.metadata), chunk.data, chunk.ref]
  )


def encode_node_fragment(node_fragment: NodeFragment):
  return umsgpack.packb([
      node_fragment.id,
      encode_chunk(node_fragment.chunk),
      node_fragment.seq,
      node_fragment.continued,
      node_fragment.child_ids,
  ])


def encode_named_parameter(named_parameter: NamedParameter):
  return umsgpack.packb([named_parameter.name, named_parameter.id])


def encode_action_message(action_message: ActionMessage):
  return umsgpack.packb([
      action_message.name,
      [encode_named_parameter(np) for np in action_message.inputs],
      [encode_named_parameter(np) for np in action_message.outputs],
  ])


def encode_session_message(session_message: SessionMessage):
  return umsgpack.packb([
      [encode_node_fragment(nf) for nf in session_message.node_fragments],
      [encode_action_message(am) for am in session_message.actions],
  ])


def decode_chunk_metadata(data):
  mimetype, role, channel, environment, original_file_name = umsgpack.unpackb(
      data
  )
  return ChunkMetadata(
      mimetype=mimetype,
      role=role,
      channel=channel,
      environment=environment,
      original_file_name=original_file_name,
  )


def decode_chunk(data):
  if data is None:
    return None
  metadata, data, ref = umsgpack.unpackb(data)
  return Chunk(metadata=decode_chunk_metadata(metadata), data=data, ref=ref)


def decode_node_fragment(data):
  node_id, chunk, seq, continued, child_ids = umsgpack.unpackb(data)
  return NodeFragment(
      id=node_id,
      chunk=decode_chunk(chunk),
      seq=seq,
      continued=continued,
      child_ids=child_ids,
  )


def decode_named_parameter(data):
  name, id_ = umsgpack.unpackb(data)
  return NamedParameter(name=name, id=id_)


def decode_action_message(data):
  name, inputs, outputs = umsgpack.unpackb(data)
  return ActionMessage(
      name=name,
      inputs=[decode_named_parameter(np) for np in inputs],
      outputs=[decode_named_parameter(np) for np in outputs],
  )


def decode_session_message(data):
  node_fragments, actions = umsgpack.unpackb(data)
  return SessionMessage(
      node_fragments=[decode_node_fragment(nf) for nf in node_fragments],
      actions=[decode_action_message(am) for am in actions],
  )


WELL_KNOWN_ENCODERS = {
    ChunkMetadata: encode_chunk_metadata,
    Chunk: encode_chunk,
    NodeFragment: encode_node_fragment,
    NamedParameter: encode_named_parameter,
    ActionMessage: encode_action_message,
    SessionMessage: encode_session_message,
}

WELL_KNOWN_DECODERS = {
    ChunkMetadata: decode_chunk_metadata,
    Chunk: decode_chunk,
    NodeFragment: decode_node_fragment,
    NamedParameter: decode_named_parameter,
    ActionMessage: decode_action_message,
    SessionMessage: decode_session_message,
}


def encode_well_known(obj: WellKnownType):
  if type(obj) in WELL_KNOWN_ENCODERS:
    return WELL_KNOWN_ENCODERS[type(obj)](obj)
  return umsgpack.packb(obj)


def decode_well_known(data, obj_type: type[Any]):
  decoder = WELL_KNOWN_DECODERS.get(obj_type)
  if decoder is not None:
    return decoder(data)
  return umsgpack.unpackb(data)
