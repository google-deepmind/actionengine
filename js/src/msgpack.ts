import { decode, encode } from '@msgpack/msgpack';

export const encodeChunkMetadata = (metadata: ChunkMetadata) => {
  const encodedMimetype = encode(metadata.mimetype || '');
  const encodedTimestamp = encode(
    metadata.timestamp
      ? encode(1000 * metadata.timestamp.getTime())
      : encode(0),
  );
  const bytes = new Uint8Array(
    encodedMimetype.length + encodedTimestamp.length,
  );
  let offset = 0;
  bytes.set(encodedMimetype, offset);
  offset += encodedMimetype.length;
  bytes.set(encodedTimestamp, offset);
  return bytes;
};

export const decodeChunkMetadata = (bytes: Uint8Array): ChunkMetadata => {
  const [mimetype, unixMicros] = decode(bytes) as [string, number | null];

  let timestamp: Date | undefined;
  if (unixMicros) {
    timestamp = new Date(unixMicros / 1000);
  }

  return {
    mimetype: mimetype || undefined,
    timestamp,
  };
};

export const encodeChunk = (chunk: Chunk) => {
  const encodedMetadata = encode(encodeChunkMetadata(chunk.metadata));
  const encodedRef = encode(chunk.ref || '');
  const encodedData = chunk.data ? encode(chunk.data) : new Uint8Array(0);
  const bytes = new Uint8Array(
    encodedMetadata.length + encodedRef.length + encodedData.length,
  );
  let offset = 0;
  bytes.set(encodedMetadata, offset);
  offset += encodedMetadata.length;
  bytes.set(encodedRef, offset);
  offset += encodedRef.length;
  bytes.set(encodedData, offset);
  return bytes;
};

export const decodeChunk = (bytes: Uint8Array): Chunk => {
  const [metadataBytes, ref, data] = decode(bytes) as [
    Uint8Array,
    string,
    Uint8Array,
  ];
  return {
    metadata: decodeChunkMetadata(metadataBytes),
    data: ref ? undefined : data,
    ref: ref || undefined,
  };
};

export const encodeNodeFragment = (fragment: NodeFragment) => {
  const encodedChunk = fragment.chunk
    ? encode(encodeChunk(fragment.chunk))
    : encode(null);

  const encodedContinued = encode(
    fragment.continued === undefined || fragment.continued === null
      ? false
      : fragment.continued,
  );
  const encodedId = encode(fragment.id || '');
  const encodedSeq = encode(
    fragment.seq === undefined || fragment.seq === null ? -1 : fragment.seq,
  );

  const bytes = new Uint8Array(
    encodedChunk.length +
      encodedContinued.length +
      encodedId.length +
      encodedSeq.length,
  );
  let offset = 0;
  bytes.set(encodedChunk, offset);
  offset += encodedChunk.length;
  bytes.set(encodedContinued, offset);
  offset += encodedContinued.length;
  bytes.set(encodedId, offset);
  offset += encodedId.length;
  bytes.set(encodedSeq, offset);
  return bytes;
};

export const decodeNodeFragment = (bytes: Uint8Array): NodeFragment => {
  const [chunkBytes, continued, id, seq] = decode(bytes) as [
    Uint8Array | null,
    boolean,
    string,
    number,
  ];

  return {
    id,
    chunk: chunkBytes ? decodeChunk(chunkBytes) : undefined,
    seq,
    continued,
  };
};

export const encodePort = (port: Port) => {
  const encodedName = encode(port.name);
  const encodedId = encode(port.id);

  const bytes = new Uint8Array(encodedName.length + encodedId.length);
  let offset = 0;
  bytes.set(encodedName, offset);
  offset += encodedName.length;
  bytes.set(encodedId, offset);
  return bytes;
};

export const decodePort = (bytes: Uint8Array): Port => {
  const [name, id] = decode(bytes) as [string, string];
  return {
    name,
    id,
  };
};

export const encodeActionMessage = (message: ActionMessage) => {
  const encodedId = encode(message.id || '');
  const encodedName = encode(message.name || '');
  const encodedInputs = encode((message.inputs || []).map(encodePort));
  const encodedOutputs = encode((message.outputs || []).map(encodePort));
  const bytes = new Uint8Array(
    encodedId.length +
      encodedName.length +
      encodedInputs.length +
      encodedOutputs.length,
  );
  let offset = 0;
  bytes.set(encodedId, offset);
  offset += encodedId.length;
  bytes.set(encodedName, offset);
  offset += encodedName.length;
  bytes.set(encodedInputs, offset);
  offset += encodedInputs.length;
  bytes.set(encodedOutputs, offset);
  return bytes;
};

export const decodeActionMessage = (bytes: Uint8Array): ActionMessage => {
  const [id, name, inputsBytes, outputsBytes] = decode(bytes) as [
    string,
    string,
    Uint8Array[],
    Uint8Array[],
  ];
  return {
    id,
    name,
    inputs: inputsBytes.map(decodePort),
    outputs: outputsBytes.map(decodePort),
  };
};

export const encodeSessionMessage = (message: SessionMessage) => {
  const packedNodeFragments = encode(
    (message.nodeFragments || []).map(encodeNodeFragment),
  );
  const packedActions = encode(
    (message.actions || []).map(encodeActionMessage),
  );

  const packedMessage = new Uint8Array(
    packedNodeFragments.length + packedActions.length,
  );
  packedMessage.set(packedNodeFragments, 0);
  packedMessage.set(packedActions, packedNodeFragments.length);
  return encode(packedMessage);
};

export const decodeSessionMessage = (bytes: Uint8Array): SessionMessage => {
  const [nodeFragmentsBytes, actionsBytes] = decode(bytes) as [
    Uint8Array[],
    Uint8Array[],
  ];
  return {
    nodeFragments: nodeFragmentsBytes.map(decodeNodeFragment),
    actions: actionsBytes.map(decodeActionMessage),
  };
};
