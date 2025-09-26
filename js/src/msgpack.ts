/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { encode, decode, decodeAsync, decodeMulti } from '@msgpack/msgpack';
import {
  Chunk,
  ChunkMetadata,
  NodeRef,
  NodeFragment,
  Port,
  ActionMessage,
  WireMessage,
} from './data.js';

const rawDecode = async (blob: Blob | Uint8Array) => {
  if (blob instanceof ArrayBuffer) {
    return decode(new Uint8Array(blob));
  }

  if (blob instanceof Blob) {
    if (blob.stream) {
      return await decodeAsync(blob.stream());
    } else {
      return await decode(await blob.arrayBuffer());
    }
  } else if (blob instanceof Uint8Array) {
    return decode(blob);
  }
  throw new Error(`Unsupported blob type for decoding: ${blob}`);
};

export const encodeChunkMetadata = (metadata: ChunkMetadata) => {
  const encodedMimetype = encode(metadata.mimetype || '');
  const encodedTimestamp = encode(
    metadata.timestamp !== undefined && metadata.timestamp !== null
      ? encode(1000 * metadata.timestamp.getTime())
      : encode(null),
  );

  const attributes = new Map<string, string>(
    Object.entries(metadata.attributes || {}),
  );
  const encodedAttributeParts = [];
  encodedAttributeParts.push(encode(attributes.size));
  for (const [key, value] of attributes) {
    encodedAttributeParts.push(encode(key));
    encodedAttributeParts.push(encode(value));
  }
  const encodedAttributeLength = encodedAttributeParts.reduce(
    (sum, part) => sum + part.length,
    0,
  );

  const bytes = new Uint8Array(
    encodedMimetype.length + encodedTimestamp.length + encodedAttributeLength,
  );
  let offset = 0;
  bytes.set(encodedMimetype, offset);
  offset += encodedMimetype.length;
  bytes.set(encodedTimestamp, offset);
  offset += encodedTimestamp.length;
  for (const part of encodedAttributeParts) {
    bytes.set(part, offset);
    offset += part.length;
  }
  return bytes;
};

export const decodeChunkMetadata = (bytes: Uint8Array): ChunkMetadata => {
  const [mimetype, timestamp] = decodeMulti(bytes) as unknown as [
    string,
    number | null,
  ];
  return {
    mimetype,
    timestamp:
      timestamp !== undefined && timestamp !== null
        ? new Date(timestamp / 1000)
        : undefined,
  };
};

export const encodeChunk = (chunk: Chunk) => {
  let encodedMetadata: Uint8Array;
  if (chunk.metadata === undefined || chunk.metadata === null) {
    encodedMetadata = encode(null);
  } else {
    encodedMetadata = encode(encodeChunkMetadata(chunk.metadata));
  }
  const encodedRef = encode(chunk.ref || '');
  const encodedData = chunk.data ? encode(chunk.data) : new Uint8Array(0);
  const bytes = new Uint8Array(
    encodedMetadata.length + encodedRef.length + encodedData.length,
  );
  let offset = 0;
  bytes.set(encodedData, offset);
  offset += encodedData.length;
  bytes.set(encodedRef, offset);
  offset += encodedRef.length;
  bytes.set(encodedMetadata, offset);
  return bytes;
};

export interface BaseModelMessage {
  model: string;
  data: unknown;
}

export const decodeBaseModelChunk = (chunk: Chunk) => {
  const [model, data] = decode(chunk.data) as unknown as [string, Uint8Array];
  return { model, data: decode(data) };
};

export const encodeBaseModelMessage = (model: string, data: unknown) => {
  return encode([model, encode(data)]) as Uint8Array<ArrayBuffer>;
};

export const decodeChunk = (bytes: Uint8Array): Chunk => {
  const [data, ref, metadataBytes] = decodeMulti(bytes) as unknown as [
    Uint8Array<ArrayBuffer>,
    string,
    Uint8Array | null,
  ];
  return {
    metadata:
      metadataBytes !== null ? decodeChunkMetadata(metadataBytes) : undefined,
    ref,
    data: data || new Uint8Array(0),
  };
};

export const encodeNodeRef = (ref: NodeRef) => {
  const encodedId = encode(ref.id || '');
  const encodedOffset = encode(ref.offset || 0);
  const encodedLength = encode(ref.length || null);
  const bytes = new Uint8Array(
    encodedId.length + encodedOffset.length + encodedLength.length,
  );
  let offset = 0;
  bytes.set(encodedId, offset);
  offset += encodedId.length;
  bytes.set(encodedOffset, offset);
  offset += encodedOffset.length;
  bytes.set(encodedLength, offset);
  return bytes;
};

export const decodeNodeRef = (bytes: Uint8Array): NodeRef => {
  const [id, offset, length] = decodeMulti(bytes) as unknown as [
    string,
    number,
    number | null,
  ];
  return {
    id,
    offset,
    length: length === null ? undefined : length,
  };
};

export const hasNodeRef = (fragment: NodeFragment): boolean => {
  const maybeRef = fragment.data as NodeRef;
  return (
    maybeRef.id !== undefined &&
    maybeRef.id !== null &&
    maybeRef.offset !== undefined &&
    maybeRef.offset !== null
  );
};

export const encodeNodeFragment = (fragment: NodeFragment) => {
  let encodedData: Uint8Array;
  if (hasNodeRef(fragment)) {
    const encodedVariant = encode(1); // 1 for NodeRef
    const encodedNodeRef = encode(encodeNodeRef(fragment.data as NodeRef));
    encodedData = new Uint8Array(encodedVariant.length + encodedNodeRef.length);
    let offset = 0;
    encodedData.set(encodedVariant, offset);
    offset += encodedVariant.length;
    encodedData.set(encodedNodeRef, offset);
  } else {
    const encodedVariant = encode(0); // 0 for Chunk
    const encodedChunk = encode(encodeChunk(fragment.data as Chunk));
    encodedData = new Uint8Array(encodedVariant.length + encodedChunk.length);
    let offset = 0;
    encodedData.set(encodedVariant, offset);
    offset += encodedVariant.length;
    encodedData.set(encodedChunk, offset);
  }

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
    encodedData.length +
      encodedContinued.length +
      encodedId.length +
      encodedSeq.length,
  );
  let offset = 0;
  bytes.set(encodedData, offset);
  offset += encodedData.length;
  bytes.set(encodedContinued, offset);
  offset += encodedContinued.length;
  bytes.set(encodedId, offset);
  offset += encodedId.length;
  bytes.set(encodedSeq, offset);
  return bytes;
};

export const decodeNodeFragment = (bytes: Uint8Array): NodeFragment => {
  const [variantIndex, dataBytes, continued, id, seq] = decodeMulti(
    bytes,
  ) as unknown as [number, Uint8Array, boolean, string, number];
  let decodedData: NodeRef | Chunk;
  if (variantIndex === 0) {
    // 0 indicates a Chunk
    decodedData = decodeChunk(dataBytes);
  } else if (variantIndex === 1) {
    // 1 indicates a NodeRef
    decodedData = decodeNodeRef(dataBytes);
  } else {
    throw new Error(`Unknown data type in NodeFragment: ${variantIndex}`);
  }
  return {
    data: decodedData,
    continued:
      continued === undefined || continued === null ? false : continued,
    id,
    seq: seq === undefined || seq === null ? -1 : seq,
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
  const [name, id] = decodeMulti(bytes) as unknown as [string, string];
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
  const [id, name, inputs, outputs] = decodeMulti(bytes) as unknown as [
    string,
    string,
    Uint8Array[],
    Uint8Array[],
  ];
  return {
    id,
    name,
    inputs: inputs.map(decodePort),
    outputs: outputs.map(decodePort),
  };
};

export const encodeWireMessage = (message: WireMessage) => {
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

export const decodeWireMessage = async (
  bytes: Blob | Uint8Array,
): Promise<WireMessage> => {
  const unpackedMessage = (await rawDecode(bytes)) as Uint8Array;
  // @ts-expect-error decodeMulti is not strictly typed
  const [packedNodeFragments, packedActions]: [Uint8Array[], Uint8Array[]] =
    decodeMulti(unpackedMessage);

  const nodeFragments = packedNodeFragments.map(decodeNodeFragment);
  const actions = packedActions.map(decodeActionMessage);

  return { nodeFragments, actions };
};
