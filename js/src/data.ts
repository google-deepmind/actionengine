const getEmptyArray = () => {
  return new Uint8Array(0);
};
const kEmptyArray = getEmptyArray();

export const endOfStream = (): Chunk => ({
  metadata: { mimetype: 'application/octet-stream' },
  data: kEmptyArray,
});

export const isNullChunk = (chunk: Chunk): boolean =>
  chunk.metadata.mimetype === 'application/octet-stream' &&
  chunk.data.byteLength === 0 &&
  !chunk.ref;

export const isEndOfStream = (chunk: Chunk): boolean => isNullChunk(chunk);
