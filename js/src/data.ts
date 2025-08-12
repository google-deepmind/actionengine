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

export const makeTextChunk = (text: string): Chunk => {
  return {
    metadata: { mimetype: 'text/plain' },
    data: new TextEncoder().encode(text),
  };
};
