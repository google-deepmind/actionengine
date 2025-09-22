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

declare interface ChunkMetadata {
  readonly mimetype: string;
  readonly timestamp?: Date;
  readonly attributes?: { [key: string]: string };
}

declare interface DataChunk {
  readonly metadata?: ChunkMetadata;
  readonly data: Uint8Array<ArrayBuffer>;
  readonly ref?: string;
}

declare interface RefChunk {
  readonly metadata?: ChunkMetadata;
  readonly ref: string;
  readonly data?: Uint8Array<ArrayBuffer>;
}

declare type Chunk = DataChunk | RefChunk;

declare interface NodeRef {
  readonly id: string;
  readonly offset: number;
  readonly length?: number;
}

declare interface NodeFragment {
  readonly id: string;
  readonly data: Chunk | NodeRef;
  readonly seq: number;
  readonly continued: boolean;
}

declare interface Port {
  name: string;
  id: string;
}

declare interface ActionMessage {
  id: string;
  name: string;
  inputs: Port[];
  outputs: Port[];
}

declare interface WireMessage {
  nodeFragments?: NodeFragment[];
  actions?: ActionMessage[];
}
