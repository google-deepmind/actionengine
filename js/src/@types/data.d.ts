declare interface ChunkMetadata {
  readonly mimetype?: string;
  readonly timestamp?: Date;
}

declare interface DataChunk {
  readonly metadata: ChunkMetadata;
  readonly data: Uint8Array;
  readonly ref?: string;
}

declare interface RefChunk {
  readonly metadata: ChunkMetadata;
  readonly ref: string;
  readonly data?: Uint8Array;
}

declare type Chunk = DataChunk | RefChunk;

declare interface NodeFragment {
  readonly id: string;
  readonly chunk: Chunk;
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

declare interface SessionMessage {
  nodeFragments?: NodeFragment[];
  actions?: ActionMessage[];
}
