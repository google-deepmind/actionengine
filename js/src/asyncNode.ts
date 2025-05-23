import { ChunkStore, LocalChunkStore } from './chunkStore.js';
import { endOfStream } from './data.js';
import { ChunkStoreReader, NumberedChunk } from './chunkStoreReader.js';
import { ChunkStoreWriter } from './chunkStoreWriter.js';
import { EvergreenStream } from './stream.js';
import { Mutex } from 'async-mutex';

export class NodeMap {
  private nodes: Map<string, AsyncNode>;
  private readonly chunkStoreFactory: (() => ChunkStore) | null;
  private mutex: Mutex;

  constructor(chunkStoreFactory: (() => ChunkStore) | null = null) {
    this.nodes = new Map();
    this.chunkStoreFactory =
      chunkStoreFactory !== null
        ? chunkStoreFactory
        : () => new LocalChunkStore();
    this.mutex = new Mutex();
  }

  async getNode(id: string): Promise<AsyncNode> {
    return await this.mutex.runExclusive(async () => {
      if (!this.nodes.has(id)) {
        this.nodes.set(id, new AsyncNode(id, this.chunkStoreFactory()));
      }
      return this.nodes.get(id) as AsyncNode;
    });
  }
}

export class AsyncNode {
  private readonly chunkStore: ChunkStore;
  private defaultReader: ChunkStoreReader | null;
  private defaultWriter: ChunkStoreWriter | null;

  private writerStream: EvergreenStream | null;

  constructor(id: string = '', chunkStore: ChunkStore | null = null) {
    this.chunkStore = chunkStore || new LocalChunkStore();
    this.chunkStore.setId(id);
    this.defaultReader = null;
    this.defaultWriter = null;
    this.writerStream = null;
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const chunk = await this.next();
        return {
          value: chunk !== null ? (chunk as Chunk) : undefined,
          done: chunk === null,
        };
      },
    };
  }

  getId(): string {
    return this.chunkStore.getId();
  }

  async nextNumberedChunk(): Promise<NumberedChunk | null> {
    this.ensureReader();
    return await this.defaultReader.next();
  }

  async next(): Promise<Chunk | null> {
    this.ensureReader();
    const numberedChunk = await this.nextNumberedChunk();
    if (numberedChunk === null) {
      return null;
    }
    return numberedChunk.chunk;
  }

  async put(
    chunk: Chunk,
    seq: number = -1,
    final: boolean = false,
  ): Promise<void> {
    this.ensureWriter();
    const writtenSeq = await this.defaultWriter.put(chunk, seq, final);
    if (this.writerStream !== null) {
      await this.writerStream.send({
        nodeFragments: [
          {
            id: this.chunkStore.getId(),
            chunk,
            seq: writtenSeq,
            continued: !final,
          },
        ],
      });
    }
  }

  async finalize(): Promise<void> {
    return await this.put(endOfStream(), -1, /*final=*/ true);
  }

  async putAndFinalize(chunk: Chunk, seq: number = -1): Promise<void> {
    return await this.put(chunk, seq, /*final=*/ true);
  }

  bindWriterStream(stream: EvergreenStream | null = null) {
    this.writerStream = stream;
  }

  setReaderOptions(
    ordered: boolean = false,
    removeChunks: boolean = false,
    nChunksToBuffer: number = -1,
  ): AsyncNode {
    this.ensureReader(ordered, removeChunks, nChunksToBuffer);
    return this;
  }

  private ensureReader(
    ordered: boolean = false,
    removeChunks: boolean = false,
    nChunksToBuffer: number = -1,
  ) {
    if (this.defaultReader === null) {
      this.defaultReader = new ChunkStoreReader(
        this.chunkStore,
        ordered,
        removeChunks,
        nChunksToBuffer,
      );
    }
  }

  private ensureWriter() {
    if (this.defaultWriter === null) {
      this.defaultWriter = new ChunkStoreWriter(this.chunkStore);
    }
  }
}
