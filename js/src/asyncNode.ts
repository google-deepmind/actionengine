import { ChunkStore, LocalChunkStore } from './chunkStore.js';
import { endOfStream } from './data.js';
import { ChunkStoreReader, NumberedChunk } from './chunkStoreReader.js';
import { ChunkStoreWriter } from './chunkStoreWriter.js';
import { BaseActionEngineStream } from './stream.js';
import { Mutex } from './utils.js';

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

  private writerStream: BaseActionEngineStream | null;
  private readonly mutex: Mutex;

  constructor(id: string = '', chunkStore: ChunkStore | null = null) {
    this.chunkStore = chunkStore || new LocalChunkStore();
    this.chunkStore.setId(id);
    this.defaultReader = null;
    this.defaultWriter = null;
    this.writerStream = null;
    this.mutex = new Mutex();
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
    await this.ensureReader();
    return await this.defaultReader.next();
  }

  async next(): Promise<Chunk | null> {
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
    await this.ensureWriter();
    const writtenSeq = await this.defaultWriter.put(chunk, seq, final);
    if (this.writerStream !== null) {
      this.writerStream
        .send({
          nodeFragments: [
            {
              id: this.chunkStore.getId(),
              chunk,
              seq: writtenSeq,
              continued: !final,
            },
          ],
        })
        .then();
    }
  }

  async finalize(): Promise<void> {
    return await this.put(endOfStream(), -1, /*final=*/ true);
  }

  async putAndFinalize(chunk: Chunk, seq: number = -1): Promise<void> {
    return await this.put(chunk, seq, /*final=*/ true);
  }

  async bindWriterStream(stream: BaseActionEngineStream | null = null) {
    await this.mutex.runExclusive(async () => {
      this.writerStream = stream;
    });
  }

  setReaderOptions(
    ordered: boolean = false,
    removeChunks: boolean = false,
    nChunksToBuffer: number = -1,
  ): AsyncNode {
    this.ensureReader(ordered, removeChunks, nChunksToBuffer).then();
    return this;
  }

  private async ensureReader(
    ordered: boolean = false,
    removeChunks: boolean = false,
    nChunksToBuffer: number = -1,
  ) {
    await this.mutex.runExclusive(() => {
      if (this.defaultReader !== null) {
        return;
      }
      this.defaultReader = new ChunkStoreReader(
        this.chunkStore,
        ordered,
        removeChunks,
        nChunksToBuffer,
      );
    });
  }

  private async ensureWriter() {
    await this.mutex.runExclusive(() => {
      if (this.defaultWriter !== null) {
        return;
      }
      this.defaultWriter = new ChunkStoreWriter(this.chunkStore);
    });
  }
}
