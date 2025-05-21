import { Channel } from './utils.js';
import { ChunkStore } from './chunkStore.js';
import { isEndOfStream } from './data.js';

export interface NumberedChunk {
  seq: number;
  chunk: Chunk;
}

export class ChunkStoreReader {
  private chunkStore: ChunkStore;

  private readonly ordered: boolean;
  private readonly removeChunks: boolean;
  private readonly timeout: number;

  private prefetchLoop: Promise<void> | null;
  private buffer: Channel<NumberedChunk>;
  private totalChunksRead: number;

  constructor(
    chunkStore: ChunkStore,
    ordered: boolean = false,
    removeChunks: boolean = false,
    timeout: number = -1,
  ) {
    this.chunkStore = chunkStore;

    this.ordered = ordered;
    this.removeChunks = removeChunks;
    this.timeout = timeout;

    this.buffer = new Channel<NumberedChunk>();
    this.totalChunksRead = 0;

    this.prefetchLoop = null;
  }

  async next(): Promise<NumberedChunk | null> {
    if (this.prefetchLoop === null) {
      this.prefetchLoop = this.runPrefetchLoop();
    }

    if (await this.buffer.isClosed()) {
      return null;
    }

    const nextNumberedChunk = await this.buffer.receive();
    if (!nextNumberedChunk.chunk || isEndOfStream(nextNumberedChunk.chunk)) {
      return null;
    }

    return nextNumberedChunk;
  }

  private async nextInternal(): Promise<NumberedChunk | null> {
    const nextReadOffset = this.totalChunksRead;

    const chunk = await this.chunkStore.getByArrivalOrder(
      nextReadOffset,
      this.timeout,
    );
    if (chunk === null) {
      return null;
    }

    const seqId =
      await this.chunkStore.getSeqIdForArrivalOffset(nextReadOffset);

    if (isEndOfStream(chunk)) {
      await this.chunkStore.pop(seqId);
      return null;
    }

    return { seq: seqId, chunk };
  }

  private async runPrefetchLoop() {
    while (true) {
      const finalSeqId = await this.chunkStore.getFinalSeqId();
      if (finalSeqId >= 0 && this.totalChunksRead > finalSeqId) {
        break;
      }

      let nextChunk: Chunk | null = null;
      let nextSeqId: number = -1;
      if (this.ordered) {
        nextChunk = await this.chunkStore.get(
          this.totalChunksRead,
          this.timeout,
        );
        nextSeqId = this.totalChunksRead;
      } else {
        const nextNumberedChunk = await this.nextInternal();
        if (nextNumberedChunk !== null) {
          nextChunk = nextNumberedChunk.chunk;
          nextSeqId = nextNumberedChunk.seq;
        }
        if (nextSeqId === -1) {
          nextSeqId = 0;
        }
      }

      if (this.removeChunks && nextSeqId >= 0) {
        await this.chunkStore.pop(nextSeqId);
      }

      if (nextChunk === null) {
        break;
      }

      await this.buffer.send({ seq: nextSeqId, chunk: nextChunk });
      this.totalChunksRead++;
    }
    await this.buffer.close();
  }
}
