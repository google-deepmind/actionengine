import { Channel, Mutex } from './utils.js';
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

  private mu: Mutex;

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
    this.mu = new Mutex();
  }

  async next(): Promise<NumberedChunk | null> {
    return await this.mu.runExclusive(async () => {
      if (this.prefetchLoop === null) {
        this.prefetchLoop = this.runPrefetchLoop();
      }

      if (await this.buffer.isClosed()) {
        return null;
      }

      let nextNumberedChunk: NumberedChunk | null = null;
      try {
        this.mu.release();
        nextNumberedChunk = await this.buffer.receive();
      } finally {
        await this.mu.acquire();
      }

      if (!nextNumberedChunk.chunk || isEndOfStream(nextNumberedChunk.chunk)) {
        return null;
      }

      return nextNumberedChunk;
    });
  }

  private async nextInternal(): Promise<NumberedChunk | null> {
    const nextReadOffset = this.totalChunksRead;

    let chunk: Chunk | null = null;
    try {
      this.mu.release();
      chunk = await this.chunkStore.getByArrivalOrder(
        nextReadOffset,
        this.timeout,
      );
    } finally {
      await this.mu.acquire();
    }
    if (chunk === null) {
      return null;
    }

    const seqId = await this.chunkStore.getSeqForArrivalOffset(nextReadOffset);

    if (isEndOfStream(chunk)) {
      await this.chunkStore.pop(seqId);
      return null;
    }

    return { seq: seqId, chunk };
  }

  private async runPrefetchLoop() {
    return await this.mu.runExclusive(async () => {
      while (true) {
        let finalSeq = await this.chunkStore.getFinalSeq();
        if (finalSeq >= 0 && this.totalChunksRead > finalSeq) {
          break;
        }

        let nextChunk: Chunk | null = null;
        let nextSeq: number = -1;
        if (this.ordered) {
          try {
            this.mu.release();
            nextChunk = await this.chunkStore.get(
              this.totalChunksRead,
              this.timeout,
            );
          } finally {
            await this.mu.acquire();
          }

          nextSeq = this.totalChunksRead;
        } else {
          let nextNumberedChunk: NumberedChunk | null = null;
          nextNumberedChunk = await this.nextInternal();

          if (nextNumberedChunk !== null) {
            nextChunk = nextNumberedChunk.chunk;
            nextSeq = nextNumberedChunk.seq;
          }
          if (nextSeq === -1) {
            nextSeq = 0;
          }
        }

        if (nextChunk !== null) {
          await this.buffer.send({ seq: nextSeq, chunk: nextChunk });
          this.totalChunksRead++;
        }

        if (this.removeChunks && nextSeq >= 0) {
          await this.chunkStore.pop(nextSeq);
        }

        finalSeq = await this.chunkStore.getFinalSeq();
        if (finalSeq >= 0 && this.totalChunksRead > finalSeq) {
          break;
        }
      }
      await this.buffer.close();
    });
  }
}
