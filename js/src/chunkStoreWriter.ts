import { ChunkStore } from './chunkStore.js';
import { Channel } from './utils.js';
import { isEndOfStream } from './data.js';

export class ChunkStoreWriter {
  private chunkStore: ChunkStore;

  private finalSeqId: number;
  private totalChunksPut: number;

  private acceptsPuts: boolean;
  private totalChunksWritten: number;

  private writerLoop: Promise<void> | null;
  private buffer: Channel<NodeFragment | null>;

  constructor(chunkStore: ChunkStore) {
    this.chunkStore = chunkStore;

    this.finalSeqId = -1;
    this.totalChunksPut = 0;

    this.acceptsPuts = true;
    this.totalChunksWritten = 0;

    this.writerLoop = null;
    this.buffer = new Channel<NodeFragment>();
  }

  async put(
    chunk: Chunk,
    seq: number = -1,
    final: boolean = false,
  ): Promise<number> {
    if (!this.acceptsPuts) {
      throw new Error('ChunkStoreWriter is closed');
    }

    if (seq !== -1 && this.finalSeqId !== -1 && seq > this.finalSeqId) {
      throw new Error(
        `Cannot put chunk with seqId ${seq} because finalSeqId is ${this.finalSeqId}`,
      );
    }

    if (isEndOfStream(chunk) && !final) {
      throw new Error('Cannot put end of stream chunk without final flag');
    }

    let writtenSeq = seq;
    if (seq === -1) {
      writtenSeq = this.totalChunksPut;
    }
    ++this.totalChunksPut;

    if (final) {
      this.finalSeqId = writtenSeq;
    }

    this.ensureWriterLoop();
    this.buffer.sendNowait({
      id: this.chunkStore.getId(),
      seq: writtenSeq,
      chunk,
      continued: !final,
    });

    return writtenSeq;
  }

  private async runWriterLoop() {
    while (true) {
      let nextFragment: NodeFragment | null = null;

      try {
        nextFragment = await this.buffer.receive();
      } catch (e) {
        console.error(e);
        break;
      }

      if (nextFragment === null) {
        break;
      }

      try {
        this.chunkStore.put(
          nextFragment.seq,
          nextFragment.chunk,
          !nextFragment.continued,
        );
      } catch (e) {
        console.error(e);
        break;
      }

      ++this.totalChunksWritten;
      if (this.finalSeqId >= 0 && this.totalChunksWritten > this.finalSeqId) {
        await this.buffer.send(null);
      }
    }
    this.acceptsPuts = false;
    await this.chunkStore.noFurtherPuts();
  }

  private ensureWriterLoop() {
    if (this.writerLoop === null) {
      this.writerLoop = this.runWriterLoop();
    }
  }
}
