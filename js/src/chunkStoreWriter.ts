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

import { ChunkStore } from './chunkStore.js';
import { Channel } from './utils.js';
import { Chunk, isEndOfStream, NodeFragment } from './data.js';

export class ChunkStoreWriter {
  private chunkStore: ChunkStore;

  private finalSeq: number;
  private totalChunksPut: number;

  private acceptsPuts: boolean;
  private totalChunksWritten: number;

  private writerLoop: Promise<void> | null;
  private buffer: Channel<NodeFragment | null>;

  constructor(chunkStore: ChunkStore) {
    this.chunkStore = chunkStore;

    this.finalSeq = -1;
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

    if (seq !== -1 && this.finalSeq !== -1 && seq > this.finalSeq) {
      throw new Error(
        `Cannot put chunk with seqId ${seq} because finalSeq is ${this.finalSeq}`,
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
      this.finalSeq = writtenSeq;
    }

    this.ensureWriterLoop();
    await this.buffer.send({
      id: this.chunkStore.getId(),
      seq: writtenSeq,
      data: chunk,
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
          nextFragment.data as Chunk,
          !nextFragment.continued,
        );
      } catch (e) {
        console.error(e);
        break;
      }

      ++this.totalChunksWritten;
      if (this.finalSeq >= 0 && this.totalChunksWritten > this.finalSeq) {
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
