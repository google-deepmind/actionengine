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

import { Chunk } from './data.js';
import { CondVar, Mutex } from './utils.js';

export interface ChunkStore {
  get(seqId: number, timeout: number): Promise<Chunk>;

  getByArrivalOrder(arrivalOffset: number, timeout: number): Promise<Chunk>;

  pop(seqId: number): Promise<Chunk | null>;

  put(seqId: number, chunk: Chunk, final: boolean): void | Promise<void>;

  noFurtherPuts(): void | Promise<void>;

  size(): number | Promise<number>;

  contains(seqId: number): boolean | Promise<boolean>;

  setId(node_id: string): void;

  getId(): string;

  getSeqForArrivalOffset(arrival_offset: number): number | Promise<number>;

  getFinalSeq(): number | Promise<number>;
}

export class LocalChunkStore implements ChunkStore {
  private id: string;

  private seqIdToArrivalOffset: Map<number, number>;
  private arrivalOffsetToSeq: Map<number, number>;

  private chunks: Map<number, Chunk>;

  private finalSeq: number;
  private maxSeq: number;
  private totalChunksPut: number;

  private noFurtherPutsFlag: boolean;

  private readonly mutex: Mutex;
  private cv: CondVar;

  constructor(id: string = '') {
    this.id = id;

    this.seqIdToArrivalOffset = new Map() as Map<number, number>;
    this.arrivalOffsetToSeq = new Map() as Map<number, number>;

    this.chunks = new Map() as Map<number, Chunk>;

    this.finalSeq = -1;
    this.maxSeq = -1;
    this.totalChunksPut = 0;

    this.noFurtherPutsFlag = false;

    this.mutex = new Mutex();
    this.cv = new CondVar();
  }

  async get(seqId: number, timeout?: number) {
    return await this.mutex.runExclusive(async () => {
      if (this.chunks.has(seqId)) {
        return this.chunks.get(seqId);
      }

      if (this.noFurtherPutsFlag) {
        throw new Error(
          `No further puts allowed, no sense to wait for chunks. Store size: ${this.chunks.size}, seqId: ${seqId}`,
        );
      }

      while (!this.chunks.has(seqId) && !this.noFurtherPutsFlag) {
        if (
          await this.cv.waitWithTimeout(
            this.mutex,
            timeout === undefined ? -1 : timeout,
          )
        ) {
          throw new Error(`Timeout waiting for chunk with seqId ${seqId}`);
        }
      }

      return this.chunks.get(seqId);
    });
  }

  async getByArrivalOrder(arrivalOffset: number, timeout?: number) {
    return await this.mutex.runExclusive(async () => {
      if (this.arrivalOffsetToSeq.has(arrivalOffset)) {
        const seqId = this.arrivalOffsetToSeq.get(arrivalOffset);
        return this.chunks.get(seqId);
      }

      if (this.noFurtherPutsFlag) {
        throw new Error(
          `No further puts allowed, no sense to wait for chunks.`,
        );
      }

      while (
        !this.arrivalOffsetToSeq.has(arrivalOffset) &&
        !this.noFurtherPutsFlag
      ) {
        if (
          await this.cv.waitWithTimeout(
            this.mutex,
            timeout === undefined ? -1 : timeout,
          )
        ) {
          throw new Error(
            `Timeout waiting for chunk with arrival offset ${arrivalOffset}`,
          );
        }
      }

      const seqId = this.arrivalOffsetToSeq.get(arrivalOffset);
      return this.chunks.get(seqId);
    });
  }

  async pop(seqId: number) {
    return await this.mutex.runExclusive(() => {
      if (!this.chunks.has(seqId)) {
        return null;
      }
      const chunk = this.chunks.get(seqId);
      this.chunks.delete(seqId);
      const arrivalOffset = this.seqIdToArrivalOffset.get(seqId);
      this.seqIdToArrivalOffset.delete(seqId);
      this.arrivalOffsetToSeq.delete(arrivalOffset);
      return chunk;
    });
  }

  async put(seqId: number, chunk: Chunk, final: boolean) {
    await this.mutex.runExclusive(() => {
      if (this.noFurtherPutsFlag) {
        throw new Error(
          `Cannot put chunk after noFurtherPuts() has been called`,
        );
      }

      this.maxSeq = Math.max(this.maxSeq, seqId);
      this.finalSeq = final ? seqId : this.finalSeq;

      this.arrivalOffsetToSeq.set(this.totalChunksPut, seqId);
      this.seqIdToArrivalOffset.set(seqId, this.totalChunksPut);
      this.chunks.set(seqId, chunk);
      ++this.totalChunksPut;

      this.cv.notifyAll();
    });
  }

  async noFurtherPuts() {
    await this.mutex.runExclusive(() => {
      this.noFurtherPutsFlag = true;
      if (this.maxSeq !== -1) {
        this.finalSeq = Math.min(this.finalSeq, this.maxSeq);
      }
      this.cv.notifyAll();
    });
  }

  async size() {
    return await this.mutex.runExclusive(() => {
      return this.chunks.size;
    });
  }

  async contains(seqId: number) {
    return await this.mutex.runExclusive(() => {
      return this.chunks.has(seqId);
    });
  }

  setId(id: string): void {
    this.id = id;
  }

  getId(): string {
    return this.id;
  }

  async getSeqForArrivalOffset(arrival_offset: number) {
    return await this.mutex.runExclusive(() => {
      if (!this.arrivalOffsetToSeq.has(arrival_offset)) {
        return -1;
      }
      return this.arrivalOffsetToSeq.get(arrival_offset);
    });
  }

  async getFinalSeq() {
    return await this.mutex.runExclusive(() => {
      return this.finalSeq;
    });
  }
}
