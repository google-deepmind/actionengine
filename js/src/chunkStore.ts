import { CondVar } from './utils.js';
import { Mutex } from 'async-mutex';

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

  getSeqIdForArrivalOffset(arrival_offset: number): number | Promise<number>;
  getFinalSeqId(): number | Promise<number>;
}

export class LocalChunkStore implements ChunkStore {
  private id: string;

  private seqIdToArrivalOffset: Map<number, number>;
  private arrivalOffsetToSeqId: Map<number, number>;

  private chunks: Map<number, Chunk>;

  private finalSeqId: number;
  private maxSeqId: number;
  private totalChunksPut: number;

  private noFurtherPutsFlag: boolean;

  private readonly mutex: Mutex;
  private cv: CondVar;

  constructor(id: string = '') {
    this.id = id;

    this.seqIdToArrivalOffset = new Map() as Map<number, number>;
    this.arrivalOffsetToSeqId = new Map() as Map<number, number>;

    this.chunks = new Map() as Map<number, Chunk>;

    this.finalSeqId = -1;
    this.maxSeqId = -1;
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
          `No further puts allowed, no sense to wait for chunks.`,
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
        if (this.noFurtherPutsFlag) {
          throw new Error(
            `No further puts flag was set while waiting for chunk`,
          );
        }
      }

      return this.chunks.get(seqId);
    });
  }

  async getByArrivalOrder(arrivalOffset: number, timeout?: number) {
    return await this.mutex.runExclusive(async () => {
      if (this.arrivalOffsetToSeqId.has(arrivalOffset)) {
        const seqId = this.arrivalOffsetToSeqId.get(arrivalOffset);
        return this.chunks.get(seqId);
      }

      if (this.noFurtherPutsFlag) {
        throw new Error(
          `No further puts allowed, no sense to wait for chunks.`,
        );
      }

      while (
        !this.arrivalOffsetToSeqId.has(arrivalOffset) &&
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
        if (this.noFurtherPutsFlag) {
          throw new Error(
            `No further puts flag was set while waiting for chunk`,
          );
        }
      }

      const seqId = this.arrivalOffsetToSeqId.get(arrivalOffset);
      return this.chunks.get(seqId);
    });
  }

  async pop(seqId: number) {
    return await this.mutex.runExclusive(async () => {
      if (!this.chunks.has(seqId)) {
        return null;
      }
      const chunk = this.chunks.get(seqId);
      this.chunks.delete(seqId);
      const arrivalOffset = this.seqIdToArrivalOffset.get(seqId);
      this.seqIdToArrivalOffset.delete(seqId);
      this.arrivalOffsetToSeqId.delete(arrivalOffset);
      return chunk;
    });
  }

  async put(seqId: number, chunk: Chunk, final: boolean) {
    await this.mutex.runExclusive(async () => {
      if (this.noFurtherPutsFlag) {
        throw new Error(
          `Cannot put chunk after noFurtherPuts() has been called`,
        );
      }

      this.maxSeqId = Math.max(this.maxSeqId, seqId);
      this.finalSeqId = final ? seqId : this.finalSeqId;

      this.arrivalOffsetToSeqId.set(this.totalChunksPut, seqId);
      this.seqIdToArrivalOffset.set(seqId, this.totalChunksPut);
      this.chunks.set(seqId, chunk);
      ++this.totalChunksPut;

      this.cv.notifyAll();
    });
  }

  async noFurtherPuts() {
    await this.mutex.runExclusive(async () => {
      this.noFurtherPutsFlag = true;
      if (this.maxSeqId !== -1) {
        this.finalSeqId = Math.min(this.finalSeqId, this.maxSeqId);
      }
      this.cv.notifyAll();
    });
  }

  async size() {
    return await this.mutex.runExclusive(async () => {
      return this.chunks.size;
    });
  }

  async contains(seqId: number) {
    return await this.mutex.runExclusive(async () => {
      return this.chunks.has(seqId);
    });
  }

  setId(id: string): void {
    this.id = id;
  }

  getId(): string {
    return this.id;
  }

  async getSeqIdForArrivalOffset(arrival_offset: number) {
    return await this.mutex.runExclusive(async () => {
      if (!this.arrivalOffsetToSeqId.has(arrival_offset)) {
        return -1;
      }
      return this.arrivalOffsetToSeqId.get(arrival_offset);
    });
  }

  async getFinalSeqId() {
    return await this.mutex.runExclusive(async () => {
      return this.finalSeqId;
    });
  }
}
