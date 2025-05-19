import { Event, Lock } from './utils.js';

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

  private lock: Lock;
  private cv: Event;
  private nPendingOperations: number;

  constructor(id: string = '') {
    this.id = id;

    this.seqIdToArrivalOffset = new Map() as Map<number, number>;
    this.arrivalOffsetToSeqId = new Map() as Map<number, number>;

    this.chunks = new Map() as Map<number, Chunk>;

    this.finalSeqId = -1;
    this.maxSeqId = -1;
    this.totalChunksPut = 0;

    this.noFurtherPutsFlag = false;

    this.lock = new Lock();
    this.cv = new Event();
    this.nPendingOperations = 0;
  }

  async get(seqId: number, timeout?: number) {
    const shouldWait = () => !this.chunks.has(seqId) && !this.noFurtherPutsFlag;

    let chunk: Chunk | null = null;
    let continueWaiting = false;

    await this.lock.acquireAndDo(async () => {
      if (this.chunks.has(seqId)) {
        chunk = this.chunks.get(seqId);
        return;
      }
      if (this.noFurtherPutsFlag) {
        throw new Error(`No further puts allowed, cannot wait for chunk`);
      }
      continueWaiting = shouldWait();
    });

    if (!continueWaiting) {
      return chunk;
    }

    ++this.nPendingOperations;

    while (continueWaiting) {
      await this.cv.wait(timeout);
      await this.lock.acquireAndDo(async () => {
        if (this.noFurtherPutsFlag && !this.chunks.has(seqId)) {
          continueWaiting = false;
        } else if (this.chunks.has(seqId)) {
          chunk = this.chunks.get(seqId);
          continueWaiting = false;
        } else {
          continueWaiting = shouldWait();
        }
      });
    }

    --this.nPendingOperations;
    this.cv.notifyAll();

    return chunk;
  }

  async getByArrivalOrder(arrivalOffset: number, timeout?: number) {
    const shouldWait = () => {
      if (this.noFurtherPutsFlag) {
        return false;
      }
      if (!this.arrivalOffsetToSeqId.has(arrivalOffset)) {
        return true;
      }
      const seqId = this.arrivalOffsetToSeqId.get(arrivalOffset);
      return !this.chunks.has(seqId);
    };

    let chunk: Chunk | null = null;
    let continueWaiting = false;

    await this.lock.acquireAndDo(async () => {
      if (this.arrivalOffsetToSeqId.has(arrivalOffset)) {
        const seqId = this.arrivalOffsetToSeqId.get(arrivalOffset);
        chunk = this.chunks.get(seqId);
        return;
      }
      if (this.noFurtherPutsFlag) {
        throw new Error(`No further puts allowed, cannot wait for chunk`);
      }
      continueWaiting = shouldWait();
    });

    if (!continueWaiting) {
      return chunk;
    }

    ++this.nPendingOperations;

    while (continueWaiting) {
      await this.cv.wait(timeout);
      await this.lock.acquireAndDo(async () => {
        if (
          this.noFurtherPutsFlag &&
          !this.arrivalOffsetToSeqId.has(arrivalOffset)
        ) {
          continueWaiting = false;
        } else if (this.arrivalOffsetToSeqId.has(arrivalOffset)) {
          const seqId = this.arrivalOffsetToSeqId.get(arrivalOffset);
          chunk = this.chunks.get(seqId);
          continueWaiting = false;
        } else {
          continueWaiting = shouldWait();
        }
      });
    }

    --this.nPendingOperations;
    this.cv.notifyAll();

    return chunk;
  }

  async pop(seqId: number) {
    return await this.lock.acquireAndDo(async () => {
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
    await this.lock.acquireAndDo(async () => {
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
    await this.lock.acquireAndDo(async () => {
      this.noFurtherPutsFlag = true;
      if (this.maxSeqId !== -1) {
        this.finalSeqId = Math.min(this.finalSeqId, this.maxSeqId);
      }
      this.cv.notifyAll();
    });
  }

  async size() {
    return await this.lock.acquireAndDo(async () => {
      return this.chunks.size;
    });
  }

  async contains(seqId: number) {
    return await this.lock.acquireAndDo(async () => {
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
    return await this.lock.acquireAndDo(async () => {
      if (!this.arrivalOffsetToSeqId.has(arrival_offset)) {
        return -1;
      }
      return this.arrivalOffsetToSeqId.get(arrival_offset);
    });
  }

  async getFinalSeqId() {
    return await this.lock.acquireAndDo(async () => {
      return this.finalSeqId;
    });
  }
}
