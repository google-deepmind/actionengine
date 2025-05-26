import { Mutex } from 'async-mutex';

class Waiter {
  private readonly promise: Promise<void>;
  private waiters: Set<Waiter>;

  private resolveInternal: (value: void | PromiseLike<void>) => void;
  private cancelInternal: (reason?: Error) => void;
  private timeout: NodeJS.Timeout | null;
  private timedOut: boolean;

  constructor(waiters: Set<Waiter>) {
    this.waiters = waiters;
    this.promise = new Promise((resolve, reject) => {
      this.resolveInternal = resolve;
      this.cancelInternal = reject;
    });
    this.timeout = null;
    this.timedOut = false;
  }

  async wait(timeout: number = -1): Promise<boolean> {
    if (timeout >= 0) {
      this.timeout = setTimeout(() => {
        this.timedOut = true;
        this.resolveInternal();
      }, timeout);
    }

    this.waiters.add(this);
    try {
      await this.promise;
      return this.timedOut;
    } finally {
      this.waiters.delete(this);
    }
  }

  cancel() {
    this.cancelInternal(new Error('CondVar cancelled'));
  }

  notify() {
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
    this.resolveInternal();
  }
}

export class CondVar {
  private readonly waiters: Set<Waiter>;

  constructor() {
    this.waiters = new Set() as Set<Waiter>;
  }

  async wait(mutex: Mutex) {
    mutex.release();
    const waiter = new Waiter(this.waiters);
    await waiter.wait();
    await mutex.acquire();
  }

  async waitWithTimeout(mutex: Mutex, timeout: number) {
    mutex.release();
    const waiter = new Waiter(this.waiters);
    const timedOut = await waiter.wait(timeout);
    await mutex.acquire();
    return timedOut;
  }

  async waitWithDeadline(mutex: Mutex, deadline: DOMHighResTimeStamp) {
    mutex.release();
    const waiter = new Waiter(this.waiters);
    const timeout = deadline - performance.now();
    const timedOut = await waiter.wait(timeout);
    await mutex.acquire();
    return timedOut;
  }

  notifyOne() {
    const waiter: Waiter = this.waiters.values().next().value;
    if (waiter) {
      waiter.notify();
    }
  }

  notifyAll() {
    for (const waiter of this.waiters) {
      waiter.notify();
    }
  }
}

export class Deque<ValueType> {
  data: Array<ValueType>;
  front: number;
  back: number;
  size: number;

  constructor() {
    this.data = [];
    this.front = 0;
    this.back = 1;
    this.size = 0;
  }

  addFront(value: ValueType) {
    if (this.size >= Number.MAX_SAFE_INTEGER) throw 'Deque capacity overflow';
    this.size++;
    this.front = (this.front + 1) % Number.MAX_SAFE_INTEGER;
    this.data[this.front] = value;
  }

  removeFront(): ValueType | null {
    if (!this.size) {
      throw 'Deque is empty';
    }
    const value = this.peekFront();
    this.size--;
    delete this.data[this.front];
    this.front = (this.front || Number.MAX_SAFE_INTEGER) - 1;
    return value;
  }

  peekFront(): ValueType | null {
    if (this.size) {
      return this.data[this.front];
    } else {
      return null;
    }
  }

  addBack(value: ValueType) {
    if (this.size >= Number.MAX_SAFE_INTEGER) throw 'Deque capacity overflow';
    this.size++;
    this.back = (this.back || Number.MAX_SAFE_INTEGER) - 1;
    this.data[this.back] = value;
  }

  removeBack(): ValueType | null {
    if (!this.size) {
      throw 'Deque is empty';
    }
    const value = this.peekBack();
    this.size--;
    delete this.data[this.back];
    this.back = (this.back + 1) % Number.MAX_SAFE_INTEGER;
    return value;
  }

  peekBack(): ValueType | null {
    if (this.size) {
      return this.data[this.back];
    } else {
      return null;
    }
  }
}

export class Channel<ValueType> {
  private deque: Deque<ValueType>;
  private readonly mutex: Mutex;
  private cv: CondVar;
  private closed: boolean;

  constructor() {
    this.deque = new Deque<ValueType>();
    this.mutex = new Mutex();
    this.cv = new CondVar();
    this.closed = false;
  }

  async send(value: ValueType) {
    await this.sendNowait(value);
  }

  async sendNowait(value: ValueType) {
    await this.mutex.runExclusive(() => {
      if (this.closed) {
        throw new Error('Channel closed');
      }
      this.deque.addBack(value);
      this.cv.notifyAll();
    });
  }

  async receive(timeout: number = -1): Promise<ValueType> {
    return await this.mutex.runExclusive(async () => {
      while (!this.closed && this.deque.size === 0) {
        if (await this.cv.waitWithTimeout(this.mutex, timeout)) {
          throw new Error('Timeout waiting for value from channel');
        }
      }
      if (this.deque.size > 0) {
        return this.deque.removeFront();
      }

      throw new Error('Channel closed');
    });
  }

  async close() {
    await this.mutex.runExclusive(async () => {
      this.closed = true;
      this.cv.notifyAll();
    });
  }

  async isClosed() {
    return await this.mutex.runExclusive(async () => {
      return this.closed;
    });
  }
}

export const makeBlobFromChunk = (chunk: Chunk): Blob => {
  return new Blob([chunk.data], { type: chunk.metadata.mimetype });
};

export const makeChunkFromBlob = async (blob: Blob): Promise<Chunk> => {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  return { metadata: { mimetype: blob.type }, data: bytes };
};
