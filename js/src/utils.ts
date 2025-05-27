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

  async wait(timeout: number = -1, mutex: Mutex): Promise<boolean> {
    if (timeout >= 0) {
      this.timeout = setTimeout(async () => {
        this.timedOut = true;
        this.resolveInternal();
      }, timeout);
    }

    this.waiters.add(this);
    try {
      mutex.release();
      await this.promise;
      return this.timedOut;
    } finally {
      this.waiters.delete(this);
      await mutex.acquire();
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
    const waiter = new Waiter(this.waiters);
    await waiter.wait(-1, mutex);
  }

  async waitWithTimeout(mutex: Mutex, timeout: number) {
    const waiter = new Waiter(this.waiters);
    return await waiter.wait(timeout, mutex);
  }

  async waitWithDeadline(mutex: Mutex, deadline: DOMHighResTimeStamp) {
    const timeout = deadline - performance.now();
    const waiter = new Waiter(this.waiters);
    return await waiter.wait(timeout, mutex);
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
  private readonly mutex: Mutex;

  constructor() {
    this.data = [];
    this.front = 0;
    this.back = 1;
    this.size = 0;
    this.mutex = new Mutex();
  }

  async addFront(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.size >= Number.MAX_SAFE_INTEGER) {
        throw 'Deque capacity overflow';
      }
      this.size++;
      this.front = (this.front + 1) % Number.MAX_SAFE_INTEGER;
      this.data[this.front] = value;
    });
  }

  async removeFront(): Promise<ValueType | null> {
    return await this.mutex.runExclusive(async () => {
      if (!this.size) {
        throw 'Deque is empty';
      }
      const value = this.size > 0 ? this.data[this.front] : null;
      this.size--;
      delete this.data[this.front];
      this.front = (this.front || Number.MAX_SAFE_INTEGER) - 1;
      return value;
    });
  }

  async peekFront(): Promise<ValueType | null> {
    return await this.mutex.runExclusive(async () => {
      if (this.size) {
        return this.data[this.front];
      } else {
        return null;
      }
    });
  }

  async addBack(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.size >= Number.MAX_SAFE_INTEGER) throw 'Deque capacity overflow';
      this.size++;
      this.back = (this.back || Number.MAX_SAFE_INTEGER) - 1;
      this.data[this.back] = value;
    });
  }

  async removeBack(): Promise<ValueType | null> {
    return await this.mutex.runExclusive(async () => {
      if (!this.size) {
        throw 'Deque is empty';
      }
      const value = this.size > 0 ? this.data[this.back] : null;
      this.size--;
      delete this.data[this.back];
      this.back = (this.back + 1) % Number.MAX_SAFE_INTEGER;
      return value;
    });
  }

  async peekBack(): Promise<ValueType | null> {
    return await this.mutex.runExclusive(async () => {
      if (this.size) {
        return this.data[this.back];
      } else {
        return null;
      }
    });
  }
}

export class Channel<ValueType> {
  private deque: Deque<ValueType>;
  private readonly nBufferedMessages: number;
  private readonly mutex: Mutex;
  private cv: CondVar;
  private closed: boolean;

  constructor(nBufferedMessages: number = 100) {
    this.deque = new Deque<ValueType>();
    this.nBufferedMessages = nBufferedMessages;
    this.mutex = new Mutex();
    this.cv = new CondVar();
    this.closed = false;
  }

  async send(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Channel closed');
      }
      while (!this.closed && this.deque.size >= this.nBufferedMessages) {
        await this.cv.wait(this.mutex);
      }
      if (this.closed) {
        throw new Error('Channel closed');
      }
      await this.deque.addBack(value);
      this.cv.notifyAll();
    });
  }

  async sendNowait(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Channel closed');
      }
      await this.deque.addBack(value);
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

      if (this.closed && this.deque.size === 0) {
        throw new Error('Channel closed');
      }

      return await this.deque.removeFront();
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
