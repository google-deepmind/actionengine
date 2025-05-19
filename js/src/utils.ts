import { v4 as uuidv4 } from 'uuid';

export class Lock {
  private readonly lockId: string;

  constructor() {
    this.lockId = uuidv4();
  }

  async acquireAndDo<T>(callback: () => Promise<T>): Promise<T> {
    return await navigator.locks.request(
      this.lockId,
      { mode: 'exclusive' },
      async () => {
        return await callback();
      },
    );
  }

  async shareAndDo<T>(callback: () => Promise<T>): Promise<T> {
    return await navigator.locks.request(
      this.lockId,
      { mode: 'shared' },
      async () => {
        return await callback();
      },
    );
  }
}

export class Event {
  waiters: Set<Promise<void>>;

  isNotified: boolean;
  promise: Promise<void>;
  resolve: () => void;

  constructor() {
    this.waiters = new Set();
    this.isNotified = false;
    this.promise = new Promise((resolve) => {
      this.resolve = resolve;
    });
  }

  async wait(timeout: number = -1): Promise<void> {
    if (this.isNotified) {
      return;
    }

    const waiter = new Promise<void>((resolve, reject) => {
      if (timeout >= 0) {
        setTimeout(() => {
          reject(new Error('Timeout waiting for event'));
        }, timeout);
      }

      this.promise.then(() => {
        resolve();
      });
    });

    this.waiters.add(waiter);
    try {
      await waiter;
      this.waiters.delete(waiter);
    } catch (e) {
      this.waiters.delete(waiter);
      throw e;
    }
  }

  notifyAll() {
    this.isNotified = true;
    this.resolve();
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
  private lock: Lock;
  private waiters: Event[];
  private closed: boolean;

  constructor() {
    this.deque = new Deque<ValueType>();
    this.waiters = [];
    this.lock = new Lock();
    this.closed = false;
  }

  async send(value: ValueType) {
    await this.lock.acquireAndDo(async () => {
      this.sendNowait(value);
    });
  }

  sendNowait(value: ValueType) {
    if (this.closed) {
      throw new Error('Channel closed');
    }
    this.deque.addBack(value);
    if (this.waiters.length > 0) {
      this.waiters.shift()?.notifyAll();
    }
  }

  async receive(timeout: number = -1): Promise<ValueType> {
    let event: Event | null = null;
    let result: ValueType | null = undefined;

    await this.lock.acquireAndDo(async () => {
      if (this.deque.size > 0) {
        result = this.deque.removeFront();
      } else {
        event = new Event();
        this.waiters.push(event);
      }
    });

    if (result !== undefined) {
      return result;
    }

    // TODO: check if this is correct
    await event.wait(timeout);

    return await this.lock.acquireAndDo(async () => {
      if (this.closed && this.deque.size === 0) {
        throw new Error('Channel closed');
      }
      return this.deque.removeFront();
    });
  }

  close() {
    this.closed = true;
    for (const waiter of this.waiters) {
      waiter.notifyAll();
    }
  }

  isClosed() {
    return this.closed;
  }
}
