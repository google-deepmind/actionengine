import { Channel, CondVar } from './utils.js';
import { decodeSessionMessage, encodeSessionMessage } from './msgpack.js';
import { Mutex } from 'async-mutex';

export class EvergreenStream {
  private socket: WebSocket;
  private readonly url: string;

  private channel: Channel<SessionMessage | null>;

  private readonly mutex: Mutex;
  private readonly cv: CondVar;

  private socketOpen: boolean;
  private closed: boolean;

  constructor(url: string) {
    this.url = url;
    this.socket = new WebSocket(this.url);

    this.channel = new Channel<SessionMessage>();

    this.socketOpen = false;
    this.closed = false;

    this.mutex = new Mutex();
    this.cv = new CondVar();

    this.socket.onopen = async () => {
      await this.mutex.runExclusive(async () => {
        console.log('socket opened');
        this.socketOpen = true;
        this.cv.notifyAll();
      });
    };

    this.socket.onerror = async (event) => {
      await this.mutex.runExclusive(async () => {
        if (this.closed) {
          return;
        }
        console.error('socket error:', event);
        await this.closeInternal();
      });
    };

    this.socket.onmessage = async (event) => {
      const array = new Uint8Array(await event.data.arrayBuffer());
      await this.channel.sendNowait(decodeSessionMessage(array));
    };
  }

  async receive(): Promise<SessionMessage> {
    return await this.channel.receive();
  }

  async send(message: SessionMessage) {
    await this.mutex.runExclusive(async () => {
      while (!this.socketOpen) {
        await this.cv.wait(this.mutex);
      }
    });

    await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Stream has been closed.');
      }
    });

    this.socket.send(encodeSessionMessage(message));
  }

  private async closeInternal() {
    if (!this.mutex.isLocked()) {
      throw new Error('Mutex is not locked');
    }

    if (this.closed) {
      return;
    }

    console.log('closing stream');

    this.closed = true;
    this.socket.close();

    await this.channel.send(null);
    await this.channel.close();

    this.cv.notifyAll();
  }

  async close() {
    await this.mutex.runExclusive(async () => {
      await this.closeInternal();
    });
  }
}
