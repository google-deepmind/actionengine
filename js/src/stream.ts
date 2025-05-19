import { Channel, Event } from './utils.js';
import { decodeSessionMessage, encodeSessionMessage } from './msgpack.js';

export class EvergreenStream {
  private socket: WebSocket;
  private readonly url: string;

  private channel: Channel<SessionMessage | null>;

  private open: boolean;
  private openEvent: Event;

  constructor(url: string) {
    this.url = url;
    this.socket = new WebSocket(this.url);

    this.channel = new Channel<SessionMessage>();

    this.open = false;
    this.openEvent = new Event();

    this.socket.onopen = () => {
      console.log('socket opened');
      this.open = true;
      this.openEvent.notifyAll();
    };

    this.socket.onerror = async (event) => {
      console.error('socket error:', event);
      await this.channel.send(null);
      this.channel.close();
    };

    this.socket.onmessage = async (event) => {
      const array = new Uint8Array(await event.data.arrayBuffer());
      await this.channel.send(decodeSessionMessage(array));
    };
  }

  async receive(): Promise<SessionMessage> {
    return await this.channel.receive();
  }

  async send(message: SessionMessage) {
    if (!this.open) {
      await this.openEvent.wait();
    }

    this.socket.send(encodeSessionMessage(message));
  }

  close() {
    this.socket.close();
  }
}
