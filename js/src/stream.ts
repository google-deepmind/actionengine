import { v4 as uuidv4 } from 'uuid';

import { Channel, CondVar, Mutex } from './utils.js';

let kWorker: Worker | null = null;

class WSWorkerManager {
  private static instance: WSWorkerManager | null = null;

  private _openHandlers: Map<string, () => void | Promise<void>>;
  private _messageHandlers: Map<
    string,
    (msg: SessionMessage) => void | Promise<void>
  >;
  private _errorHandlers: Map<
    string,
    (message: string) => void | Promise<void>
  >;
  private _closeHandlers: Map<
    string,
    (event?: CloseEvent) => void | Promise<void>
  >;

  private constructor() {
    this._openHandlers = new Map();
    this._messageHandlers = new Map();
    this._errorHandlers = new Map();
    this._closeHandlers = new Map();

    kWorker = new Worker(new URL('./wsworker.js', import.meta.url), {
      type: 'module',
    });

    kWorker.onmessage = async (event: MessageEvent) => {
      const { socketId, type, message } = event.data;

      switch (type) {
        case 'open': {
          const openHandler = this._openHandlers.get(socketId);
          if (openHandler) {
            await openHandler();
          }
          break;
        }
        case 'message': {
          const messageHandler = this._messageHandlers.get(socketId);
          if (messageHandler) {
            await messageHandler(message);
          }
          break;
        }
        case 'error': {
          const errorHandler = this._errorHandlers.get(socketId);
          if (errorHandler) {
            await errorHandler(message);
          }
          break;
        }
        case 'close': {
          const closeHandler = this._closeHandlers.get(socketId);
          if (closeHandler) {
            await closeHandler();
          }
          break;
        }
      }
    };
  }

  public static getInstance(): WSWorkerManager {
    if (!WSWorkerManager.instance) {
      WSWorkerManager.instance = new WSWorkerManager();
    }
    return WSWorkerManager.instance;
  }

  public createSocket(
    socketId: string,
    url: string,
    onOpen?: () => void | Promise<void>,
    onMessage?: (msg: SessionMessage) => void | Promise<void>,
    onError?: (message: string) => void | Promise<void>,
    onClose?: (event?: CloseEvent) => void | Promise<void>,
  ) {
    if (this._messageHandlers.has(socketId)) {
      throw new Error(`Socket with id ${socketId} already exists`);
    }

    this._openHandlers.set(socketId, onOpen || (() => {}));
    this._messageHandlers.set(socketId, onMessage || (() => {}));
    this._errorHandlers.set(socketId, onError || (() => {}));
    this._closeHandlers.set(socketId, () => {
      onClose();
      this._openHandlers.delete(socketId);
      this._messageHandlers.delete(socketId);
      this._errorHandlers.delete(socketId);
      this._closeHandlers.delete(socketId);
    });

    const send = (message: SessionMessage) => {
      if (!this._messageHandlers.has(socketId)) {
        throw new Error(`Socket with id ${socketId} does not exist`);
      }
      kWorker.postMessage({
        command: 'send',
        socketId,
        message,
      });
    };

    const close = () => {
      if (!this._messageHandlers.has(socketId)) {
        return;
      }
      kWorker.postMessage({
        command: 'close',
        socketId,
      });
    };

    kWorker.postMessage({
      command: 'open',
      socketId,
      url,
    });
    console.log(`Creating socket with id ${socketId} for URL ${url}`);

    return {
      send,
      close,
    };
  }
}

export interface BaseActionEngineStream {
  receive(): Promise<SessionMessage>;
  send(message: SessionMessage): Promise<void>;
  close(): Promise<void>;
}

export class ActionEngineStream implements BaseActionEngineStream {
  private readonly uid: string;

  private channel: Channel<SessionMessage | null>;

  private readonly mutex: Mutex;
  private readonly cv: CondVar;

  private socket: {
    send: (message: SessionMessage) => void;
    close: () => void;
  };
  private socketOpen: boolean;
  private closed: boolean;

  constructor(url: string) {
    this.uid = uuidv4();

    this.channel = new Channel<SessionMessage | null>();

    this.socketOpen = false;
    this.closed = false;

    this.mutex = new Mutex();
    this.cv = new CondVar();

    const onopen = async () => {
      await this.mutex.runExclusive(async () => {
        console.log('socket opened');
        this.socketOpen = true;
        this.cv.notifyAll();
      });
    };

    const onerror = async (message: string) => {
      await this.mutex.runExclusive(async () => {
        if (this.closed) {
          return;
        }
        console.error('socket error:', message);
        await this.closeInternal();
      });
    };

    const onmessage = async (message: SessionMessage) => {
      await this.channel.sendNowait(message);
    };

    const onclose = async () => {
      await this.mutex.runExclusive(async () => {
        if (this.closed) {
          return;
        }
        console.log('socket closed.');
        await this.closeInternal();
      });
    };

    const wsWorkerManager = WSWorkerManager.getInstance();
    const { send, close } = wsWorkerManager.createSocket(
      this.uid,
      url,
      onopen,
      onmessage,
      onerror,
      onclose,
    );
    this.socket = {
      send: send,
      close: close,
    };
  }

  async receive(): Promise<SessionMessage> {
    return await this.channel.receive();
  }

  async send(message: SessionMessage) {
    await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Stream is closed');
      }
      while (!this.socketOpen) {
        await this.cv.wait(this.mutex);
      }
    });

    this.socket.send(message);
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
