import { BaseEvergreenStream } from './stream';
import { v4 as uuidv4 } from 'uuid';
import { Channel, CondVar, Mutex } from './utils';
import { decodeSessionMessage, encodeSessionMessage } from './msgpack';

const kRtcConfig: RTCConfiguration = {
  iceServers: [
    {
      urls: 'stun:stun.l.google.com:19302', // change to your STUN server
    },
  ],
};

const setupDataChannel = (
  channel: RTCDataChannel,
  stream: WebRtcEvergreenStream,
  streamChannel: Channel<SessionMessage | null>,
  socket?: WebSocket,
) => {
  channel.onopen = async () => {
    console.log(`DataChannel open`);
    if (socket !== undefined) {
      socket.close();
    }
    await stream.signalReady();
  };
  channel.onclose = async () => {
    console.log(`DataChannel closed`);
    await stream.close();
  };
  channel.onmessage = async (e) => {
    // console.log(e.data);
    const message = await decodeSessionMessage(e.data as unknown as Blob);
    // console.log(`Received message:`, message);

    await streamChannel.sendNowait(message);
  };
};

async function sendLocalDescription(
  ws: WebSocket,
  id: string,
  connection: RTCPeerConnection,
  type: string,
) {
  let description: RTCSessionDescriptionInit;
  if (type === 'offer') {
    description = await connection.createOffer();
  } else {
    description = await connection.createAnswer();
  }

  await connection.setLocalDescription(description);

  const { sdp, type: descType } = connection.localDescription;
  ws.send(
    JSON.stringify({
      id,
      type: descType,
      description: sdp,
    }),
  );
}

const openSignaling = async (url: string, connection: RTCPeerConnection) => {
  const ws = new WebSocket(url);

  let resolveOpen: (ws: WebSocket) => void;
  let rejectOpen: (err: Error) => void;
  const promise = new Promise<WebSocket>((resolve, reject) => {
    resolveOpen = resolve;
    rejectOpen = reject;
  });

  ws.onopen = () => {
    console.log('WebSocket opened');
    resolveOpen(ws);
  };

  ws.onerror = (e) => {
    console.error('WebSocket error:', e);
    rejectOpen(new Error('WebSocket error'));
  };

  ws.onclose = () => {
    console.error('WebSocket disconnected');
    // Optionally handle reconnection logic here
  };

  ws.onmessage = async (e) => {
    if (typeof e.data != 'string') {
      console.warn('Received non-string message:', e.data);
      return;
    }

    const message = JSON.parse(e.data);
    console.log(message);

    if (message.type === 'offer') {
      throw new Error('Unexpected offer message');
    }

    if (message.type === 'candidate') {
      if (
        connection.iceConnectionState === 'connected' ||
        connection.iceConnectionState === 'completed' ||
        connection.iceConnectionState === 'closed'
      ) {
        return;
      }
      await connection.addIceCandidate({
        candidate: message.candidate,
        sdpMid: message.mid,
      });
      return;
    }

    if (message.type !== 'answer') {
      console.warn(`Unknown message type: ${message.type}`);
      return;
    }
    if (connection.connectionState === 'closed') {
      return;
    }
    await connection.setRemoteDescription({
      sdp: message.description,
      type: message.type,
    });
  };

  return await promise;
};

export class WebRtcEvergreenStream implements BaseEvergreenStream {
  private readonly signalingUrl: string;
  private readonly identity: string;
  // private id: string

  private readonly connection: RTCPeerConnection;
  private rtcDataChannel: RTCDataChannel;
  private readonly channel: Channel<SessionMessage | null>;

  private ready: boolean;
  private readonly mutex: Mutex;
  private readonly cv: CondVar;

  constructor(
    signalingUrl: string,
    identity: string = '',
    serverId: string = 'server',
  ) {
    this.signalingUrl = signalingUrl;

    this.identity = identity || uuidv4();

    this.connection = new RTCPeerConnection(kRtcConfig);
    this.rtcDataChannel = this.connection.createDataChannel(this.identity, {
      ordered: false,
    });
    this.channel = new Channel<SessionMessage | null>();

    this.ready = false;
    this.mutex = new Mutex();
    this.cv = new CondVar();

    console.log(
      `WebRtcEvergreenStream initializing with identity: ${this.identity}`,
    );

    this._initialize(serverId).then();
  }

  async receive(): Promise<SessionMessage> {
    return this.channel.receive();
  }

  async send(message: SessionMessage): Promise<void> {
    await this.mutex.runExclusive(async () => {
      while (!this.ready) {
        await this.cv.wait(this.mutex);
      }
    });

    if (!this.rtcDataChannel || this.rtcDataChannel.readyState !== 'open') {
      throw new Error('Data channel is not open');
    }

    this.rtcDataChannel.send(encodeSessionMessage(message));
    // console.log(`Sent message: ${message}`);
  }

  async waitUntilReady(): Promise<boolean> {
    return await this.mutex.runExclusive(async () => {
      while (!this.ready && !(await this.channel.isClosed())) {
        await this.cv.wait(this.mutex);
      }
      return this.ready;
    });
  }

  async signalReady(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.ready = true;
      this.cv.notifyAll();
    });
  }

  async close(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      if (!this.ready) {
        return;
      }
      this.ready = false;

      this.rtcDataChannel.close();
      this.connection.close();

      await this.channel.send(null);
      await this.channel.close();

      this.cv.notifyAll();
    });
  }

  private async _initialize(serverId: string): Promise<void> {
    if (this.ready) {
      return;
    }

    await this.mutex.runExclusive(async () => {
      if (this.ready) {
        return;
      }

      const ws = await openSignaling(
        `${this.signalingUrl}/${this.identity}`,
        this.connection,
      );
      console.log('WebSocket connected, signaling ready');

      this.connection.onicecandidate = (e) => {
        if (e.candidate && e.candidate.candidate) {
          const { candidate, sdpMid } = e.candidate;
          ws.send(
            JSON.stringify({
              id: serverId,
              type: 'candidate',
              candidate,
              mid: sdpMid,
            }),
          );
        }
      };

      this.connection.ondatachannel = (e: RTCDataChannelEvent) => {
        this.rtcDataChannel = e.channel;
        setupDataChannel(this.rtcDataChannel, this, this.channel);
      };

      this.connection.oniceconnectionstatechange = () => {};

      this.connection.onicegatheringstatechange = () => {};

      setupDataChannel(this.rtcDataChannel, this, this.channel, ws);
      await sendLocalDescription(ws, serverId, this.connection, 'offer');

      // this.ready = true;
      // this.cv.notifyAll();
    });
  }
}
