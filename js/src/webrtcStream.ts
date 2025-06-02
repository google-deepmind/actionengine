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
) => {
  channel.onopen = () => {
    console.log(`DataChannel open`);
  };
  channel.onclose = async () => {
    console.log(`DataChannel closed`);
    await stream.close();
  };
  channel.onmessage = async (e) => {
    console.log(e.data);
    const message = await decodeSessionMessage(e.data as unknown as Blob);
    console.log(`Received message:`, message);

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
    await connection.setRemoteDescription({
      sdp: message.description,
      type: message.type,
    });
  };

  return await promise;
};

export class WebRtcEvergreenStream {
  private readonly signalingUrl: string;
  private readonly uid: string;
  // private id: string

  private readonly connection: RTCPeerConnection;
  private rtcDataChannel: RTCDataChannel;
  private readonly channel: Channel<SessionMessage | null>;

  private ready: boolean;
  private readonly mutex: Mutex;
  private readonly cv: CondVar;

  constructor(signalingUrl: string, serverId: string = 'server') {
    this.signalingUrl = signalingUrl;

    this.uid = uuidv4();

    this.connection = new RTCPeerConnection(kRtcConfig);
    this.rtcDataChannel = this.connection.createDataChannel(this.uid, {
      ordered: false,
    });
    this.channel = new Channel<SessionMessage | null>();

    this.ready = false;
    this.mutex = new Mutex();
    this.cv = new CondVar();

    console.log(`WebRtcEvergreenStream initializing with uid: ${this.uid}`);

    this._initialize(serverId).then();
  }

  async receive(): Promise<SessionMessage> {
    return this.channel.receive();
  }

  async send(message: SessionMessage): Promise<void> {
    if (!this.ready) {
      await this.cv.wait(this.mutex);
    }

    if (!this.rtcDataChannel || this.rtcDataChannel.readyState !== 'open') {
      throw new Error('Data channel is not open');
    }

    this.rtcDataChannel.send(encodeSessionMessage(message));
    console.log(`Sent message: ${message}`);
  }

  async close(): Promise<void> {
    await this.mutex.runExclusive(async () => {
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

      const ws = await openSignaling(this.signalingUrl, this.connection);
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

      this.connection.oniceconnectionstatechange = () => {
        console.log(`
    Connection
    state: $
    {
      this.connection.iceConnectionState
    }
    `);
      };

      this.connection.onicegatheringstatechange = () => {
        console.log(`
    Gathering
    state: $
    {
      this.connection.iceGatheringState
    }
    `);
      };

      setupDataChannel(this.rtcDataChannel, this, this.channel);
      await sendLocalDescription(ws, serverId, this.connection, 'offer');

      this.ready = true;
      this.cv.notifyAll();
    });
  }
}
