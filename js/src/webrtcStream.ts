import { LocalChunkStore } from './chunkStore';
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

enum WebRtcPacketType {
  PlainSessionMessage = 0,
  SessionMessageChunk = 1,
  LengthSuffixedSessionMessageChunk = 2,
}

interface WebRtcPlainSessionMessage {
  serializedMessage: Uint8Array;
  transientId: number;
}

interface WebRtcSessionMessageChunk {
  chunk: Uint8Array;
  seq: number;
  transientId: number;
}

interface WebRtcLengthSuffixedSessionMessageChunk {
  chunk: Uint8Array;
  length: number;
  seq: number;
  transientId: number;
}

type WebRtcPacket = (
  | WebRtcPlainSessionMessage
  | WebRtcSessionMessageChunk
  | WebRtcLengthSuffixedSessionMessageChunk
) & { type: WebRtcPacketType };

const parseLittleEndianNumber = (
  data: Uint8Array,
  startOffset: number,
  length: number,
): number => {
  let value = 0;
  for (let i = 0; i < length; i++) {
    value |= data[startOffset + i] << (i * 8);
  }
  return value;
};

const encodeLittleEndianNumber = (
  value: number,
  length: number,
): Uint8Array => {
  const data = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    data[i] = (value >> (i * 8)) & 0xff;
  }
  return data;
};

const parseWebRtcPacket = (data: Uint8Array): WebRtcPacket => {
  let dataEnd = data.length;

  const type = data[dataEnd - 1];
  dataEnd--;

  const transientId = parseLittleEndianNumber(data, dataEnd - 8, 8);
  dataEnd -= 8;

  if (type === WebRtcPacketType.PlainSessionMessage) {
    return {
      type: WebRtcPacketType.PlainSessionMessage,
      serializedMessage: data.slice(0, dataEnd),
      transientId,
    };
  }

  const seq = parseLittleEndianNumber(data, dataEnd - 4, 4);
  dataEnd -= 4;

  if (type === WebRtcPacketType.LengthSuffixedSessionMessageChunk) {
    const length = parseLittleEndianNumber(data, dataEnd - 4, 4);
    dataEnd -= 4;

    return {
      type: WebRtcPacketType.LengthSuffixedSessionMessageChunk,
      chunk: data.slice(0, dataEnd),
      length,
      seq,
      transientId,
    };
  }

  if (type === WebRtcPacketType.SessionMessageChunk) {
    return {
      type: WebRtcPacketType.SessionMessageChunk,
      chunk: data.slice(0, dataEnd),
      seq,
      transientId,
    };
  }

  throw new Error(`Unknown WebRTC packet type: ${type}`);
};

const concatenate = (uint8arrays: Uint8Array[]): Uint8Array => {
  const totalLength = uint8arrays.reduce(
    (total, uint8array) => total + uint8array.byteLength,
    0,
  );

  const result = new Uint8Array(totalLength);

  let offset = 0;
  uint8arrays.forEach((uint8array) => {
    result.set(uint8array, offset);
    offset += uint8array.byteLength;
  });

  return result;
};

const splitDataIntoWebRtcPackets = (
  data: Uint8Array,
  transientId: number,
  packetSize: number,
): WebRtcPacket[] => {
  const packets: WebRtcPacket[] = [];

  if (data.length <= packetSize - 9) {
    packets.push({
      type: WebRtcPacketType.PlainSessionMessage,
      serializedMessage: data,
      transientId,
    });
    return packets;
  }

  if (packetSize < 18) {
    throw new Error('Packet size must be at least 18 bytes');
  }

  const firstPacketSize = packetSize - 17; // 1 byte for type, 8 bytes for transientId, 4 bytes for seq, 4 bytes for length
  packets.push({
    type: WebRtcPacketType.LengthSuffixedSessionMessageChunk,
    chunk: data.slice(0, firstPacketSize),
    length: 0, // this will be set later
    seq: 0,
    transientId,
  });

  let seq = 1;
  let offset = firstPacketSize;

  while (offset < data.length) {
    const remainingSize = data.length - offset;
    const packetLength = Math.min(remainingSize, packetSize - 13); // 1 byte for type, 8 bytes for transientId, 4 bytes for seq

    packets.push({
      type: WebRtcPacketType.SessionMessageChunk,
      chunk: data.slice(offset, offset + packetLength),
      seq,
      transientId,
    });

    offset += packetLength;
    seq++;
  }

  // Update the length of the first packet
  (packets[0] as WebRtcLengthSuffixedSessionMessageChunk).length =
    packets.length;

  return packets;
};

const serializeWebRtcPacket = (packet: WebRtcPacket): Uint8Array => {
  let transientId = 0;
  let typeByte = 0;

  if (packet.type == WebRtcPacketType.PlainSessionMessage) {
    typeByte = WebRtcPacketType.PlainSessionMessage;
    transientId = (packet as WebRtcPlainSessionMessage).transientId;
    return new Uint8Array([
      ...(packet as WebRtcPlainSessionMessage).serializedMessage,
      ...encodeLittleEndianNumber(transientId, 8),
      typeByte,
    ]);
  }

  if (packet.type == WebRtcPacketType.SessionMessageChunk) {
    typeByte = WebRtcPacketType.SessionMessageChunk;
    transientId = (packet as WebRtcSessionMessageChunk).transientId;
    const seq = (packet as WebRtcSessionMessageChunk).seq;
    return new Uint8Array([
      ...(packet as WebRtcSessionMessageChunk).chunk,
      ...encodeLittleEndianNumber(seq, 4),
      ...encodeLittleEndianNumber(transientId, 8),
      typeByte,
    ]);
  }

  if (packet.type == WebRtcPacketType.LengthSuffixedSessionMessageChunk) {
    typeByte = WebRtcPacketType.LengthSuffixedSessionMessageChunk;
    const chunk = packet as WebRtcLengthSuffixedSessionMessageChunk;
    transientId = chunk.transientId;
    const seq = chunk.seq;
    const length = chunk.length;
    return new Uint8Array([
      ...(chunk.chunk as Uint8Array),
      ...encodeLittleEndianNumber(length, 4),
      ...encodeLittleEndianNumber(seq, 4),
      ...encodeLittleEndianNumber(transientId, 8),
      typeByte,
    ]);
  }

  throw new Error(`Unsupported packet type: ${packet.type}`);
};

class ChunkedMessage {
  private readonly chunkStore: LocalChunkStore;
  private totalMessageSize: number;
  private totalChunksExpected: number;

  constructor() {
    this.chunkStore = new LocalChunkStore();
    this.totalMessageSize = 0;
    this.totalChunksExpected = -1;
  }

  async feedPacket(packet: WebRtcPacket): Promise<boolean> {
    if (
      (await this.chunkStore.size()) >= this.totalChunksExpected &&
      this.totalChunksExpected !== -1
    ) {
      throw new Error('Chunk store is full, cannot feed more packets');
    }

    if (packet.type === WebRtcPacketType.PlainSessionMessage) {
      this.totalMessageSize += (
        packet as WebRtcPlainSessionMessage
      ).serializedMessage.length;
      this.totalChunksExpected = 1;
      await this.chunkStore.put(
        0,
        {
          data: (packet as WebRtcPlainSessionMessage).serializedMessage,
          metadata: {},
        },
        true,
      );
      return true;
    }

    if (packet.type === WebRtcPacketType.SessionMessageChunk) {
      if (this.totalChunksExpected === -1) {
        throw new Error('Received a chunk while not processing a message');
      }
      const chunk = packet as WebRtcSessionMessageChunk;
      this.totalMessageSize += chunk.chunk.length;
      await this.chunkStore.put(
        chunk.seq,
        {
          data: chunk.chunk,
          metadata: {},
        },
        chunk.seq === this.totalChunksExpected - 1,
      );
      return (await this.chunkStore.size()) === this.totalChunksExpected;
    }

    if (packet.type === WebRtcPacketType.LengthSuffixedSessionMessageChunk) {
      if (this.totalChunksExpected !== -1) {
        throw new Error(
          'Received a length-suffixed chunk while already processing a message',
        );
      }
      const chunk = packet as WebRtcLengthSuffixedSessionMessageChunk;
      this.totalMessageSize += chunk.chunk.length;
      this.totalChunksExpected = chunk.length;
      await this.chunkStore.put(
        0,
        {
          data: chunk.chunk,
          metadata: {},
        },
        chunk.seq === this.totalChunksExpected - 1,
      );
      return (await this.chunkStore.size()) === this.totalChunksExpected;
    }

    throw new Error('Not implemented');
  }

  async consume(): Promise<Uint8Array> {
    if ((await this.chunkStore.size()) < this.totalChunksExpected) {
      throw new Error(
        `Not enough chunks to consume: expected ${this.totalChunksExpected}, got ${await this.chunkStore.size()}`,
      );
    }

    const chunks: Uint8Array[] = [];
    for (let i = 0; i < this.totalChunksExpected; i++) {
      chunks.push((await this.chunkStore.get(i)).data);
    }

    return concatenate(chunks);
  }
}

const setupDataChannel = (
  channel: RTCDataChannel,
  stream: WebRtcEvergreenStream,
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
    let data: Uint8Array;
    if (e.data instanceof ArrayBuffer) {
      data = new Uint8Array(e.data);
    } else if (e.data instanceof Blob) {
      data = new Uint8Array(await e.data.arrayBuffer());
    } else if (e.data instanceof Uint8Array) {
      data = e.data;
    } else {
      console.warn('Received unknown data type:', e.data);
      return;
    }
    const packet = parseWebRtcPacket(data);
    await stream.feedWebRtcPacket(packet);
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

  private nextTransientId: number;
  private readonly chunkedMessages: Map<number, ChunkedMessage>;

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

    this.nextTransientId = 0;
    this.chunkedMessages = new Map<number, ChunkedMessage>();

    console.log(
      `WebRtcEvergreenStream initializing with identity: ${this.identity}`,
    );

    this._initialize(serverId).then();
  }

  async receive(): Promise<SessionMessage> {
    return this.channel.receive();
  }

  async feedWebRtcPacket(packet: WebRtcPacket) {
    let chunkedMessage: ChunkedMessage;
    await this.mutex.runExclusive(async () => {
      if (!this.chunkedMessages.has(packet.transientId)) {
        this.chunkedMessages.set(packet.transientId, new ChunkedMessage());
      }
      chunkedMessage = this.chunkedMessages.get(
        packet.transientId,
      ) as ChunkedMessage;
    });

    const isComplete = await chunkedMessage.feedPacket(packet);
    if (isComplete) {
      const messageData = await chunkedMessage.consume();
      const message = await decodeSessionMessage(messageData);
      await this.mutex.runExclusive(async () => {
        await this.channel.sendNowait(message);
        this.chunkedMessages.delete(packet.transientId);
      });
    }
  }

  async send(message: SessionMessage): Promise<void> {
    let currentTransientId = 0;

    await this.mutex.runExclusive(async () => {
      while (!this.ready) {
        await this.cv.wait(this.mutex);
      }
      if (!this.rtcDataChannel || this.rtcDataChannel.readyState !== 'open') {
        throw new Error('Data channel is not open');
      }
      currentTransientId = this.nextTransientId++;
    });

    const encoded = encodeSessionMessage(message);
    const packets = splitDataIntoWebRtcPackets(
      encoded,
      currentTransientId,
      this.connection.sctp.maxMessageSize,
    );

    for (const packet of packets) {
      const packetData = serializeWebRtcPacket(packet);
      this.rtcDataChannel.send(packetData);
    }
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
        setupDataChannel(this.rtcDataChannel, this);
      };

      this.connection.oniceconnectionstatechange = () => {};

      this.connection.onicegatheringstatechange = () => {};

      setupDataChannel(this.rtcDataChannel, this, ws);
      await sendLocalDescription(ws, serverId, this.connection, 'offer');

      // this.ready = true;
      // this.cv.notifyAll();
    });
  }
}
