import { decodeSessionMessage, encodeSessionMessage } from './msgpack.js';

const sockets = new Map() as Map<string, WebSocket>;

addEventListener('message', (event) => {
  const { command, socketId, url, message } = event.data;

  switch (command) {
    case 'open':
      if (sockets.has(socketId)) {
        postMessage({
          type: 'error',
          socketId,
          message: 'Socket already exists',
        });
        return;
      }
      try {
        const socket = new WebSocket(url);

        socket.onopen = () => {
          postMessage({ type: 'open', socketId });
        };

        socket.onmessage = async (wsEvent) => {
          postMessage({
            type: 'message',
            socketId,
            message: await decodeSessionMessage(wsEvent.data),
          });
        };

        socket.onerror = (wsEvent) => {
          postMessage({
            type: 'error',
            socketId,
            message: `WebSocket error: ${wsEvent}`,
          });
        };

        socket.onclose = () => {
          postMessage({ type: 'close', socketId });
          sockets.delete(socketId);
        };

        sockets.set(socketId, socket);
      } catch (err) {
        postMessage({ type: 'error', socketId, message: err.message });
      }
      break;

    case 'send':
      if (sockets.has(socketId)) {
        const encoded = encodeSessionMessage(message);
        sockets.get(socketId).send(encoded);
      } else {
        postMessage({ type: 'error', socketId, message: 'Socket not found' });
      }
      break;

    case 'close':
      if (sockets.has(socketId)) {
        sockets.get(socketId).close();
        sockets.delete(socketId);
      } else {
        postMessage({ type: 'error', socketId, message: 'Socket not found' });
      }
      break;

    default:
      postMessage({ type: 'error', message: 'Unknown command' });
      break;
  }
});
