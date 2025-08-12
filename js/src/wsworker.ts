/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { decodeWireMessage, encodeWireMessage } from './msgpack.js';

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
            message: await decodeWireMessage(wsEvent.data),
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
        const encoded = encodeWireMessage(message);
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
