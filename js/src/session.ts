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

import { BaseActionEngineStream } from './stream.js';
import { NodeMap } from './asyncNode.js';
import { isNullChunk } from './data.js';
import { ActionRegistry, fromActionMessage } from './action.js';
import { Mutex } from './utils.js';

export class Session {
  stream: BaseActionEngineStream;
  nodeMap: NodeMap;
  actionRegistry: ActionRegistry;

  loop: Promise<void> | null = null;
  private _mutex: Mutex;

  constructor(
    stream: BaseActionEngineStream,
    nodeMap: NodeMap | null = null,
    actionRegistry: ActionRegistry | null = null,
  ) {
    this._mutex = new Mutex();

    this.nodeMap = nodeMap || new NodeMap();
    this.actionRegistry = actionRegistry;

    this.bindStream(stream).then();
  }

  async dispatchMessage(message: SessionMessage) {
    for (const fragment of message.nodeFragments) {
      const node = await this.nodeMap.getNode(fragment.id);
      if (fragment.chunk !== null) {
        const isFinal = !fragment.continued || isNullChunk(fragment.chunk);
        node.put(fragment.chunk, fragment.seq, isFinal).then();
      }
    }
    for (const actionMessage of message.actions) {
      await this._mutex.runExclusive(() => {
        const action = fromActionMessage(
          actionMessage,
          this.actionRegistry,
          this.nodeMap,
          this.stream,
          this,
        );
        action.run().then();
      });
    }
  }

  getActionRegistry(): ActionRegistry {
    return this.actionRegistry;
  }

  getNodeMap(): NodeMap {
    return this.nodeMap;
  }

  async bindActionRegistry(registry: ActionRegistry) {
    await this._mutex.runExclusive(() => {
      this.actionRegistry = registry;
    });
  }

  async bindStream(stream: BaseActionEngineStream) {
    // bindStream may be called in ctor, where this.stream can be undefined
    if (this.stream !== undefined) {
      await this.stream.close();
    }

    if (this.loop !== null) {
      await this.loop;
    }

    await this._mutex.runExclusive(() => {
      this.stream = stream;
      this.loop = this.run();
    });
  }

  async run() {
    while (true) {
      let message: SessionMessage | null = null;

      try {
        await this._mutex.runExclusive(async () => {
          message = await this.stream.receive();
        });
      } catch (e) {
        console.error('error receiving message:', e);
        break;
      }

      if (message === null) {
        break;
      }
      this.dispatchMessage(message).then();
    }
  }
}
