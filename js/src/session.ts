import { EvergreenStream } from './stream.js';
import { NodeMap } from './asyncNode.js';
import { isNullChunk } from './data.js';
import { ActionRegistry, fromActionMessage } from './action.js';

export class Session {
  stream: EvergreenStream;
  nodeMap: NodeMap;
  actionRegistry: ActionRegistry;

  loop: Promise<void> | null = null;

  constructor(
    stream: EvergreenStream,
    nodeMap: NodeMap | null = null,
    actionRegistry: ActionRegistry | null = null,
  ) {
    this.stream = stream;
    this.nodeMap = nodeMap || new NodeMap();
    this.actionRegistry = actionRegistry;
    this.loop = this.run();
  }

  async dispatchMessage(message: SessionMessage) {
    for (const fragment of message.nodeFragments) {
      const node = await this.nodeMap.getNode(fragment.id);
      if (fragment.chunk !== null) {
        const isFinal =
          (!fragment.continued && fragment.seq != -1) ||
          isNullChunk(fragment.chunk);
        await node.put(fragment.chunk, fragment.seq, isFinal);
      }
    }
    for (const actionMessage of message.actions) {
      const action = fromActionMessage(
        actionMessage,
        this.actionRegistry,
        this.nodeMap,
        this.stream,
        this,
      );
      action.run();
    }
  }

  getActionRegistry(): ActionRegistry {
    return this.actionRegistry;
  }

  getNodeMap(): NodeMap {
    return this.nodeMap;
  }

  async run() {
    while (true) {
      let message: SessionMessage | null = null;

      try {
        message = await this.stream.receive();
      } catch (e) {
        console.error('error receiving message:', e);
        break;
      }

      if (message === null) {
        break;
      }
      await this.dispatchMessage(message);
    }
  }
}
