import { AsyncNode, NodeMap } from './asyncNode.js';
import { EvergreenStream } from './stream.js';
import { Session } from './session.js';

export class ActionRegistry {
  definitions: Map<string, ActionDefinition>;
  handlers: Map<string, ActionHandler>;

  constructor() {
    this.definitions = new Map();
    this.handlers = new Map();
  }

  register(name: string, def: ActionDefinition, handler: ActionHandler) {
    this.definitions.set(name, def);
    this.handlers.set(name, handler);
  }

  makeActionMessage(name: string, id: string): ActionMessage {
    const def = this.definitions.get(name);

    const inputs: Port[] = [];
    for (const input of def.inputs) {
      inputs.push({
        name: input.name,
        id: `${id}#${input.name}`,
      });
    }

    const outputs: Port[] = [];
    for (const output of def.outputs) {
      outputs.push({
        name: output.name,
        id: `${id}#${output.name}`,
      });
    }

    return {
      id,
      name,
      inputs,
      outputs,
    };
  }

  makeAction(
    name: string,
    id: string,
    nodeMap: NodeMap,
    stream: EvergreenStream,
    session: Session | null,
  ): Action {
    const def = this.definitions.get(name);
    const handler = this.handlers.get(name);

    return new Action(def, handler, id, nodeMap, stream, session);
  }
}

export class Action {
  private readonly definition: ActionDefinition;
  private handler: ActionHandler;
  private readonly id: string;

  private nodeMap: NodeMap | null;
  private stream: EvergreenStream | null;
  private session: Session | null;

  private bindStreamsOnInputsDefault: boolean = true;
  private bindStreamsOnOutputsDefault: boolean = false;

  private readonly nodesWithBoundStreams: Set<AsyncNode>;

  constructor(
    definition: ActionDefinition,
    handler: ActionHandler,
    id: string = '',
    nodeMap: NodeMap | null = null,
    stream: EvergreenStream | null = null,
    session: Session | null = null,
  ) {
    this.definition = definition;
    this.handler = handler;
    this.id = id;
    this.nodeMap = nodeMap;
    this.stream = stream;
    this.session = session;

    this.bindStreamsOnInputsDefault = true;
    this.bindStreamsOnOutputsDefault = false;
    this.nodesWithBoundStreams = new Set();
  }

  getDefinition(): ActionDefinition {
    return this.definition;
  }

  getActionMessage(): ActionMessage {
    const def = this.definition;

    const inputs: Port[] = [];
    for (const input of def.inputs) {
      inputs.push({
        name: input.name,
        id: this.getInputId(input.name),
      });
    }

    const outputs: Port[] = [];
    for (const output of def.outputs) {
      outputs.push({
        name: output.name,
        id: this.getOutputId(output.name),
      });
    }

    return {
      id: this.id,
      name: def.name,
      inputs,
      outputs,
    };
  }

  async run(): Promise<void> {
    this.bindStreamsOnInputsDefault = false;
    this.bindStreamsOnOutputsDefault = true;

    try {
      await this.handler(this);
    } finally {
      await this.unbindStreams();
    }
  }

  async call(): Promise<void> {
    this.bindStreamsOnInputsDefault = true;
    this.bindStreamsOnOutputsDefault = false;

    await this.stream.send({ actions: [this.getActionMessage()] });
  }

  async getNode(id: string): Promise<AsyncNode> {
    return await this.nodeMap.getNode(id);
  }

  async getInput(
    name: string,
    bindStream: boolean | null = null,
  ): Promise<AsyncNode> {
    const node = await this.getNode(this.getInputId(name));
    let bindStreamResolved = bindStream;
    if (bindStream === null) {
      bindStreamResolved = this.bindStreamsOnInputsDefault;
    }
    if (this.stream !== null && bindStreamResolved) {
      await node.bindWriterStream(this.stream);
      this.nodesWithBoundStreams.add(node);
    }
    return node;
  }

  async getOutput(
    name: string,
    bindStream: boolean | null = null,
  ): Promise<AsyncNode> {
    const node = await this.getNode(this.getOutputId(name));
    let bindStreamResolved = bindStream;
    if (bindStream === null) {
      bindStreamResolved = this.bindStreamsOnOutputsDefault;
    }
    if (this.stream !== null && bindStreamResolved) {
      await node.bindWriterStream(this.stream);
      this.nodesWithBoundStreams.add(node);
    }
    return node;
  }

  bindHandler(handler: ActionHandler) {
    this.handler = handler;
  }

  bindNodeMap(nodeMap: NodeMap) {
    this.nodeMap = nodeMap;
  }
  getNodeMap(): NodeMap {
    return this.nodeMap as NodeMap;
  }

  bindStream(stream: EvergreenStream) {
    this.stream = stream;
  }
  getStream(): EvergreenStream {
    return this.stream as EvergreenStream;
  }

  getRegistry(): ActionRegistry {
    return this.session.getActionRegistry();
  }

  bindSession(session: Session) {
    this.session = session;
  }
  getSession(): Session {
    return this.session as Session;
  }

  private getInputId(name: string): string {
    return `${this.id}#${name}`;
  }

  private getOutputId(name: string): string {
    return `${this.id}#${name}`;
  }

  private async unbindStreams() {
    for (const node of this.nodesWithBoundStreams) {
      await node.bindWriterStream(null);
    }
    this.nodesWithBoundStreams.clear();
  }
}

export function fromActionMessage(
  message: ActionMessage,
  registry: ActionRegistry,
  nodeMap: NodeMap,
  stream: EvergreenStream,
  session: Session | null = null,
) {
  const inputs = message.inputs;
  const outputs = message.outputs;

  let actionIdSrc: string = '';
  if (inputs.length == 0) {
    actionIdSrc = outputs[0].id;
  } else {
    actionIdSrc = inputs[0].id;
  }

  const actionIdParts = actionIdSrc.split('#');
  const actionId = actionIdParts[0];

  const def = registry.definitions.get(message.name);
  const handler = registry.handlers.get(message.name);

  return new Action(def, handler, actionId, nodeMap, stream, session);
}
