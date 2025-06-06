type AsyncNode = import('../asyncNode.js').AsyncNode;
type NodeMap = import('../asyncNode.js').NodeMap;
type BaseEvergreenStream = import('../stream.js').BaseEvergreenStream;
type Session = import('../session.js').Session;

declare interface ActionNode {
  name: string;
  type: string;
}

declare interface ActionDefinition {
  name: string;
  inputs: ActionNode[];
  outputs: ActionNode[];
}

declare class ActionRegistry {
  definitions: Map<string, ActionDefinition>;
  handlers: Map<string, ActionHandler>;

  constructor();
  register(name: string, def: ActionDefinition, handler: ActionHandler): void;
  makeActionMessage(name: string, id: string): ActionMessage;
  makeAction(
    name: string,
    id: string,
    nodeMap: NodeMap,
    stream: BaseEvergreenStream,
    session: Session | null,
  ): Action;
}

declare class Action {
  constructor(
    def: ActionDefinition,
    handler: ActionHandler,
    id: string,
    nodeMap: NodeMap,
    stream: BaseEvergreenStream,
    session: Session | null,
  );

  getDefinition(): ActionDefinition;
  getActionMessage(): ActionMessage;

  run(): Promise<void>;
  call(): Promise<void>;

  getNode(id: string): Promise<AsyncNode>;
  getInput(name: string): Promise<AsyncNode>;
  getOutput(name: string): Promise<AsyncNode>;

  bindHandler(handler: ActionHandler): void;

  bindNodeMap(nodeMap: NodeMap): void;
  getNodeMap(): NodeMap;
  bindStream(stream: BaseEvergreenStream): void;
  getStream(): BaseEvergreenStream;
  getRegistry(): ActionRegistry;

  bindSession(session: Session): void;
  getSession(): Session;
}

declare type ActionHandler = (action: Action) => Promise<void>;
