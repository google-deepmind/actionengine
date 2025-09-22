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

type AsyncNode = import('../asyncNode.js').AsyncNode;
type NodeMap = import('../asyncNode.js').NodeMap;
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
    stream: BaseActionEngineStream,
    session: Session | null,
  ): Action;
}

declare class Action {
  constructor(
    def: ActionDefinition,
    handler: ActionHandler,
    id: string,
    nodeMap: NodeMap,
    stream: BaseActionEngineStream,
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
  bindStream(stream: BaseActionEngineStream): void;
  getStream(): BaseActionEngineStream;
  getRegistry(): ActionRegistry;

  bindSession(session: Session): void;
  getSession(): Session;
}

declare type ActionHandler = (action: Action) => Promise<void>;
