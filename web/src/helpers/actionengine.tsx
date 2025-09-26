import { trait, World } from 'koota'
import {
  Action,
  ActionRegistry,
  NodeMap,
  Session,
  ActionEngineStream,
  WebRtcActionEngineStream,
} from 'actionengine'
import { v4 as uuidv4 } from 'uuid'
import { ReadonlyURLSearchParams } from 'next/navigation'
import {
  createContext,
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useState,
} from 'react'

export interface ActionEngineState {
  nodeMap: NodeMap
  actionRegistry: ActionRegistry
  stream: ActionEngineStream | WebRtcActionEngineStream | null
  session: Session
}

export const makeActionEngineState = (): ActionEngineState => {
  const state: ActionEngineState = {
    nodeMap: new NodeMap(),
    actionRegistry: new ActionRegistry(),
    stream: null,
    session: null as Session | null,
  }
  state.session = new Session(state.stream, state.nodeMap)
  state.session.bindActionRegistry(state.actionRegistry).then()
  return state
}

export const ActionEngineContext = createContext<
  [s: ActionEngineState, set: Dispatch<SetStateAction<ActionEngineState>>]
>([makeActionEngineState(), () => {}])

export const ActionEngineStateTrait = trait(() => {
  return makeActionEngineState()
})

export const ensureActionEngine = (world: World) => {
  if (!world.has(ActionEngineStateTrait)) {
    world.add(ActionEngineStateTrait)
  }
  return world.get(ActionEngineStateTrait)
}

export interface WebRtcParams {
  peer: string
  signallingHost: string
  turnIceServer: {
    urls: string
    username: string
    credential: string
  } | null
}

export const parseWebRtcParamsFromSearchParams = (
  params: ReadonlyURLSearchParams,
) => {
  const peer = params.get('webrtc_peer') || 'demoserver'

  const signallingHost =
    params.get('webrtc_signalling_host') || 'wss://demos.helena.direct:19001'

  const turnServer = params.get('webrtc_turn_server') || 'actionengine.dev:3478'
  const turnUsername = params.get('webrtc_turn_username') || 'helena'
  const turnCredential =
    params.get('webrtc_turn_credential') || 'actionengine-webrtc-testing'

  const turnIceServer =
    turnServer && turnUsername && turnCredential
      ? {
          urls: `turn:${turnServer}`,
          username: turnUsername,
          credential: turnCredential,
        }
      : null

  return { peer, signallingHost, turnIceServer } as WebRtcParams
}

export const useWebRtcStream = (identity: string, params: WebRtcParams) => {
  const [stream, setStream] = useState<WebRtcActionEngineStream | null>(null)
  let cancelled = false
  const replaceStream = useCallback(
    async (identity: string, params: WebRtcParams) => {
      if (stream !== null) {
        await stream.close()
      }

      const newStream = new WebRtcActionEngineStream(
        params.signallingHost,
        identity,
        params.peer,
        params.turnIceServer,
      )

      await newStream.waitUntilReady()
      if (cancelled && newStream) {
        await newStream.close()
        return
      }

      setStream(newStream)
    },
    [stream, setStream],
  )

  useEffect(() => {
    replaceStream(identity, params).then()
    return () => {
      cancelled = true
    }
  }, [identity, params])
  return stream
}

export const makeAction = (name: string, engine: ActionEngineState) => {
  return engine.actionRegistry.makeAction(
    name,
    uuidv4(),
    engine.nodeMap,
    engine.stream,
    engine.session,
  )
}

export type WorldActionHandler = (world: World, action: Action) => Promise<void>
export type ActionHandler = (action: Action) => Promise<void>

export const bindActionHandlerToWorld = (
  handler: WorldActionHandler,
  world: World,
): ActionHandler => {
  return async (action: Action) => {
    await handler(world, action)
  }
}
