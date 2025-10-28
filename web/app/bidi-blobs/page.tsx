'use client'

import dynamic from 'next/dynamic'
import { useWorld } from 'koota/react'
import { makeTextChunk, Action, ActionRegistry, AsyncNode } from 'actionengine'
import {
  ActionEngineState,
  bindActionHandlerToWorld,
  makeAction,
  ActionEngineContext,
} from '@/helpers/actionengine'
import { encode } from '@msgpack/msgpack'
import { useTrait } from 'koota/react'
import { Suspense, useCallback, useContext, useEffect, useState } from 'react'
import { Entity, unpackEntity, World } from 'koota'
import { RootState } from '@react-three/fiber'
import { View, Common } from '@/components/canvas/View'

import {
  Color,
  IsSelected,
  PositionTrait,
  RotationTrait,
  ScaleTrait,
} from '@/helpers/traits'
import { Chat, ChatMessage } from '@/components/dom/Chat'
import {
  bindTakeGlScreenshotToState,
  GET_SELECTED_ENTITY_IDS_SCHEMA,
  getSelectedEntityIds,
  SET_COLOR_SCHEMA,
  SET_POSITION_SCHEMA,
  setColor,
  setPosition,
  SHIFT_POSITION_SCHEMA,
  shiftPosition,
  TAKE_GL_SCREENSHOT_SCHEMA,
} from '@/actions/gui'
import { invalidate } from '@react-three/fiber'

const Blob = dynamic(
  () => import('@/components/canvas/Examples').then((mod) => mod.Blob),
  { ssr: false },
)

const registerActions = (world: World, actionEngine: ActionEngineState) => {
  const registry = actionEngine.actionRegistry
  registry?.register(
    'set_position',
    SET_POSITION_SCHEMA,
    bindActionHandlerToWorld(setPosition, world),
  )
  registry?.register(
    'set_color',
    SET_COLOR_SCHEMA,
    bindActionHandlerToWorld(setColor, world),
  )
  registry?.register(
    'shift_position',
    SHIFT_POSITION_SCHEMA,
    bindActionHandlerToWorld(shiftPosition, world),
  )
  registry?.register(
    'get_selected_entity_ids',
    GET_SELECTED_ENTITY_IDS_SCHEMA,
    bindActionHandlerToWorld(getSelectedEntityIds, world),
  )
  registry?.register(
    'execute_prompt',
    {
      name: 'execute_prompt',
      inputs: [{ name: 'prompt', type: 'text/plain' }],
      outputs: [{ name: 'logs', type: 'text/plain' }],
    },
    async (_: Action) => {},
  )
}

const setRandomPosition = (entity: Entity, engine: ActionEngineState) => {
  const randomX = (Math.random() - 0.5) * 2
  const randomY = (Math.random() - 0.5) * 2
  const randomZ = (Math.random() - 0.5) * 2

  const action = makeAction('set_position', engine)
  action.run().then()
  action.getInput('request', /*bindStream*/ false).then(async (requestNode) => {
    const { entityId } = unpackEntity(entity)
    await requestNode.putAndFinalize({
      metadata: { mimetype: 'application/x-msgpack' },
      data: encode({
        entity_ids: [entityId],
        positions: [[randomX, randomY, randomZ]],
      }) as Uint8Array<ArrayBuffer>,
    })
  })
}

const setRandomColor = (entity: Entity, engine: ActionEngineState) => {
  console.log('setRandomColor')
  let randomColorHex = Math.floor(Math.random() * 16777216).toString(16)
  if (randomColorHex.length < 6) {
    randomColorHex = '0'.repeat(6 - randomColorHex.length) + randomColorHex
  }

  const action = makeAction('set_color', engine)
  action.run().then()
  action.getInput('request', /*bindStream*/ false).then(async (requestNode) => {
    const { entityId } = unpackEntity(entity)
    await requestNode.putAndFinalize({
      metadata: { mimetype: 'application/x-msgpack' },
      data: encode({
        entity_ids: [entityId],
        colors: [`#${randomColorHex}`],
      }) as Uint8Array<ArrayBuffer>,
    })
  })
}

const BlobByEntity = ({
  entity,
  onClick,
  ...props
}: {
  entity: Entity
  onClick?: (e: Event) => void
}) => {
  const position = useTrait(entity, PositionTrait)
  const color = useTrait(entity, Color)
  const scale = useTrait(entity, ScaleTrait)

  return (
    <Blob
      color={color}
      position={position}
      scale={scale}
      onClick={(e: Event) => {
        if (onClick) {
          e.stopPropagation()
          onClick(e)
        }
      }}
      {...props}
    ></Blob>
  )
}

const addLogsToMessages = async (logs: AsyncNode, setMessages: any) => {
  logs.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  const textDecoder = new TextDecoder('utf-8')
  let idx = 0
  for await (const chunk of logs) {
    const text = textDecoder.decode(chunk.data)
    setMessages((prev: ChatMessage[]) => [
      ...prev,
      {
        id: `log-${idx++}-${Date.now()}`,
        text,
        sender: 'Log',
      },
    ])
  }
}

const subscribeToActionLogs = async (
  actionNode: AsyncNode,
  localLogs: AsyncNode,
) => {
  actionNode.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  for await (const chunk of actionNode) {
    console.log(chunk)
    await localLogs.put(chunk)
  }
}

export default function Page() {
  const world = useWorld()
  const [actionEngine, setActionEngine] = useContext(ActionEngineContext)

  const [initialized, setInitialized] = useState(false)

  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: 'start',
      sender: 'Intro',
      text: 'Try sending various instructions about the position and color of blobs. See an example in the placeholder. If nothing happens when you send a message, try reloading the page.',
    },
  ])

  useEffect(() => {
    invalidate()
  }, [messages])

  useEffect(() => {
    if (!world || !actionEngine.stream || initialized) {
      return
    }
    registerActions(world, actionEngine)

    actionEngine.nodeMap
      .getNode('logs')
      .then((logs) => addLogsToMessages(logs, setMessages))
      .then()
    setInitialized(true)

    return () => {
      setActionEngine({ ...actionEngine, actionRegistry: new ActionRegistry() })
      setInitialized(false)
    }
  }, [world, actionEngine])

  const [entities, setEntities] = useState<Entity[]>([])

  useEffect(() => {
    if (!world) {
      return
    }
    const spawnedEntities: Entity[] = []
    for (let i = 0; i < 3; i++) {
      const entity = world.spawn(
        PositionTrait,
        RotationTrait,
        ScaleTrait,
        Color,
        IsSelected,
      )
      spawnedEntities.push(entity)
      entity.set(PositionTrait, [
        (Math.random() - 0.5) * 2,
        (Math.random() - 0.5) * 2,
        (Math.random() - 0.5) * 2,
      ])
      entity.set(ScaleTrait, [0.5, 0.5, 0.5])

      let randomColorHex = Math.floor(Math.random() * 16777216).toString(16)
      if (randomColorHex.length < 6) {
        randomColorHex = '0'.repeat(6 - randomColorHex.length) + randomColorHex
      }
      entity.set(Color, `#${randomColorHex}`)

      setEntities((prev) => [...prev, entity])
    }

    return () => {
      for (const entity of spawnedEntities) {
        entity.destroy()
      }
      setEntities([])
    }
  }, [world])

  const getBlobs = useCallback(() => {
    return entities.map((entity, index) => {
      return (
        <BlobByEntity
          key={index}
          entity={entity}
          onClick={(e: Event) => {
            e.stopPropagation()
            setRandomPosition(entity, actionEngine!)
            setRandomColor(entity, actionEngine!)
          }}
        ></BlobByEntity>
      )
    })
  }, [entities, actionEngine])

  const [r3fState, setR3fState] = useState<RootState | null>(null)
  useEffect(() => {
    if (!actionEngine?.actionRegistry) {
      return
    }
    actionEngine.actionRegistry.register(
      'take_screenshot',
      TAKE_GL_SCREENSHOT_SCHEMA,
      bindTakeGlScreenshotToState(r3fState),
    )
    console.log('Registered take_screenshot action')
  }, [r3fState, actionEngine?.actionRegistry])

  return (
    <div>
      <Suspense>
        <View
          className='absolute top-0 flex h-screen w-3/5 flex-col items-center justify-center'
          orbit
        >
          {(() => {
            return getBlobs()
          })()}
          <Common setR3fState={setR3fState} />
        </View>
      </Suspense>
      <Chat
        name='Action log'
        messages={messages}
        placeholder={
          messages.length > 1 ? undefined : 'move blob 1 to the origin'
        }
        sendMessage={async (msg: ChatMessage) => {
          setMessages((prev) => [...prev, msg])

          const executeAction = makeAction('execute_prompt', actionEngine!)
          const promptNode = await executeAction.getInput('prompt')
          await promptNode.putAndFinalize(makeTextChunk(msg.text))

          const logs = await actionEngine?.nodeMap.getNode('logs')
          subscribeToActionLogs(
            await executeAction.getInput('logs'),
            logs,
          ).then()
          await executeAction.call()

          await logs.put(
            makeTextChunk(`CALL execute_prompt, prompt="${msg.text}"`),
          )
        }}
        className='absolute top-0 h-screen w-2/5 left-3/5'
      ></Chat>
    </div>
  )
}
