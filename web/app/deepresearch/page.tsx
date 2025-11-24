'use client'

import { Chat, ChatMessage } from '@/components/dom/Chat'
import { decode } from '@msgpack/msgpack'

import {
  Action,
  ActionRegistry,
  AsyncNode,
  makeTextChunk,
} from '@helenapankov/actionengine'
import { useCallback, useContext, useEffect, useState } from 'react'
import { useControls } from 'leva'
import { useSearchParams } from 'next/navigation'
import { ActionEngineContext, makeAction } from '@/helpers/actionengine'
import {
  GENERATE_CONTENT_SCHEMA,
  REHYDRATE_SESSION_SCHEMA,
} from '@/actions/chat'
import { setChatMessagesFromAsyncNode } from '@/helpers/demoChats'

interface DeepResearchAction {
  id: string
  type: string
}

const iterateLogs = async (node: AsyncNode, setThoughts) => {
  console.log('Iterating logs @', node.getId())
  node.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  const textDecoder = new TextDecoder('utf-8')
  let idx = 0
  for await (const chunk of node) {
    const text = textDecoder.decode(chunk.data)
    const id = `${node.getId()}-${idx.toString(36)}`
    setThoughts((prev) => [...prev, { text, sender: 'Agent', id }])
    ++idx
  }
}

const observeNestedActions = async (
  actionsNode: AsyncNode,
  cb?: (action: DeepResearchAction) => Promise<void>,
) => {
  actionsNode.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  for await (const chunk of actionsNode) {
    const [model, data] = decode(chunk.data) as [string, Uint8Array]
    const action = decode(data) as DeepResearchAction
    console.log('Observing nested action:', model, action)
    if (cb !== undefined) {
      cb(action).then()
    }
  }
}

const useAuxControls = () => {
  const searchParams = useSearchParams()
  const secret = searchParams.get('q')

  return useControls(
    '',
    () => {
      return {
        apiKey: {
          value: secret ? secret : '',
          label: 'API key',
        },
      }
    },
    [],
  )
}

const registerDeepResearchAction = (registry: ActionRegistry) => {
  registry.register(
    'deep_research',
    {
      name: 'deep_research',
      inputs: [
        { name: 'api_key', type: 'text/plain' },
        { name: 'topic', type: 'text/plain' },
      ],
      outputs: [
        { name: 'report', type: 'text/plain' },
        { name: 'actions', type: '__BaseModel__' },
      ],
    },
    async (_: Action) => {},
  )
}

export default function Page() {
  const [actionEngine] = useContext(ActionEngineContext)

  const [controls] = useAuxControls()

  const [streamReady, setStreamReady] = useState(false)
  useEffect(() => {
    if (!actionEngine || !actionEngine.stream) {
      setStreamReady(false)
      return
    }
    const current = streamReady
    let cancelled = false
    actionEngine.stream.waitUntilReady().then(() => {
      if (cancelled) {
        return
      }
      setStreamReady(true)
    })
    return () => {
      cancelled = true
      setStreamReady(current)
    }
  }, [actionEngine])

  const apiKey = controls.apiKey
  const enableInput = !!apiKey && streamReady
  const disabledInputMessage = !streamReady
    ? 'Waiting for connection...'
    : 'Please enter your API key'

  useEffect(() => {
    const { actionRegistry } = actionEngine
    if (!actionRegistry) {
      return
    }
    actionRegistry.register('generate_content', GENERATE_CONTENT_SCHEMA)
    actionRegistry.register('rehydrate_session', REHYDRATE_SESSION_SCHEMA)
    registerDeepResearchAction(actionRegistry)
  }, [actionEngine])

  const [messages, setMessages] = useState([])
  const [thoughts, setThoughts] = useState([])

  const observeActionCallback = useCallback(
    async (action: DeepResearchAction) => {
      const logNode = await actionEngine.nodeMap.getNode(
        `${action.id}#user_log`,
      )
      iterateLogs(logNode, setThoughts).then()
    },
    [actionEngine],
  )

  const sendMessage = async (msg: ChatMessage) => {
    if (msg.text.startsWith('/get')) {
      const nodeId = msg.text.split(' ')[1]
      if (!nodeId) {
        setThoughts((prev) => [
          ...prev,
          { text: 'Usage: /get [nodeId]', sender: 'System', id: Date.now() },
        ])
        return
      }
      if (!(await actionEngine.nodeMap.hasNode(nodeId))) {
        setThoughts((prev) => [
          ...prev,
          {
            text: `Node ${nodeId} not found`,
            sender: 'System',
            id: Date.now(),
          },
        ])
        return
      }
      setThoughts((prev) => [
        ...prev,
        { text: `Fetching node ${nodeId}:`, sender: 'System', id: Date.now() },
      ])
      const node = await actionEngine.nodeMap.getNode(nodeId)
      setChatMessagesFromAsyncNode(node, setThoughts).then()
      return
    }
    const action = makeAction('deep_research', actionEngine)
    action.call().then()

    setMessages((prev) => [...prev, msg])

    const apiKeyNode = await action.getInput('api_key')
    await apiKeyNode.putAndFinalize(makeTextChunk(apiKey))

    const topicNode = await action.getInput('topic')
    await topicNode.putAndFinalize(makeTextChunk(msg.text))

    setChatMessagesFromAsyncNode(
      await action.getOutput('report'),
      setMessages,
    ).then()
    observeNestedActions(
      await action.getOutput('actions'),
      observeActionCallback,
    ).then()
  }

  return (
    <>
      <div className='flex max-h-screen w-screen flex-row justify-center'>
        <div className='flex w-full max-w-2xl flex-col items-center justify-center space-y-4 p-4'>
          <Chat
            name='Deep Research'
            messages={messages}
            sendMessage={sendMessage}
            disableInput={!enableInput}
            disabledInputMessage={disabledInputMessage}
          />
        </div>
        <div className='flex w-full max-w-2xl flex-col items-center justify-center space-y-4 p-4'>
          <Chat
            name='Debug messages'
            messages={thoughts}
            sendMessage={async (_) => {}}
            disableInput
            disabledInputMessage='This is a read-only chat for generated content.'
          />
        </div>
      </div>
    </>
  )
}
