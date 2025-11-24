'use client'

import { Chat, ChatMessage } from '@/components/dom/Chat'

import { AsyncNode, makeTextChunk } from '@helenapankov/actionengine'
import { useCallback, useContext, useEffect, useState } from 'react'
import { useControls } from 'leva'
import { usePathname, useSearchParams } from 'next/navigation'
import { ActionEngineContext, makeAction } from '@/helpers/actionengine'
import {
  GENERATE_CONTENT_SCHEMA,
  REHYDRATE_SESSION_SCHEMA,
} from '@/actions/chat'
import {
  rehydrateMessages,
  rehydrateThoughts,
  setChatMessagesFromAsyncNode,
} from '@/helpers/demoChats'

const setSessionTokenFromAction = async (node: AsyncNode, setSessionToken) => {
  node.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  for await (const chunk of node) {
    const sessionToken = new TextDecoder('utf-8').decode(chunk.data)
    setSessionToken(sessionToken)
  }
}

const useAuxControls = () => {
  const searchParams = useSearchParams()
  const secret = searchParams.get('q')
  return useControls('', () => {
    return {
      apiKey: {
        value: secret ? secret : '',
        label: 'API key',
      },
    }
  })
}

export default function Page() {
  const [actionEngine] = useContext(ActionEngineContext)

  const searchParams = useSearchParams()

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
  }, [actionEngine])

  const [messages, setMessages] = useState([])
  const [thoughts, setThoughts] = useState([])

  const [rehydrated, setRehydrated] = useState(false)
  const sessionToken = useSearchParams().get('session_token') || ''
  useEffect(() => {
    if (!actionEngine.stream) {
      return
    }
    if (rehydrated) {
      return
    }
    const rehydrate = async () => {
      console.log('Rehydrating session with token:', sessionToken)
      const action = makeAction('rehydrate_session', actionEngine)
      action.call().then()

      const sessionTokenNode = await action.getInput('session_token')
      await sessionTokenNode.putAndFinalize(makeTextChunk(sessionToken || ''))

      const previousMessagesNode = await action.getOutput('previous_messages')
      rehydrateMessages(previousMessagesNode, setMessages).then()

      const previousThoughtsNode = await action.getOutput('previous_thoughts')
      rehydrateThoughts(previousThoughtsNode, setThoughts).then()
    }
    actionEngine.stream.waitUntilReady().then(() => {
      setRehydrated(true)
      if (sessionToken) {
        rehydrate().then()
      }
    })
  }, [actionEngine, rehydrated])

  const createQueryString = useCallback(
    (name: string, value: string) => {
      const params = new URLSearchParams(searchParams.toString())
      params.set(name, value)

      return params.toString()
    },
    [searchParams],
  )

  const pathname = usePathname()

  const [nextSessionToken, setNextSessionToken] = useState<string>(sessionToken)
  useEffect(() => {
    if (!nextSessionToken) {
      return
    }
    window.history.replaceState(
      null,
      '',
      pathname +
        '?' +
        createQueryString('session_token', nextSessionToken || ''),
    )
  }, [createQueryString, nextSessionToken, pathname])

  const sendMessage = async (msg: ChatMessage) => {
    const action = makeAction('generate_content', actionEngine)
    await action.call()

    setMessages((prev) => [...prev, msg])

    const sessionTokenNode = await action.getInput('session_token')
    await sessionTokenNode.putAndFinalize(makeTextChunk(sessionToken || ''))

    const chatInputNode = await action.getInput('chat_input')
    await chatInputNode.putAndFinalize(makeTextChunk(msg.text))

    const apiKeyNode = await action.getInput('api_key')
    await apiKeyNode.putAndFinalize(makeTextChunk(apiKey))

    setChatMessagesFromAsyncNode(
      await action.getOutput('output'),
      setMessages,
    ).then()
    setChatMessagesFromAsyncNode(
      await action.getOutput('thoughts'),
      setThoughts,
    ).then()
    setSessionTokenFromAction(
      await action.getOutput('new_session_token'),
      setNextSessionToken,
    ).then()
  }

  return (
    <>
      <div className='flex max-h-screen w-screen flex-row justify-center'>
        <div className='flex w-full max-w-2xl flex-col items-center justify-center space-y-4 p-4'>
          <Chat
            name={`${apiKey === 'ollama' ? 'Ollama' : 'Gemini'} session ${nextSessionToken}`}
            messages={messages}
            sendMessage={sendMessage}
            disableInput={!enableInput}
            disabledInputMessage={disabledInputMessage}
          />
        </div>
        <div className='flex w-full max-w-2xl flex-col items-center justify-center space-y-4 p-4'>
          <Chat
            name='Thoughts'
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
