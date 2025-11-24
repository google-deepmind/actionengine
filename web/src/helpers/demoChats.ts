import { AsyncNode } from '@helenapankov/actionengine'

import { ChatMessage } from '@/components/dom/Chat'

type MessageSetter = (fn: (prev: ChatMessage[]) => ChatMessage[]) => void

export const rehydrateMessages = async (
  node: AsyncNode,
  setMessages: MessageSetter,
) => {
  node.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  let idx = 0
  const textDecoder = new TextDecoder('utf-8')
  for await (const chunk of node) {
    const id = `m${Date.now()}-${idx.toString(36)}`
    const text = textDecoder.decode(chunk.data)
    const sender = idx % 2 === 0 ? 'You' : 'Server'

    setMessages((prev) => [...prev, { text, sender, id }])
    ++idx
  }
}

export const rehydrateThoughts = async (
  node: AsyncNode,
  setThoughts: MessageSetter,
) => {
  node.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  const textDecoder = new TextDecoder('utf-8')
  let idx = 0
  for await (const chunk of node) {
    const id = `t${Date.now()}-${idx.toString(36)}`
    const text = textDecoder.decode(chunk.data)
    setThoughts((prev) => [
      ...prev,
      {
        text,
        sender: 'Thoughts',
        id,
      },
    ])
    ++idx
  }
}

export const setChatMessagesFromAsyncNode = async (
  node: AsyncNode,
  setMessages: MessageSetter,
) => {
  node.setReaderOptions(
    /* ordered */ true,
    /* removeChunks */ true,
    /* timeout */ -1,
  )
  const textDecoder = new TextDecoder('utf-8')
  let first = true
  let idx = -1
  for await (const chunk of node) {
    ++idx
    const text = textDecoder.decode(chunk.data)
    if (first) {
      first = false
      setMessages((prev) => [
        ...prev,
        { text, sender: 'Server', id: `${Date.now()}-${idx.toString(36)}` },
      ])
      continue
    }

    setMessages((prev) => {
      const currentMessage = prev[prev.length - 1]
      if (currentMessage && currentMessage.sender === 'Server') {
        return [
          ...prev.slice(0, -1),
          {
            ...currentMessage,
            text: (currentMessage.text + text).replace('\n', '<br/>'),
          },
        ]
      }
    })
  }
}
