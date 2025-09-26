import { useEffect, useRef, useState } from 'react'
import { marked } from 'marked'

export interface ChatMessage {
  id: string
  text: string
  sender: string
}

export interface ChatProps {
  name?: string
  messages: ChatMessage[]
  sendMessage: (msg: ChatMessage) => Promise<void>
  disableInput?: boolean
  disabledInputMessage?: string
  className?: string
  placeholder?: string
}

export function Chat(props: ChatProps) {
  const { messages } = props
  const [input, setInput] = useState('')

  const messagesEndRef = useRef(null)
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
  }, [messages])

  const className =
    props.className || 'mx-4 flex w-full max-w-full flex-row p-4 max-h-screen'

  const numRowsInInput = Math.max(1, input.split('\n').length)
  return (
    <>
      <div className={className}>
        <div className='flex max-h-full w-full max-w-full flex-col space-y-4 rounded-lg bg-white p-4 shadow-lg'>
          <h1 className='max-h-fit text-center text-2xl font-bold'>
            {props.name || 'Echo action'}
          </h1>
          <div className='max-h-full overflow-y-auto rounded-md border bg-gray-50 p-4'>
            {messages.map((msg, idx) => {
              const markedText = { __html: marked(msg.text) }
              return (
                <div
                  key={msg.id}
                  ref={idx === messages.length - 1 ? messagesEndRef : null}
                  className={`mb-2 flex ${
                    msg.sender === 'You' ? 'justify-end' : 'justify-start'
                  }`}
                >
                  <div
                    className={`rounded px-4 py-2 text-white ${
                      msg.sender === 'You' ? 'bg-green-500' : 'bg-blue-500'
                    }`}
                  >
                    <span className='block text-xs'>{msg.sender}</span>
                    <div dangerouslySetInnerHTML={markedText} className='' />
                  </div>
                </div>
              )
            })}
          </div>

          <form
            onSubmit={async (e) => {
              e.preventDefault()
              if (!input.trim()) return
              await props.sendMessage({
                id: `${Date.now()}`,
                text: input,
                sender: 'You',
              })
              setInput('')
            }}
            className='mt-4 flex items-end space-x-2'
          >
            <textarea
              className={`flex-1 rounded border px-3 py-2 ${props.disableInput ? 'bg-gray-50' : 'bg-white'}`}
              rows={Math.min(10, numRowsInInput)}
              placeholder={
                props.disableInput
                  ? props.disabledInputMessage || 'Establishing connection...'
                  : props.placeholder || 'Type a message'
              }
              value={input}
              disabled={props.disableInput}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  if (!input.trim()) return

                  props
                    .sendMessage({
                      id: `${Date.now()}`,
                      text: input,
                      sender: 'You',
                    })
                    .then()
                  setInput('')
                }
              }}
              onChange={(e) => setInput(e.target.value)}
            />
            <button
              type='submit'
              className='max-h-12 rounded bg-blue-600 px-4 py-2 text-white hover:bg-blue-700'
            >
              Send
            </button>
          </form>
        </div>
      </div>
    </>
  )
}
