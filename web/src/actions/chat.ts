import { ChatMessage } from '@/components/dom/Chat'

type MessageSetter = (fn: (prev: ChatMessage[]) => ChatMessage[]) => void

export const REHYDRATE_SESSION_SCHEMA = {
  name: 'rehydrate_session',
  inputs: [{ name: 'session_token', type: 'text/plain' }],
  outputs: [
    { name: 'previous_messages', type: 'text/plain' },
    { name: 'previous_thoughts', type: 'text/plain' },
  ],
}

export const GENERATE_CONTENT_SCHEMA = {
  name: 'generate_content',
  inputs: [
    { name: 'api_key', type: 'text/plain' },
    { name: 'session_token', type: 'text/plain' },
    { name: 'prompt', type: 'text/plain' },
    { name: 'chat_input', type: 'text/plain' },
  ],
  outputs: [
    { name: 'output', type: 'text/plain' },
    { name: 'thoughts', type: 'text/plain' },
    { name: 'new_session_token', type: 'text/plain' },
  ],
}
