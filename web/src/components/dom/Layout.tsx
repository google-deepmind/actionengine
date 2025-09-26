'use client'

import { useEffect, useRef, useState } from 'react'
import dynamic from 'next/dynamic'
import { createWorld } from 'koota'
import { WorldProvider } from 'koota/react'
import {
  ActionEngineContext,
  makeActionEngineState,
  parseWebRtcParamsFromSearchParams,
  useWebRtcStream,
} from '@/helpers/actionengine'
import { useSearchParams } from 'next/navigation'

const world = createWorld()

const Scene = dynamic(() => import('@/components/canvas/Scene'), { ssr: false })

const Layout = ({ children }) => {
  const [actionEngine, setActionEngine] = useState(makeActionEngineState())

  const searchParams = useSearchParams()

  const webRtcIdentity = '' // random
  const [webRtcParams, setWebRtcParams] = useState(
    parseWebRtcParamsFromSearchParams(searchParams),
  )
  useEffect(() => {
    const newParams = parseWebRtcParamsFromSearchParams(searchParams)
    if (JSON.stringify(newParams) !== JSON.stringify(webRtcParams)) {
      setWebRtcParams(newParams)
    }
  }, [searchParams])
  const stream = useWebRtcStream(webRtcIdentity, webRtcParams)
  useEffect(() => {
    if (!stream || !actionEngine) {
      return
    }
    if (actionEngine.stream === stream) {
      return
    }

    let cancelled = false
    stream.waitUntilReady().then(async () => {
      if (cancelled) {
        return
      }
      await actionEngine.session.bindStream(stream)
      setActionEngine({ ...actionEngine, stream })
    })

    return () => {
      cancelled = true
    }
  }, [stream])

  const ref = useRef<HTMLDivElement>(null)

  return (
    <ActionEngineContext value={[actionEngine, setActionEngine]}>
      <WorldProvider world={world}>
        <div
          ref={ref}
          style={{
            position: 'relative',
            width: ' 100%',
            height: '100%',
            overflow: 'auto',
            touchAction: 'auto',
          }}
        >
          {children}
          <Scene
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              width: '100vw',
              height: '100vh',
              pointerEvents: 'none',
            }}
            eventSource={ref}
            eventPrefix='client'
          />
        </div>
      </WorldProvider>
    </ActionEngineContext>
  )
}

export { Layout }
