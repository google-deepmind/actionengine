'use client'

import dynamic from 'next/dynamic'
import { Suspense, useEffect, useState } from 'react'
import { Common } from '@/components/canvas/View'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Blob } from '@/components/canvas/Examples'
import { useWorld } from 'koota/react'
import { Check, Clipboard } from 'lucide-react'

const View = dynamic(
  () => import('@/components/canvas/View').then((mod) => mod.View),
  {
    ssr: false,
  },
)

const CopyCommand = ({ command }) => {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(command)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch (err) {
      console.error('Copy failed:', err)
    }
  }

  return (
    <div className='inline-flex items-center rounded-lg bg-white dark:bg-gray-900 shadow-sm ring-1 ring-gray-200 dark:ring-gray-700 overflow-hidden max-w-full'>
      {/* Code text */}
      <code className='px-3 py-2 text-sm font-mono text-gray-800 dark:text-gray-100 select-all'>
        {command.split('\n').map((line, index) => (
          <span key={index}>
            {line}
            {index < command.split('\n').length - 1 && <br />}
          </span>
        ))}
      </code>

      {/* Copy button */}
      <button
        onClick={handleCopy}
        className='flex items-center gap-1 px-3 py-2 text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition'
        title='Copy to clipboard'
        aria-label='Copy command'
      >
        {copied ? (
          <>
            <Check className='w-4 h-4 text-green-500' />
            <span className='text-xs text-green-600'>Copied!</span>
          </>
        ) : (
          <>
            <Clipboard className='w-4 h-4' />
            <span className='text-xs'>Copy</span>
          </>
        )}
      </button>
    </div>
  )
}

const GoogleBlobs = () => {
  const router = useRouter()

  const xShift = 0.65

  return (
    <group scale={1.5}>
      <Blob
        scale={0.33}
        position={[xShift - 1.7, 0, 0]}
        color='#4285F4'
        onClick={() => {
          router.push('/bidi-blobs')
        }}
      />
      <Blob
        scale={0.25}
        position={[xShift - 1.08, -0.07, 0]}
        color='#EA4335'
        onClick={() => {
          router.push('/gemini?q=ollama')
        }}
      />
      <Blob
        scale={0.25}
        position={[xShift - 0.55, -0.07, 0]}
        color='#FBBC04'
        onClick={() => {
          router.push('/blob')
        }}
      />
      <Blob
        scale={[0.23, 0.3, 0.25]}
        position={[xShift - 0.03, -0.12, 0]}
        color='#4285F4'
        onClick={() => {
          router.push('/deepresearch?q=alpha-demos')
        }}
      />
      <Blob
        scale={[0.15, 0.3, 0.25]}
        position={[xShift + 0.41, -0.03, 0]}
        color='#34A853'
        onClick={() => {
          router.push(
            'https://github.com/google-deepmind/actionengine/tree/main/examples/003-python-speech-to-text',
          )
        }}
      />
      <Blob
        scale={0.25}
        position={[xShift + 0.88, -0.07, 0]}
        color='#EA4335'
        onClick={() => {
          router.push('/bidi-blobs')
        }}
      />
    </group>
  )
}

export default function Page() {
  const world = useWorld()

  return (
    <>
      <div className='mx-auto flex w-full flex-col flex-wrap items-center md:flex-row  xl:w-4/5'>
        {/* jumbo */}
        <div className='flex w-full flex-col items-start justify-center p-12 pb-6 md:w-2/5 md:text-left'>
          {/*<p className='w-full uppercase'>Interactive demo showcase</p>*/}
          <h1 className='my-4 text-4xl font-bold leading-tight'>
            Action Engine
          </h1>
          <p className='mb-4 text-2xl leading-normal'>
            A toolkit for building multimodal, streaming APIs and UIs
          </p>
          <iframe
            src='https://ghbtns.com/github-btn.html?user=google-deepmind&repo=actionengine&type=star&count=true'
            frameBorder='0'
            scrolling='0'
            width='170'
            height='30'
            title='GitHub'
          />
          <p className='mb-8 text-gray-900 leading-[1.5] text-xs'>
            Jump to examples: various features in{' '}
            <Link className='text-blue-600' href='#python-examples'>
              <u>Python</u>
            </Link>
            , browser client behavior in{' '}
            <Link
              className='text-blue-600'
              href='https://github.com/google-deepmind/actionengine/tree/main/web'
            >
              <u>TypeScript</u>
            </Link>
            , simple starter project in{' '}
            <Link
              className='text-blue-600'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/000-actions'
            >
              <u>C++</u>
            </Link>
            {'.'}
          </p>
        </div>
        <div className='w-full text-center md:w-3/5'>
          <Suspense fallback={null}>
            <View className='flex h-64 w-full flex-col items-center justify-center'>
              <Suspense>
                <GoogleBlobs />
              </Suspense>
              <Common />
            </View>
          </Suspense>
        </div>
      </div>

      <div className='mx-auto flex w-full flex-col flex-wrap items-start pl-12 pr-12 md:flex-row  xl:w-4/5'>
        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative h-fit w-full pb-6 pr-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Super quick start:
            </h2>
            <div className='flex flex-row flex-wrap mb-2'>
              <div className='w-full md:w-1/2 pr-12'>
                <div className='mb-4 text-gray-600 max-w-full'>
                  <h3 className='mb-3 font-semibold text-gray-800'>
                    Python 3.11+, Linux x64 / macOS arm64
                  </h3>
                  <div className='mb-2 mt-2'>
                    <CopyCommand command='pip install action-engine' />
                  </div>
                </div>
                <div className='mb-2 mt-4 text-gray-600 w-full'>
                  <h3 className='mb-3 font-semibold text-gray-800'>
                    or if there is no wheel for your platform,
                  </h3>
                  <p>ensure you have a modern clang installed, and then run:</p>
                  <div className='mb-2 mt-2'>
                    <CopyCommand command='pip install git+https://github.com/google-deepmind/actionengine' />
                  </div>
                </div>
              </div>
              <div className='w-full md:w-1/2 pr-6 pt-6 md:pt-0'>
                <div className='mb-4 text-gray-600 w-full'>
                  <h3 className='mb-3 font-semibold text-gray-800'>
                    TypeScript / Node.js 22+
                  </h3>
                  <p className='mb-2'>In your project, run:</p>
                  <div className='mb-2 mt-2'>
                    <CopyCommand command='npm install @helenapankov/actionengine' />
                  </div>
                </div>
              </div>
            </div>
            <h3
              className='mb-2 font-semibold text-gray-600 underline decoration-dotted decoration-2'
              style={{ cursor: 'pointer' }}
              onClick={() => {
                const element = document.getElementById('python-examples')
                if (element) {
                  element.scrollIntoView({ behavior: 'smooth' })
                }
              }}
            >
              Show me code examples!
            </h3>
          </div>
        </div>
        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-8 md:mb-18 pr-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              First-class streaming and multimodality
            </h2>
            <p className='mb-2 text-gray-600'>
              Send streams of data asynchronously over a single wire connection,
              mixing text, images, audio and your custom types.
            </p>
            <p className='mb-8 text-gray-600'>
              See this in action in a{' '}
              <Link href='/blob' className='text-blue-600'>
                <u>text-to-image</u>
              </Link>{' '}
              generation demo with live progress updates.
            </p>
          </div>
          <div className='relative my-8 h-48 w-full py-6 sm:w-1/2 md:mb-18'>
            <View className='relative h-full  sm:h-48 sm:w-full'>
              <Suspense fallback={null}>
                <Common color={'lightpink'} />
              </Suspense>
            </View>
          </div>
        </div>
        {/* second row */}
        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative h-48 w-full py-6 my-8 sm:w-1/2 md:mb-18 order-last sm:order-first'>
            <View className='relative h-full sm:h-48 sm:w-full'>
              <Suspense fallback={null}>
                <Common color={'lightgreen'} />
              </Suspense>
            </View>
          </div>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-8 md:mb-18 pr-6 sm:pl-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Dedicated I/O channels, selective persistence
            </h2>
            <p className='mb-2 text-gray-600'>
              <Link href='/gemini?q=ollama' className='text-blue-600'>
                <u>Chat with an LLM</u>
              </Link>
              &nbsp;seeing its thought process as a separate stream, retrieve
              history later with a session token.
            </p>
            <p className='mb-8 text-gray-600'>
              Build stateful applications separating logically different data
              easily.
            </p>
          </div>
        </div>
        {/* third row */}
        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-8 md:mb-12'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Observable workflows
            </h2>
            <p className='mb-2 text-gray-600 pr-6'>
              See an agent{' '}
              <Link
                href='/deepresearch?q=alpha-demos'
                className='text-blue-600'
              >
                <u>research a topic</u>
              </Link>{' '}
              by making a plan, running web searches in multiple investigative
              actions, and compiling a final report.
            </p>
            <p className='mb-8 text-gray-600 pr-6'>
              Tap into the intermediate steps, logs and thoughts as they happen.
            </p>
          </div>
          <div className='relative my-8 h-48 w-full py-6 sm:w-1/2 md:mb-12'>
            <View className='relative h-full  sm:h-48 sm:w-full'>
              <Suspense fallback={null}>
                <Common color={'lightblue'} />
              </Suspense>
            </View>
          </div>
        </div>
        {/* fourth row */}

        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative h-fit w-full pb-6 pr-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Explore Action Engine in code examples:
            </h2>
            <div className='flex flex-row flex-wrap'>
              <div className='w-full md:w-1/2 pr-12'>
                <div className='mb-4 text-gray-600 max-w-full'>
                  <h3
                    className='mb-3 font-semibold text-gray-800'
                    id='python-examples'
                  >
                    Python
                  </h3>
                </div>
                <div className='mb-2 mt-4 text-gray-600 w-full'>
                  <p className='mb-3 text-gray-600'>
                    Try this simple example to check everything is installed
                    correctly:
                  </p>
                  <div className='mb-2 mt-2'>
                    <CopyCommand
                      command={`import actionengine\nactionengine.to_chunk("Hello, world!")`}
                    />
                  </div>
                </div>
                <div className='mb-2 mt-4 text-gray-600 w-full'>
                  <h3 className='mb-3 font-semibold text-gray-800'>
                    Once it runs, you're now ready to:{' '}
                  </h3>
                  <ul content='space' className='list-disc pl-5 mb-2'>
                    <li>
                      {' '}
                      integrate with{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/gemini.py'
                        className='text-blue-600'
                      >
                        <u>LLMs</u>
                      </Link>{' '}
                      and{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/text_to_image.py'
                        className='text-blue-600'
                      >
                        <u>other AI models</u>
                      </Link>
                      ,
                    </li>
                    <li>
                      {' '}
                      build{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/010-service/service.py'
                        className='text-blue-600'
                      >
                        <u>web services</u>
                      </Link>
                      , integrating with FastAPI,{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/text_to_image.py#L14-L129'
                        className='text-blue-600'
                      >
                        <u>Pydantic</u>
                      </Link>{' '}
                      or anything else,
                    </li>
                    <li>
                      create{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/deep_research/deep_research.py'
                        className='text-blue-600'
                      >
                        <u>agentic applications</u>
                      </Link>{' '}
                      that{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/tree/main/examples/003-python-speech-to-text'
                        className='text-blue-600'
                      >
                        <u>process media</u>,
                      </Link>
                    </li>
                    <li>
                      connect peers flexibly through{' '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/003-python-speech-to-text/run_client.py#L27-L30'
                        className='text-blue-600'
                      >
                        <u>WebSocket</u>
                      </Link>
                      {', '}
                      <Link
                        href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/server.py#L73-L98'
                        className='text-blue-600'
                      >
                        <u>WebRTC</u>
                      </Link>
                      , or any{' '}
                      <Link
                        href='https://actionengine.dev/docs/classact_1_1_wire_stream.html'
                        className='text-blue-600'
                      >
                        <u>custom transport</u>
                      </Link>
                      {', '}
                    </li>
                  </ul>
                </div>
              </div>
              <div className='w-full md:w-1/2 pr-6 pt-6 md:pt-0'>
                <div className='mb-4 text-gray-600 w-full'>
                  <h3 className='mb-3 font-semibold text-gray-800'>
                    TypeScript / Node.js 22+
                  </h3>
                  <p className='mb-2'>
                    In your project, try making a simple text chunk:
                  </p>
                  <div className='mb-2 mt-2'>
                    <CopyCommand
                      command={`import { makeTextChunk } from actionengine\n\nconsole.log(makeTextChunk('Hello, world!'))`}
                    />
                  </div>
                  <div className='mb-2 mt-4 text-gray-600 w-full'>
                    <h3 className='mb-3 font-semibold text-gray-800'>
                      Once that works, you're ready to:
                    </h3>
                    <ul content='space' className='list-disc pl-5 mb-2'>
                      <li>
                        <Link
                          href='https://github.com/google-deepmind/actionengine/blob/main/web/app/echo/page.tsx'
                          className='text-blue-600'
                        >
                          <u>communicate</u>
                        </Link>{' '}
                        with Action Engine servers,
                      </li>
                      <li>
                        orchestrate{' '}
                        <Link
                          href='https://github.com/google-deepmind/actionengine/blob/main/web/app/bidi-blobs/page.tsx'
                          className='text-blue-600'
                        >
                          <u>client-side actions</u>
                        </Link>
                        {','}
                      </li>
                      <li>
                        seamlessly handle streams of{' '}
                        <Link
                          href='https://github.com/google-deepmind/actionengine/blob/main/web/src/components/canvas/Genmedia.tsx'
                          className='text-blue-600'
                        >
                          <u>multimodal data</u>
                        </Link>{' '}
                        in the browser,
                      </li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className='flex flex-row w-full items-center flex-wrap'>
          <div className='relative  h-48 w-full py-6 my-8 sm:w-1/2 md:mb-18 order-last sm:order-first'>
            <View className='relative h-full sm:h-48 sm:w-full'>
              <Suspense fallback={null}>
                <Common color={'lightblue'} />
              </Suspense>
            </View>
          </div>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-8 md:mb-18 pr-6 sm:pl-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              ...and much more.
            </h2>
            <p className='mb-2 text-gray-600 leading-[1.5]'>
              Check out{' '}
              <Link href='https://github.com/google-deepmind/actionengine/tree/main/examples/003-python-speech-to-text'>
                <u>speech to text</u>
              </Link>{' '}
              with RealtimeSTT,{' '}
              <Link href='/bidi-blobs'>
                <u>manipulating UI agentically</u>
              </Link>{' '}
              through Gemini function calling,
            </p>{' '}
            <p className='mb-2 text-gray-600 leading-[1.5]'>
              or take a look at{' '}
              <Link
                className='text-blue-600'
                href='https://github.com/google-deepmind/actionengine/tree/main/examples/web'
              >
                <u>these very pages in React</u>
              </Link>
              , a{' '}
              <Link
                className='text-blue-600'
                href='https://github.com/google-deepmind/actionengine/tree/main/examples/010-service'
              >
                <u>Python project that integrates Action Engine with FastAPI</u>
              </Link>{' '}
              or{' '}
              <Link
                className='text-blue-600'
                href='https://github.com/google-deepmind/actionengine/tree/main/examples/000-actions'
              >
                <u>a starter C++ project</u>
              </Link>
              —and start building your own!
            </p>
          </div>
        </div>
      </div>
    </>
  )
}
