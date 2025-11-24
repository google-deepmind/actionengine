'use client'

import dynamic from 'next/dynamic'
import { common, createStarryNight } from '@wooorm/starry-night'
import '@/helpers/codeStyle.css'
import { toHtml } from 'hast-util-to-html'
import { Suspense, useEffect, useState } from 'react'
import { Common } from '@/components/canvas/View'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Blob } from '@/components/canvas/Examples'
import { useWorld } from 'koota/react'
import { Check, Clipboard } from 'lucide-react'
import Image from 'next/image'

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

const HighlightedCode = ({
  language,
  starryNightInstance,
  children,
  ...props
}) => {
  if (!starryNightInstance) {
    return <pre>{children}</pre>
  }
  const scope = starryNightInstance.flagToScope(language)
  const tree = starryNightInstance.highlight(children, scope)
  const html = toHtml(tree)
  return <pre {...props} dangerouslySetInnerHTML={{ __html: html }} />
}

export default function Page() {
  const world = useWorld()

  const [starryNightInstance, setStarryNightInstance] = useState(null)
  useEffect(() => {
    let cancel = false
    createStarryNight(common).then((instance) => {
      if (!cancel) {
        setStarryNightInstance(instance)
      }
    })
    return () => {
      cancel = true
    }
  }, [])

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
            <div className='flex flex-row flex-wrap mb-2 items-start'>
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
{/*            <HighlightedCode*/}
{/*              language='python'*/}
{/*              className='w-full mb-6'*/}
{/*              starryNightInstance={starryNightInstance}*/}
{/*            >*/}
{/*              {`*/}
{/*import actionengine*/}
{/*actionengine.to_chunk("Hello, world!")*/}
{/*a = True*/}
{/*`}*/}
{/*            </HighlightedCode>*/}
          </div>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
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
        <div className='flex flex-row w-full items-start flex-wrap'>
          <h1 className='mb-3 text-4xl font-bold leading-[1.15] text-gray-800'>
            Key features and demos
          </h1>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 pr-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              First-class streaming and multimodality
            </h2>
            <p className='mb-2 text-gray-600'>
              Send streams of data asynchronously over a single wire connection,
              mixing text, images, audio and your custom types.
            </p>
            <p className='mb-2 text-gray-600'>
              Avoid manually chunking and reassembling data, and focus on
              building your application logic.
            </p>
            <p className='mb-2 text-gray-600'>
              Simply drop your data into an action's input node, and read
              processed data from its output node as it arrives.
            </p>
            <p className='mb-2 text-gray-600'>
              Reading works like a channel with a timeout, so you can always
              decide how long to wait for new data. Writing does not block, so
              you don't have to worry about tight loops.
            </p>
            <p className='mb-8 text-gray-600'>
              See this in action in a{' '}
              <Link href='/blob' className='text-blue-600'>
                <u>text-to-image</u>
              </Link>{' '}
              generation demo with live progress updates, and{' '}
              <Link
                href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/text_to_image.py'
                className='text-blue-600'
              >
                <u>see the code</u>
              </Link>
              .
            </p>
          </div>
          <Suspense>
            <Image
              src='/genmedia.gif'
              alt='Text-to-image generation'
              width={600}
              height={400}
              className='relative my-8 h-full w-full py-6 sm:w-1/2'
            />
          </Suspense>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
          <Suspense>
            <Image
              src='/ollama.gif'
              alt='Stateful LLM chat with a thought stream'
              width={600}
              height={400}
              className='relative h-full w-full py-6 my-8 sm:w-1/2 order-last sm:order-first'
            />
          </Suspense>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 pr-6 sm:pl-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Granular, abundant streams, persisted on demand
            </h2>
            <p className='mb-2 text-gray-600'>
              With named streams, have as many intermediate logs, thoughts, or
              other data as you need, without cluttering the main data flow.
            </p>
            <p className='mb-2 text-gray-600'>
              Streams can be local, shared between peers, or distributed—per
              individual stream, not in one storage for the whole application.
            </p>
            <p className='mb-2 text-gray-600'>
              Bring your own storage backends, or use the built-in in-memory and
              Redis Streams stores.
            </p>
            <p className='mb-8 text-gray-600'>
              <Link href='/gemini?q=ollama' className='text-blue-600'>
                <u>Chat with an LLM</u>
              </Link>
              &nbsp;seeing its thought process as a separate stream, retrieve
              history later with a session token, and{' '}
              <Link
                href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/gemini.py'
                className='text-blue-600'
              >
                <u>see the code</u>
              </Link>
              .
            </p>
          </div>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 md:mb-12'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Observable workflows
            </h2>
            <p className='mb-2 text-gray-600 pr-6'>
              Tap into intermediate steps, logs and thoughts as they happen.
            </p>
            <p className='mb-8 text-gray-600 pr-6'>
              See an agent{' '}
              <Link
                href='/deepresearch?q=alpha-demos'
                className='text-blue-600'
              >
                <u>research a topic</u>
              </Link>{' '}
              by making a plan, running web searches in multiple investigative
              actions, and compiling a final report, and{' '}
              <Link
                href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/deep_research/deep_research.py'
                className='text-blue-600'
              >
                <u>see the code</u>
              </Link>
              .
            </p>
          </div>
          <Suspense>
            <Image
              src='/planning.png'
              alt='Planning and executing a research task'
              width={600}
              height={400}
              className='relative my-8 h-full w-full py-6 sm:w-1/2'
            />
          </Suspense>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
          <Suspense>
            <Image
              src='/bidi-blobs.gif'
              alt='Stateful LLM chat with a thought stream'
              width={600}
              height={400}
              className='relative h-full w-full py-6 my-8 sm:w-1/2 order-last sm:order-first'
            />
          </Suspense>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 pr-6 sm:pl-6'>
            <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
              Interactions beyond just servers and clients
            </h2>
            <p className='mb-2 text-gray-600'>
              Actions can run anywhere: in backend services, browser clients,
              edge devices, or model servers.
            </p>
            <p className='mb-2 text-gray-600'>
              Use bidirectional wire streams to have clients and servers
              communicate flexibly, sending streams of data and action calls
              both ways.
            </p>
            <p className='mb-2 text-gray-600'>
              Build peer-to-peer applications using WebRTC data channels as
              transports.
            </p>
            <p className='mb-8 text-gray-600'>
              See Gemini-powered UI agentically{' '}
              <Link href='/bidi-blobs' className='text-blue-600'>
                <u>manipulate three blobs</u>
              </Link>{' '}
              and color them on your command, and{' '}
              <Link
                href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/actions/gemini_fc.py'
                className='text-blue-600'
              >
                <u>see the code</u>
              </Link>
              .
            </p>
          </div>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap'>
          <div className='relative  h-48 w-full py-6 my-8 sm:w-1/2 order-last sm:order-first'>
            <View className='relative h-full sm:h-48 sm:w-full'>
              <Suspense fallback={null}>
                <Common color={'lightblue'} />
              </Suspense>
            </View>
          </div>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 pr-6 sm:pl-6'>
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
                href='https://github.com/google-deepmind/actionengine/tree/main/web'
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
        <div className='flex flex-row w-full items-start flex-wrap'>
          <div className='relative h-fit w-full py-6 sm:w-1/2 md:my-6 pr-6'>
            <h2
              className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'
              id='why-another-framework'
            >
              Why another framework?
            </h2>
            <p className='mb-2 text-gray-600'>
              <i>Framework</i> is a loaded word, but it is hard to find anything
              better and still have people understand what the project is about.
              The problem of <i>“agentic frameworks”</i> is that they give
              excessively rigid abstractions. The novel challenge is not to{' '}
              <i>define</i> “agents”. They are just chains of calls in some
              distributed context.
            </p>
            <p className='mb-2 text-gray-600'>
              The actual novel challenge is to{' '}
              <i>cultivate a common language</i> to express highly dynamic,
              highly experimental interactions performantly (and safely!) in
              very different kinds of applications and environments—and{' '}
              <i>build tools</i> to do so. In other words, the challenge is to
              acknowledge and enable the diversity of applications and contexts
              code runs from.
            </p>
            <h2
              className='mt-6 mb-3 text-2xl font-bold leading-[1.15] text-gray-800'
              id='observations-and-principles'
            >
              Observations and principles
            </h2>
            <h3 className='mb-2 text-gray-600 underline decoration-dotted'>
              Developers don't really need <i>“loop runner”</i> type frameworks
              with tight abstractions, but rather a set of thin layers they can
              combine to:
            </h3>
            <ul content='space' className='list-disc pl-5 mb-6 text-gray-600'>
              <li className='mb-2'>
                relieve “daily”, “boring” issues (e.g. serialisation of custom
                types, chaining tasks),
              </li>
              <li className='mb-2'>
                have consistent, similar ways to store and transmit state and
                express agentic behaviour across backend peers, browser clients,
                model servers etc. (edge devices even),
              </li>
              <li className='mb-2'>
                “productionise”: serve, scale, authorise, discover.
              </li>
            </ul>
            <h3 className='mb-6 text-gray-600 underline decoration-dotted'>
              It is important to design such tools and frameworks at the full
              stack to enable builders of all types of apps: web/native, client
              orchestration or a worker group in a cluster, and everything in
              between.
            </h3>
            <h3 className='mb-6 text-gray-600 underline decoration-dotted'>
              Data representation, storage and transport matter much more than
              the runtime/execution context.
            </h3>
            <p className='mb-2 text-gray-600'>
              Obligatory opt-in into heavy abstractions or dense, non-obvious
              runtimes can solve some problems in one ecosystem, but often at
              the cost of being unusable in another, therefore hindering
              innovation and interoperability.
            </p>
            <p className='mb-2 text-gray-600 font-semibold'>
              Applications dictate the abstractions and shape runtimes, and
              frameworks should adapt to them, not the other way round.
            </p>
          </div>
          <div className='relative h-full w-full md:my-6 py-6 sm:w-1/2'>
            <h2
              className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'
              id='what-is-action-engine'
            >
              What is Action Engine?
            </h2>
            <p className='mb-2 text-gray-600'>
              Action Engine is a kit of <i>optional</i> components, for
              different needs of different applications. That makes it stand out
              from other frameworks: they lock you in the whole set of
              abstractions, which you might not need.
            </p>
            <p className='mb-2 text-gray-600'>
              We, early supporters at Google and DeepMind, want a flexible,
              quick-feedback way to navigate the fast-evolving landscape of AI
              infrastructure. Current frameworks solidified around abstractions
              that are growing increasingly inadequate for the new demands of
              multimodal, streaming, stateful, long-running, agentic
              applications.
            </p>
            <p className='mb-6 text-gray-600 font-semibold'>
              Action Engine aims to be a common building block for these new
              kinds of applications, without imposing rigid abstractions or
              heavy dependencies.
            </p>
            <h2
              className='mt-6 mb-3 text-2xl font-bold leading-[1.15] text-gray-800'
              id='architecture-overview'
            >
              Architecture overview
            </h2>
            <p className='mb-2 text-gray-600'>
              At its core, Action Engine provides{' '}
              <u className='decoration-dotted'>actions</u> and{' '}
              <u className='decoration-dotted'>async nodes</u>.
            </p>
            <p className='mb-2 text-gray-600'>
              <u className='decoration-dotted'>Action</u> is simple: it's just
              executable code with a name and i/o schema assigned, and some
              well-defined behaviour to prepare and clean up.
            </p>
            <p className='mb-2 text-gray-600'>
              <u className='decoration-dotted'>Async node</u> is more
              interesting: it is a logical “stream” of data: a channel-like
              interface that one party (or parties!) can write into, and another
              can read with a{' '}
              <Link
                href='https://actionengine.dev/docs/classact_1_1_chunk_store.html'
                className='text-blue-600'
              >
                <u>“block with timeout” semantic</u>
              </Link>
              .
            </p>
            <p className='mb-2 text-gray-600'>
              These core concepts are easy to understand, but <i>powerful</i>.
              Unlike with loaded terms like “agent”, “context” or “graph
              executor”, you won't make any big mistake thinking about{' '}
              <u className='decoration-dotted'>actions</u> as about functions,
              and about <u className='decoration-dotted'>async nodes</u> as
              about channels or queues that go as inputs and outputs to those
              functions.
            </p>
            <p className='mb-2 text-gray-600'>
              They are <b>familiar</b>, yet flexible building blocks to express
              complex, dynamic, streaming, multimodal workflows.
            </p>
            <p className='mb-2 text-gray-600'>
              The rest of the library simply cares about building context to run
              or call actions, but leaves it up to you how to compose them,
              store their state, or transport their data. There are components
              to help you with that, but you can also build your own—for{' '}
              <Link
                href='https://actionengine.dev/docs/classact_1_1_session.html'
                className='text-blue-600'
              >
                <u>sessions</u>
              </Link>
              ,{' '}
              <Link
                href='https://actionengine.dev/docs/classact_1_1_service.html'
                className='text-blue-600'
              >
                <u>services</u>
              </Link>
              ,{' '}
              <Link
                href='https://actionengine.dev/docs/classact_1_1_wire_stream.html'
                className='text-blue-600'
              >
                <u>transports</u>
              </Link>
              ,{' '}
              <Link
                href='https://github.com/google-deepmind/actionengine/blob/main/examples/007-python-generative-media/server.py'
                className='text-blue-600'
              >
                <u>servers</u>
              </Link>
              , and more (these docs will get filled out over time!).
            </p>
          </div>
        </div>
        <div className='flex flex-row w-full items-start flex-wrap mt-4'>
          <p className='mb-4 text-gray-500 text-sm'>
            Action Engine is an open source project licensed under the{' '}
            <Link
              className='text-blue-600'
              href='https://www.apache.org/licenses/LICENSE-2.0'
            >
              <u>Apache 2.0 License</u>
            </Link>{' '}
            and released by Google DeepMind. The source code is hosted on GitHub
            at{' '}
            <Link
              className='text-blue-600'
              href='https://github.com/google-deepmind/actionengine'
            >
              <u>google-deepmind/actionengine</u>
            </Link>
            . Static non-code assets are provided under the{' '}
            <Link
              className='text-blue-600'
              href='https://creativecommons.org/licenses/by/4.0/'
            >
              <u>Creative Commons Attribution 4.0 International License</u>
            </Link>
            .
          </p>
          <p className='mb-4 text-gray-500 text-sm'>
            This page, demos, and any other resources or services provided at
            actionengine.dev are provided “as is” by individual maintainers, not
            by or on behalf of Google or its affiliates, and without warranty of
            any kind.
          </p>
          <p className='mb-4 text-gray-500 text-sm'>
            Unless explicitly stated otherwise, Google and Google DeepMind do
            not endorse or support any third party services, products, or
            applications mentioned, demonstrated, or linked to from this page.
          </p>
        </div>
      </div>
    </>
  )
}
