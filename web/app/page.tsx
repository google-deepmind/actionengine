'use client'

import dynamic from 'next/dynamic'
import { Suspense, useCallback } from 'react'
import { Common } from '@/components/canvas/View'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Blob } from '@/components/canvas/Examples'
import { useWorld } from 'koota/react'

// const Blob = dynamic(
//   () => import('@/components/canvas/Examples').then((mod) => mod.Blob),
//   { ssr: false },
// )

const View = dynamic(
  () => import('@/components/canvas/View').then((mod) => mod.View),
  {
    ssr: false,
  },
)

export default function Page() {
  const router = useRouter()
  const world = useWorld()

  const onClick = useCallback(() => {
    router.replace('/bidi-blobs')
  }, [])

  const xShift = 0.65

  return (
    <>
      <div className='mx-auto flex w-full flex-col flex-wrap items-center md:flex-row  lg:w-2/3'>
        {/* jumbo */}
        <div className='flex w-full flex-col items-start justify-center p-12 pb-6 text-center md:w-2/5 md:text-left'>
          <p className='w-full uppercase'>Interactive demo showcase</p>
          <h1 className='my-4 text-4xl font-bold leading-tight'>
            Action Engine Gallery
          </h1>
          <p className='mb-4 text-2xl leading-normal'>
            Various examples of UIs, APIs and agents built with Action Engine
          </p>
          <p className='mb-8 text-gray-600 leading-[1.5] text-xs'>
            Jump to example projects:{' '}
            <Link
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/000-actions'
            >
              <u>C++</u>
            </Link>
            ,{' '}
            <Link
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/010-service'
            >
              <u>Python</u>
            </Link>
            ,{' '}
            <Link
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/web'
            >
              <u>TypeScript</u>
            </Link>
          </p>
        </div>
        <div className='w-full text-center md:w-3/5'>
          <Suspense fallback={null}>
            <View
              className='flex h-96 w-full flex-col items-center justify-center'
              orbit
            >
              <Suspense>
                <group scale={1.5}>
                  <Blob
                    scale={0.33}
                    position={[xShift - 1.7, 0, 0]}
                    color='#4285F4'
                    onClick={onClick}
                  />
                  <Blob
                    scale={0.25}
                    position={[xShift - 1.08, -0.07, 0]}
                    color='#EA4335'
                    onClick={onClick}
                  />
                  <Blob
                    scale={0.25}
                    position={[xShift - 0.55, -0.07, 0]}
                    color='#FBBC04'
                    onClick={onClick}
                  />
                  <Blob
                    scale={[0.23, 0.3, 0.25]}
                    position={[xShift - 0.03, -0.12, 0]}
                    color='#4285F4'
                    onClick={onClick}
                  />
                  <Blob
                    scale={[0.15, 0.3, 0.25]}
                    position={[xShift + 0.41, -0.03, 0]}
                    color='#34A853'
                    onClick={onClick}
                  />
                  <Blob
                    scale={0.25}
                    position={[xShift + 0.88, -0.07, 0]}
                    color='#EA4335'
                    onClick={onClick}
                  />
                </group>
              </Suspense>
              <Common />
            </View>
          </Suspense>
        </div>
      </div>

      <div className='mx-auto flex w-full flex-col flex-wrap items-start pl-12 pr-12 md:flex-row  lg:w-2/3'>
        {/* first row */}
        <div className='relative h-48 w-full py-6 sm:w-1/2 md:my-8 md:mb-18 pr-6'>
          <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
            First-class streaming and multimodality
          </h2>
          <p className='mb-2 text-gray-600'>
            Send streams of data asynchronously over a single wire connection,
            mixing text, images, audio and your custom types.
          </p>
          <p className='mb-8 text-gray-600'>
            See this in action in a{' '}
            <Link href='/blob' className='text-blue-400'>
              <u>text-to-image</u>
            </Link>{' '}
            generation demo with live progress updates.
          </p>
        </div>
        <div className='relative my-8 h-48 w-full py-6 sm:w-1/2 md:mb-18'>
          <View orbit className='relative h-full  sm:h-48 sm:w-full'>
            <Suspense fallback={null}>
              <Common color={'lightpink'} />
            </Suspense>
          </View>
        </div>
        {/* second row */}
        <div className='relative  h-48 w-full py-6 my-8 sm:w-1/2 md:mb-18'>
          <View orbit className='relative h-full sm:h-48 sm:w-full'>
            <Suspense fallback={null}>
              <Common color={'lightgreen'} />
            </Suspense>
          </View>
        </div>
        <div className='w-full p-6 sm:w-1/2 my-8'>
          <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
            Dedicated I/O channels, selective persistence
          </h2>
          <p className='mb-2 text-gray-600'>
            <Link href='/gemini?q=ollama' className='text-blue-400'>
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
        <div className='relative h-48 w-full py-6 sm:w-1/2 md:my-8 md:mb-12'>
          <h2 className='mb-3 text-2xl font-bold leading-[1.15] text-gray-800'>
            Observable workflows
          </h2>
          <p className='mb-2 text-gray-600 pr-6'>
            See an agent{' '}
            <Link href='/deepresearch?q=alpha-demos' className='text-blue-400'>
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
          <View orbit className='relative h-full  sm:h-48 sm:w-full'>
            <Suspense fallback={null}>
              <Common color={'lightblue'} />
            </Suspense>
          </View>
        </div>
        <div className='relative  h-48 w-full py-6 my-8 sm:w-1/2 md:mb-18'>
          <View orbit className='relative h-full sm:h-48 sm:w-full'>
            <Suspense fallback={null}>
              <Common color={'lightblue'} />
            </Suspense>
          </View>
        </div>
        <div className='w-full p-6 sm:w-1/2 my-8'>
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
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/web'
            >
              <u>this React app</u>
            </Link>
            , a{' '}
            <Link
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/010-service'
            >
              <u>Python project that integrates Action Engine with FastAPI</u>
            </Link>{' '}
            or{' '}
            <Link
              className='text-blue-400'
              href='https://github.com/google-deepmind/actionengine/tree/main/examples/000-actions'
            >
              <u>a starter C++ project</u>
            </Link>{' '}
            and start building your own!
          </p>
        </div>
      </div>
    </>
  )
}
