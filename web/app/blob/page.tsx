'use client'

import React from 'react'
import { Leva } from 'leva'
import { Common, View } from '@/components/canvas/View'
import dynamic from 'next/dynamic'

const GenmediaExample = dynamic(
  () =>
    import('@/components/canvas/Genmedia').then((mod) => mod.GenmediaExample),
  { ssr: false },
)

export default function Page() {
  return (
    <>
      <Leva />
      <>
        <canvas hidden id='canvas' width='1024' height='1024'></canvas>
        <div className=' flex w-fit flex-col flex-wrap items-center md:flex-row  lg:w-4/5'>
          <div className='flex w-full flex-col items-start justify-center p-12 text-center md:w-2/5 md:text-left'>
            <p className='w-full uppercase'></p>
            <h1 className='my-4 text-5xl font-bold leading-tight'>
              Action Engine x GenMedia
            </h1>
            <p className='mb-8 text-2xl leading-normal'></p>
          </div>
        </div>

        <View className='absolute top-0 flex h-screen w-full flex-col items-center justify-center'>
          <GenmediaExample />
          <Common />
        </View>
      </>
    </>
  )
}
