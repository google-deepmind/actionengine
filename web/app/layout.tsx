import { Layout } from '@/components/dom/Layout'
import '@/global.css'
import { Suspense } from 'react'

export const metadata = {
  title: 'Action Engine Demos',
  description:
    'A toolkit for building and shipping AI-powered applications with ease and efficiency.',
}

export default function RootLayout({ children }) {
  return (
    <html lang='en' className='antialiased'>
      {/*
          <head /> will contain the components returned by the nearest parent
          head.tsx. Find out more at https://beta.nextjs.org/docs/api-reference/file-conventions/head
        */}
      <head />
      <body>
        <Suspense>
          {/* To avoid FOUT with styled-components wrap Layout with StyledComponentsRegistry https://beta.nextjs.org/docs/styling/css-in-js#styled-components */}
          <Layout>{children}</Layout>
        </Suspense>
      </body>
    </html>
  )
}
