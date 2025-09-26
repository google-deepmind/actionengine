'use client'

import {
  forwardRef,
  Suspense,
  useEffect,
  useImperativeHandle,
  useRef,
} from 'react'
import {
  OrbitControls,
  PerspectiveCamera,
  View as ViewImpl,
} from '@react-three/drei'
import { Three } from '@/helpers/components/Three'
import { useThree, RootState } from '@react-three/fiber'

interface CommonProps {
  color?: string
  setR3fState?: (state: RootState) => void
}

export const Common = ({ color, setR3fState }: CommonProps) => {
  const three = useThree()
  if (setR3fState) {
    setR3fState(three)
  }

  return (
    <Suspense fallback={null}>
      {color && <color attach='background' args={[color]} />}
      <ambientLight />
      <pointLight position={[20, 30, 10]} intensity={3} decay={0.2} />
      {/*<pointLight position={[-10, -10, -10]} color='blue' decay={0.2} />*/}
      <PerspectiveCamera makeDefault fov={40} position={[0, 0, 6]} />
    </Suspense>
  )
}

interface ViewProps extends React.HTMLAttributes<HTMLDivElement> {
  orbit?: boolean
  children?: React.ReactNode
}

const View = forwardRef(({ children, ...props }: ViewProps, ref) => {
  const localRef = useRef(null)
  useImperativeHandle(ref, () => localRef.current)

  const { orbit, ...rest } = props

  return (
    <>
      <div ref={localRef} {...rest} />
      <Three>
        <ViewImpl track={localRef}>
          {children}
          {props.orbit && <OrbitControls />}
        </ViewImpl>
      </Three>
    </>
  )
})
View.displayName = 'View'

export { View }
