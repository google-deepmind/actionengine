import { ThreeElements } from '@react-three/fiber'

declare module '*.vert' {
  const content: string
  export default content
}

declare module '*.frag' {
  const content: string
  export default content
}

declare global {
  namespace React {
    namespace JSX {
      interface IntrinsicElements extends ThreeElements {}
    }
  }
}
