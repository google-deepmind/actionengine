'use client'

import { MeshDistortMaterial } from '@react-three/drei'
import { useRef, useState } from 'react'
import { useCursor } from '@react-three/drei'
import * as THREE from 'three'
import { useFrame } from '@react-three/fiber'

export const Blob = ({ ...props }) => {
  const [hovered, hover] = useState(false)
  useCursor(hovered)

  const meshRef = useRef<THREE.Mesh>(null)
  useFrame((_, delta) => {
    if (meshRef.current) {
      meshRef.current.position.set(
        props.position?.[0] || 0,
        props.position?.[1] || 0,
        props.position?.[2] || 0,
      )
      // meshRef.current.rotation.x += delta * 0.1
    }
  })

  return (
    <mesh
      onClick={() => props.onClick?.()}
      onPointerOver={() => hover(true)}
      onPointerOut={() => hover(false)}
      ref={meshRef}
      {...props}
    >
      <sphereGeometry args={[1, 64, 64]} />
      <MeshDistortMaterial
        roughness={0.5}
        color={hovered ? 'hotpink' : props.color}
        distort={0.4}
        speed={2}
      />
    </mesh>
  )
}
