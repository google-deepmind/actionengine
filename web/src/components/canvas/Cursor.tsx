import React, { useEffect, useImperativeHandle, useRef, useState } from 'react'
import { Entity, trait } from 'koota'
import * as THREE from 'three'
import { useTraitEffect } from 'koota/react'
import { useWorld } from 'koota/react'
import {
  Material,
  Mesh,
  Position,
  Rotation,
  IsVisible,
  IsActive,
  Scale,
} from '@/helpers/genmedia/traits'

export declare interface CursorProps {
  radius?: number
  children?: React.ReactNode
  ref?: React.Ref<Entity>
}

export const GlobalCursor = trait()

export const Cursor: React.FC<CursorProps> = React.forwardRef(
  (props: CursorProps, ref: React.Ref<Entity>) => {
    const entityRef = useRef<Entity>(null)
    const meshRef = useRef<THREE.Mesh>(null)
    const materialRef = useRef<THREE.MeshBasicMaterial>(null)

    const world = useWorld()

    if (entityRef.current === null) {
      entityRef.current = world.spawn(Position, Rotation, Scale, Mesh, Material)
    }
    useImperativeHandle(ref, () => {
      return entityRef.current
    }, [entityRef])

    useEffect(() => {
      const entity = entityRef.current
      if (
        entity == null ||
        meshRef.current === null ||
        materialRef.current === null
      ) {
        return
      }

      entity.set(Mesh, meshRef.current)
      entity.set(Material, materialRef.current)
      entity.add(IsVisible)
      entity.add(IsActive)
    }, [])

    const entity = entityRef.current

    useTraitEffect(entity, IsVisible, (isVisible) => {
      meshRef.current.visible = !!isVisible
    })

    useTraitEffect(entity, Position, (position) => {
      if (!entity) {
        return
      }
      console.log('.has in Cursor')
      if (!entity.has(Mesh)) {
        return
      }

      const mesh = entity.get(Mesh)
      mesh.position.x = position.x
      mesh.position.y = position.y
      mesh.position.z = position.z
    })

    const [radius, setRadius] = useState(props.radius)
    useTraitEffect(entity, Scale, (scale) => {
      setRadius(scale.x)
    })

    return (
      <mesh ref={meshRef}>
        <sphereGeometry args={[radius, 32, 32]} />
        <meshBasicMaterial
          color='white'
          transparent
          opacity={0.5}
          ref={materialRef}
        />
      </mesh>
    )
  },
)
Cursor.displayName = 'Cursor'
