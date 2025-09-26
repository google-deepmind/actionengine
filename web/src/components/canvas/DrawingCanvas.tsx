'use client'

import { createActions, Entity, trait } from 'koota'
import React, { useEffect, useImperativeHandle, useRef, useState } from 'react'
import * as THREE from 'three'
import {
  useActions,
  useQueryFirst,
  useTrait,
  useTraitEffect,
  useWorld,
} from 'koota/react'
import {
  GlobalUIState,
  IsActive,
  IsVisible,
  Mesh,
  Position,
  Rotation,
  Scale,
} from '@/helpers/genmedia/traits'
import { GlobalCursor } from '@/components/canvas/Cursor'
import { getRoundedPlaneShape } from '@/helpers/genmedia/primitives'
import { useCursor } from '@react-three/drei'

const encodeCanvasToImageBlob = async (
  canvas: HTMLCanvasElement,
  type: string = 'image/png',
): Promise<Blob> => {
  const dataUrl = canvas.toDataURL(type)
  return await (await fetch(dataUrl)).blob()
}

export declare interface DrawingCanvasState {
  canvas: HTMLCanvasElement
  clean: boolean
  width: number
  height: number
}

export const TDrawingCanvasState = trait((): DrawingCanvasState => {
  const canvas = document.createElement('canvas')
  canvas.width = 1024
  canvas.height = 1024
  return {
    canvas,
    clean: true,
    width: 1024,
    height: 1024,
  }
})

export interface DrawingCanvasController {
  entity: Entity
  clear: () => void
  getImage: (type: string) => Promise<Blob>
  lineFromTo: (x1: number, y1: number, x2: number, y2: number) => void
}

export declare interface DrawingCanvasProps {
  width?: number
  height?: number
  disabled?: boolean
  ref?: React.Ref<DrawingCanvasController>
}

export const DrawingCanvas: React.FC<DrawingCanvasProps> = React.forwardRef(
  (props: DrawingCanvasProps, ref: React.Ref<DrawingCanvasController>) => {
    const entityRef = useRef<Entity>(null)
    const canvasRef = useRef<HTMLCanvasElement>(null)
    const textureRef = useRef<THREE.CanvasTexture>(null)
    const meshRef = useRef<THREE.Mesh>(null)

    const width = props.width || 1024
    const height = props.height || 1024

    const world = useWorld()

    if (entityRef.current === null) {
      entityRef.current = world.spawn(
        TDrawingCanvasState,
        Position,
        Mesh,
        Rotation,
        IsVisible,
        IsActive,
      )
    }

    if (canvasRef.current === null) {
      canvasRef.current = entityRef.current.get(TDrawingCanvasState).canvas
    }

    const [radius, setRadius] = useState(0.01)
    const cursorEntity = useQueryFirst(GlobalCursor)
    const scale = useTrait(cursorEntity, Scale)
    useEffect(() => {
      if (!scale) {
        return
      }
      setRadius(scale.x)
    }, [scale])

    const actions = createActions((world) => ({
      clear: () => {
        const entity = entityRef.current
        if (entity == null) {
          return
        }
        const state = entity.get(TDrawingCanvasState)
        const { canvas } = state

        const ctx = canvas.getContext('2d')
        ctx.clearRect(0, 0, 1024, 1024)

        entity.set(TDrawingCanvasState, {
          ...state,
          clean: true,
        })

        if (textureRef.current !== null) {
          textureRef.current.needsUpdate = true
        }
      },
      getImage: async (type: string) => {
        const entity = entityRef.current
        if (entity == null) {
          return
        }
        const state = entity.get(TDrawingCanvasState)
        const { canvas } = state

        return await encodeCanvasToImageBlob(canvas, type)
      },
      lineFromTo: (x1: number, y1: number, x2: number, y2: number) => {
        const entity = entityRef.current
        if (entity == null) {
          return
        }
        const state = entity.get(TDrawingCanvasState)
        const { canvas } = state

        const ctx = canvas.getContext('2d')
        ctx.fillStyle = 'white'
        ctx.strokeStyle = 'white'
        ctx.lineWidth = 2048 * radius
        ctx.lineCap = 'round'
        ctx.lineJoin = 'round'

        ctx.beginPath()
        ctx.moveTo(x1, y1)
        ctx.lineTo(x2, y2)
        ctx.closePath()
        ctx.stroke()
        ctx.fill()

        entity.set(TDrawingCanvasState, {
          ...state,
          clean: false,
        })

        if (textureRef.current !== null) {
          textureRef.current.needsUpdate = true
        }
      },
    }))
    const { clear, getImage, lineFromTo } = useActions(actions)

    useImperativeHandle(ref, () => {
      return {
        entity: entityRef.current,
        clear,
        getImage,
        lineFromTo,
      }
    }, [entityRef, clear, getImage, lineFromTo])

    useEffect(() => {
      const entity = entityRef.current
      if (
        entity == null ||
        canvasRef.current === null ||
        textureRef.current === null
      ) {
        return
      }

      entity.set(Mesh, meshRef.current)
      entity.set(TDrawingCanvasState, {
        canvas: canvasRef.current,
        clean: true,
        width,
        height,
      })

      const ctx = canvasRef.current.getContext('2d')
      ctx.globalAlpha = 0.5
      ctx.globalCompositeOperation = 'xor'
      ctx.fillStyle = 'rgba(0, 0, 0, 0)'
      ctx.fillRect(0, 0, 1024, 1024)

      textureRef.current.image = canvasRef.current

      entity.add(IsVisible)
      entity.add(IsActive)
    }, [height, width])

    const cursor = useQueryFirst(GlobalCursor)

    const entity = entityRef.current
    useTraitEffect(entity, Position, (position) => {
      if (!entity) {
        return
      }
      if (!entity.has(Mesh)) {
        return
      }

      const mesh = entity.get(Mesh)
      mesh.position.x = position.x
      mesh.position.y = position.y
      mesh.position.z = position.z
    })

    const [hovered, hover] = useState(false)
    useCursor(hovered && !props.disabled, 'none', 'auto')

    const [lastUV, setLastUV] = useState<THREE.Vector2 | null>(null)

    return (
      <mesh
        ref={meshRef}
        onPointerDown={() => {
          if (props.disabled) {
            return
          }
          world.set(GlobalUIState, {
            orbitingBlocked: true,
            pointerIsDown: true,
          })
          setLastUV(null)
        }}
        onPointerOver={async () => {
          if (props.disabled) {
            return
          }
          cursor?.add(IsVisible)
          hover(true)
          const uiState = world.get(GlobalUIState)
          if (uiState.pointerIsDown) {
            world.set(GlobalUIState, { ...uiState, orbitingBlocked: true })
          }
        }}
        onPointerUp={() => {
          if (props.disabled) {
            return
          }
          world.set(GlobalUIState, {
            ...world.get(GlobalUIState),
            pointerIsDown: false,
          })
          setLastUV(null)
        }}
        onPointerOut={() => {
          if (props.disabled) {
            return
          }
          world.set(GlobalUIState, {
            ...world.get(GlobalUIState),
            orbitingBlocked: false,
          })
          hover(false)
          setLastUV(null)
        }}
        onPointerMove={(event) => {
          if (props.disabled) {
            return
          }
          cursor?.set(Position, {
            x: event.point.x,
            y: event.point.y,
            z: 0,
          })

          if (world.get(GlobalUIState)?.pointerIsDown) {
            lineFromTo(
              lastUV?.x * 1024,
              (1 - lastUV?.y) * 1024,
              event.intersections[0].uv.x * 1024,
              (1 - event.intersections[0].uv.y) * 1024,
            )
            setLastUV(event.intersections[0].uv)
          }
        }}
      >
        <shapeGeometry args={[getRoundedPlaneShape(1, 1, 0.05)]} />
        <meshBasicMaterial color='#ffffff' side={THREE.DoubleSide} transparent>
          <canvasTexture attach='map' ref={textureRef} />
        </meshBasicMaterial>
      </mesh>
    )
  },
)
DrawingCanvas.displayName = 'DrawingCanvas'
