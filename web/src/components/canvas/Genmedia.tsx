'use client'

import {
  Action,
  encodeBaseModelMessage,
  makeBlobFromChunk,
} from '@helenapankov/actionengine'

import { OrbitControls, useTexture } from '@react-three/drei'
import { makeAction } from '@/helpers/actionengine'
import * as THREE from 'three'
import React, {
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from 'react'
import { useCursor } from '@react-three/drei'
import { ActionRegistry, makeTextChunk } from '@helenapankov/actionengine'
import { levaStore, useControls } from 'leva'
import { trait } from 'koota'
import { useWorld } from 'koota/react'
import { useTrait, useTraitEffect } from 'koota/react'
import {
  GlobalUIState,
  Material,
  Mesh,
  Position,
  Rotation,
  Scale,
} from '@/helpers/genmedia/traits'
import { GlobalCursor } from '@/components/canvas/Cursor'
import { getRoundedPlaneShape } from '@/helpers/genmedia/primitives'
import {
  DrawingCanvas,
  DrawingCanvasController,
} from '@/components/canvas/DrawingCanvas'
import { SpecialInputs } from 'leva/plugin'
import { ActionEngineContext } from '@/helpers/actionengine'

const IsSelected = trait()

const kDefaultTextureUrl =
  'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mOMdtrxHwAEqwJW7xjHFQAAAABJRU5ErkJggg=='

const TextureUrl = trait(() => {
  return {
    url: kDefaultTextureUrl,
  }
})

const Progress = trait(() => {
  return {
    progress: 0,
  }
})

const registerGenerateContentAction = (registry: ActionRegistry) => {
  registry.register(
    'generate_content',
    {
      name: 'generate_content',
      inputs: [
        { name: 'api_key', type: 'text/plain' },
        { name: 'session_token', type: 'text/plain' },
        { name: 'chat_input', type: 'text/plain' },
      ],
      outputs: [
        { name: 'output', type: 'text/plain' },
        { name: 'thoughts', type: 'text/plain' },
        { name: 'new_session_token', type: 'text/plain' },
      ],
    },
    async (_: Action) => {},
  )
}

const registerTextToImageAction = (registry: ActionRegistry) => {
  registry.register(
    'text_to_image',
    {
      name: 'text_to_image',
      inputs: [{ name: 'request', type: '__BaseModel__' }],
      outputs: [
        { name: 'progress', type: '__BaseModel__' },
        { name: 'image', type: 'image/png' },
      ],
    },
    async (_) => {},
  )
}

interface DiffusionRequest {
  prompt: string
  num_inference_steps: number
  height: number
  width: number
  seed: number
}

const makeDiffusionRequest = (params: Partial<DiffusionRequest>) => {
  return {
    prompt: 'a fox',
    num_inference_steps: kInferenceSteps,
    height: 512,
    width: 512,
    seed: Math.floor(Math.random() * 1000000),
    ...params,
  }
}

const kInferenceSteps = 25

const kCreatePromptPrompt = `surprise me with a prompt to generate a really nice image. keep it short. only use small letters and no punctuation. only output one prompt. do NOT think much. --nothink`

const useBlobControls = () => {
  const world = useWorld()
  if (!world) {
    throw new Error('World is not available')
  }
  const [actionEngine] = useContext(ActionEngineContext)

  return useControls(
    '',
    () => {
      const get = levaStore.get
      const set = levaStore.set
      return {
        prompt: { value: 'a fox', label: 'Prompt', rows: 5 },
        position: {
          value: { x: -1, y: 1 },
          label: 'Position',
          step: 0.02,
          disabled: false,
        },
        progress: {
          value: 0,
          min: 0,
          max: kInferenceSteps,
          step: 1,
          disabled: true,
          label: 'Progress',
        },
        createPrompt: {
          label: 'Create Prompt',
          onClick: async () => {
            set({ prompt: 'Generating...' }, false)
            const action = makeAction('generate_content', actionEngine)
            await action.call()

            const apiKeyNode = await action.getInput('api_key')
            await apiKeyNode.putAndFinalize(makeTextChunk('ollama'))

            const sessionTokenNode = await action.getInput('session_token')
            await sessionTokenNode.putAndFinalize(makeTextChunk(''))

            const chatInputNode = await action.getInput('chat_input')
            await chatInputNode.putAndFinalize(
              makeTextChunk(kCreatePromptPrompt),
            )

            const outputNode = await action.getOutput('output')
            const iterateOutput = async () => {
              outputNode.setReaderOptions(
                /* ordered */ true,
                /* removeChunks */ true,
                /* timeout */ -1,
              )
              const textDecoder = new TextDecoder('utf-8')
              let first = true
              for await (const chunk of outputNode) {
                const text = textDecoder.decode(chunk.data)
                if (first && text) {
                  first = false
                  set({ prompt: text }, false)
                  continue
                }
                set({ prompt: get('prompt') + text }, false)
              }
            }
            iterateOutput().then()
          },
          type: SpecialInputs.BUTTON,
          settings: {
            disabled: false,
          },
        },
        generate: {
          label: 'Generate',
          onClick: async () => {
            const action = makeAction('text_to_image', actionEngine)
            await action.call()

            const requestNode = await action.getInput('request')
            await requestNode.putAndFinalize({
              metadata: { mimetype: '__BaseModel__' },
              data: encodeBaseModelMessage(
                'actions.text_to_image.DiffusionRequest',
                makeDiffusionRequest({ prompt: get('prompt') }),
              ),
            })

            const selectedEntity = world.queryFirst(IsSelected)
            if (!selectedEntity) {
              console.warn('No entity selected to update texture')
              return
            }

            selectedEntity.set(TextureUrl, { url: kDefaultTextureUrl })

            const fetchImage = async () => {
              const imageNode = await action.getOutput('image')
              imageNode.setReaderOptions(
                /* ordered */ true,
                /* removeChunks */ true,
                /* timeout */ -1,
              )

              try {
                for await (let chunk of imageNode) {
                  const url = URL.createObjectURL(makeBlobFromChunk(chunk))
                  if (!selectedEntity.has(TextureUrl)) {
                    return
                  }
                  selectedEntity.set(TextureUrl, { url })
                  set({ progress: kInferenceSteps }, false)
                  selectedEntity.set(Progress, { progress: kInferenceSteps })
                }
              } catch (error) {
                console.error('Error fetching image:', error)
              }
            }
            fetchImage().then()

            const fetchProgress = async () => {
              const progressNode = await action.getOutput('progress')
              progressNode.setReaderOptions(
                /* ordered */ true,
                /* removeChunks */ true,
                /* timeout */ -1,
              )

              let progress = 0

              try {
                for await (const chunk of progressNode) {
                  progress += 1
                  if (progress >= kInferenceSteps) {
                    return
                  }
                  set({ progress }, false)
                  selectedEntity.set(Progress, { progress })
                }
              } catch (error) {
                console.error('Error fetching progress:', error)
              }
            }

            Promise.all([fetchImage(), fetchProgress()]).catch((error) => {
              console.error('Error in generating image:', error)
            })
          },
          type: SpecialInputs.BUTTON,
          settings: {
            disabled: false,
          },
        },
      }
    },
    [actionEngine],
  )
}

export const GenmediaExample = () => {
  const world = useWorld()
  useEffect(() => {
    if (!world) {
      return
    }
    const entity = world.spawn(
      GlobalCursor,
      Position,
      Rotation,
      Mesh,
      Material,
      Scale,
    )
    entity.set(Position, { x: 0, y: 0, z: 0 })
    world.add(GlobalUIState)
    return () => {
      world.remove(GlobalUIState)
    }
  }, [world])

  const [actionEngine] = useContext(ActionEngineContext)
  useEffect(() => {
    if (!actionEngine || !actionEngine.actionRegistry) {
      return
    }
    registerTextToImageAction(actionEngine.actionRegistry)
    registerGenerateContentAction(actionEngine.actionRegistry)
  }, [actionEngine])

  const meshRef = useRef<THREE.Mesh>(new THREE.Mesh())

  const [imageEntity, setImageEntity] = useState(null)

  useEffect(() => {
    if (!world) {
      return
    }
    setImageEntity(world.spawn())
  }, [world])

  useEffect(() => {
    if (!imageEntity) {
      return
    }
    imageEntity.add(Mesh)
    imageEntity.set(Mesh, meshRef.current)
    imageEntity.add(Position)
    imageEntity.set(Position, { x: -0.5, y: 0.5, z: 0 })
    imageEntity.add(Scale)
    imageEntity.set(Scale, { x: 1, y: 1, z: 1 })
    imageEntity.add(IsSelected)
    imageEntity.add(TextureUrl)
    imageEntity.set(TextureUrl, {
      url: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mOMdtrxHwAEqwJW7xjHFQAAAABJRU5ErkJggg==',
    })
    imageEntity.add(Progress)
    imageEntity.set(Progress, { progress: 0 })

    return () => {
      imageEntity.destroy()
      setImageEntity(world.spawn())
    }
  }, [world, imageEntity])

  const [hovered, hover] = useState(false)
  useCursor(hovered, 'none', 'auto')

  // const cursorEntity = useQueryFirst(GlobalCursor)
  // useTraitEffect(cursorEntity, Position, (position) => {
  //   if (!position) {
  //     return
  //   }
  // })

  const canvasRef = useRef<DrawingCanvasController>(null)
  useEffect(() => {
    return () => {
      if (canvasRef.current) {
        canvasRef.current.entity.destroy()
        canvasRef.current = null
      }
    }
  }, [world])
  canvasRef.current?.entity.set(Position, { x: -0.5, y: -0.5, z: 0 })

  const [controls, setControl, getControl] = useBlobControls()

  const generatePromptCallback = useCallback(async () => {
    if (
      !actionEngine.stream ||
      !actionEngine.actionRegistry.definitions.has('generate_content')
    ) {
      return
    }
    setControl({ prompt: 'Generating...' })
    const action = makeAction('generate_content', actionEngine)
    await action.call()

    const apiKeyNode = await action.getInput('api_key')
    await apiKeyNode.putAndFinalize(makeTextChunk('ollama'))

    const sessionTokenNode = await action.getInput('session_token')
    await sessionTokenNode.putAndFinalize(makeTextChunk(''))

    const chatInputNode = await action.getInput('chat_input')
    await chatInputNode.putAndFinalize(
      makeTextChunk(
        'surprise me with a prompt to generate a really nice image. keep it short. only use small letters and no punctuation. only output one prompt. do NOT think much. This prompt MUST be a simple description. --nothink',
      ),
    )

    const outputNode = await action.getOutput('output')
    const iterateOutput = async () => {
      outputNode.setReaderOptions(
        /* ordered */ true,
        /* removeChunks */ true,
        /* timeout */ -1,
      )
      const textDecoder = new TextDecoder('utf-8')
      let first = true
      for await (const chunk of outputNode) {
        const text = textDecoder.decode(chunk.data)
        if (first && text) {
          first = false
          setControl({ prompt: text })
          continue
        }
        setControl({ prompt: getControl('prompt') + text })
      }
    }
    iterateOutput().then()
  }, [actionEngine])

  useEffect(() => {
    if (
      !actionEngine ||
      !actionEngine.stream ||
      !actionEngine.actionRegistry.definitions.has('generate_content')
    ) {
      return
    }
    let cancelled = false
    actionEngine.stream.waitUntilReady().then(() => {
      if (cancelled) {
        return
      }
      console.log('Generating initial prompt', actionEngine)
      generatePromptCallback().then()
    })
    return () => {
      cancelled = true
    }
  }, [actionEngine])

  useEffect(() => {
    if (imageEntity == null || !controls.position) {
      return
    }
    const mesh = imageEntity.get(Mesh)
    mesh.position.x = controls.position.x
    mesh.position.y = -controls.position.y
  }, [controls.position, imageEntity])

  const [textureUrl, setTextureUrl] = useState(kDefaultTextureUrl)
  const textureUrlFromTrait =
    useTrait(imageEntity, TextureUrl)?.url || kDefaultTextureUrl
  useEffect(() => {
    setTextureUrl(textureUrlFromTrait)
  }, [textureUrlFromTrait])

  const texture = useTexture(textureUrl)

  const progress = useTrait(imageEntity, Progress)?.progress || 0

  const [orbit, setOrbit] = useState(true)
  useTraitEffect(world, GlobalUIState, (uiState) => {
    if (!uiState) {
      return
    }
    setOrbit(!uiState.orbitingBlocked)
  })

  return (
    <>
      <group>
        {orbit && <OrbitControls />}
        <mesh ref={meshRef} scale={[2, 2, 2]}>
          <shapeGeometry args={[getRoundedPlaneShape(1, 1, 0.05)]} />
          <meshBasicMaterial
            color='#ffffff'
            side={THREE.DoubleSide}
            map={texture}
            wireframe={progress > 0 && textureUrl == kDefaultTextureUrl}
          />
        </mesh>

        <DrawingCanvas ref={canvasRef} width={1024} height={1024} disabled />
      </group>
    </>
  )
}
