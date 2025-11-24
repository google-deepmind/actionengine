import { Entity, unpackEntity, World } from 'koota'
import {
  Action,
  makeChunkFromBlob,
  makeTextChunk,
} from '@helenapankov/actionengine'
import {
  Color,
  IsSelected,
  PositionArray,
  PositionTrait,
} from '@/helpers/traits'
import { RootState } from '@react-three/fiber'
import { decode, encode } from '@msgpack/msgpack'
import * as THREE from 'three'

const getSelectedEntityIdsImpl = (world: World) => {
  const ids = [] as number[]
  world.query(IsSelected).forEach((entity) => {
    const { entityId } = unpackEntity(entity)
    ids.push(entityId)
  })
  return ids
}

export const getSelectedEntityIds = async (world: World, action: Action) => {
  const outputNode = await action.getOutput('response')
  const ids = getSelectedEntityIdsImpl(world)
  await outputNode.putAndFinalize({
    metadata: { mimetype: 'application/x-msgpack' },
    data: encode(ids) as Uint8Array<ArrayBuffer>,
  })

  const logs = await action.getNodeMap().getNode('logs')
  await logs.put(makeTextChunk(`RUN get_selected_entity_ids, ids=${ids}`))
}

export const GET_SELECTED_ENTITY_IDS_SCHEMA = {
  name: 'get_selected_entity_ids',
  inputs: [],
  outputs: [{ name: 'response', type: 'application/x-msgpack' }],
}

const resolveEntity = (world: World, id: number) => {
  return world.entities[id]
}

interface SetPositionRequest {
  entity_ids: Array<number>
  positions: Array<PositionArray>
}

const moveOverTime = async (
  entity: Entity,
  endPos: PositionArray,
  durationMs: number,
  fps: number,
) => {
  const startPos = entity.get(PositionTrait) || [0, 0, 0]
  const frames = (durationMs / 1000) * fps
  const frameDuration = 1000 / fps
  for (let frame = 0; frame < frames; frame++) {
    const newPos: PositionArray = [
      startPos[0] + ((endPos[0] - startPos[0]) * (frame + 1)) / frames,
      startPos[1] + ((endPos[1] - startPos[1]) * (frame + 1)) / frames,
      startPos[2] + ((endPos[2] - startPos[2]) * (frame + 1)) / frames,
    ]
    entity.set(PositionTrait, newPos)
    await new Promise((resolve) => setTimeout(resolve, frameDuration))
  }
}

export const setPosition = async (world: World, action: Action) => {
  const requestNode = await action.getInput('request')
  const requestChunk = await requestNode.next()
  const request = decode(requestChunk.data) as SetPositionRequest

  const logs = await action.getNodeMap().getNode('logs')
  await logs.put(
    makeTextChunk(
      `RUN set_position, entity_ids=${request.entity_ids}, positions=${request.positions}`,
    ),
  )

  if (request.entity_ids.length === 0) {
    // if no entity_ids provided, apply to all entities with PositionTrait
    request.entity_ids = world.query(PositionTrait).map((e) => {
      const { entityId } = unpackEntity(e)
      return entityId
    })
  }

  let positions = request.positions
  if (positions.length === 1 && request.entity_ids.length > 1) {
    // if only one position provided, apply to all entities
    positions = Array(request.entity_ids.length).fill(request.positions[0])
  }

  for (let i = 0; i < request.entity_ids.length; i++) {
    const entity = resolveEntity(world, request.entity_ids[i])
    moveOverTime(entity, positions[i], 500, 60).then()
  }
}

export const SET_POSITION_SCHEMA = {
  name: 'set_position',
  inputs: [{ name: 'request', type: 'application/x-msgpack' }],
  outputs: [],
}

interface ShiftPositionRequest {
  entity_ids: Array<number>
  offsets: Array<PositionArray>
}

export const shiftPosition = async (world: World, action: Action) => {
  const requestNode = await action.getInput('request')
  const requestChunk = await requestNode.next()
  const request = decode(requestChunk.data) as ShiftPositionRequest

  const logs = await action.getNodeMap().getNode('logs')
  await logs.put(
    makeTextChunk(
      `RUN shift_position, entity_ids=${request.entity_ids}, offsets=${request.offsets}`,
    ),
  )

  const entityIds = [] as number[]
  if (!request.entity_ids.length) {
    world.query(PositionTrait).forEach((entity) => {
      const { entityId } = unpackEntity(entity)
      entityIds.push(entityId)
    })
  } else {
    entityIds.push(...request.entity_ids)
  }

  const offsets =
    request.offsets.length > 1
      ? request.offsets
      : Array(entityIds.length).fill(request.offsets[0])

  for (let i = 0; i < entityIds.length; i++) {
    const entity = resolveEntity(world, entityIds[i])
    const currentPos = entity.get(PositionTrait) || [0, 0, 0]
    const newPos: PositionArray = [
      currentPos[0] + offsets[i][0],
      currentPos[1] + offsets[i][1],
      currentPos[2] + offsets[i][2],
    ]
    moveOverTime(entity, newPos, 500, 60).then()
  }
}

export const SHIFT_POSITION_SCHEMA = {
  name: 'shift_position',
  inputs: [{ name: 'request', type: 'application/x-msgpack' }],
  outputs: [],
}

interface SetColorRequest {
  entity_ids: Array<number>
  colors: Array<string>
}

export const setColor = async (world: World, action: Action) => {
  const requestNode = await action.getInput('request')
  const requestChunk = await requestNode.next()
  const request = decode(requestChunk.data) as SetColorRequest

  const logs = await action.getNodeMap().getNode('logs')
  await logs.put(
    makeTextChunk(
      `RUN set_color, entity_ids=${request.entity_ids}, colors=${request.colors}`,
    ),
  )

  for (let i = 0; i < request.entity_ids.length; i++) {
    const entity = resolveEntity(world, request.entity_ids[i])
    console.log('setColor', request.colors[i], entity)
    entity.set(Color, request.colors[i])
  }
}

export const SET_COLOR_SCHEMA = {
  name: 'set_color',
  inputs: [{ name: 'request', type: 'application/x-msgpack' }],
  outputs: [],
}

export const takeGlScreenshot = async (state: RootState, action: Action) => {
  if (!state) {
    throw new Error('No state in takeGlScreenshot')
  }

  const viewport = state.viewport
  const pixelWidth = viewport.width * viewport.dpr * viewport.factor
  const pixelHeight = viewport.height * viewport.dpr * viewport.factor

  const fbo = new THREE.WebGLRenderTarget(pixelWidth, pixelHeight, {
    format: THREE.RGBAFormat,
    type: THREE.UnsignedByteType,
    samples: 4, // Use 4x MSAA if available
    colorSpace: THREE.SRGBColorSpace,
  })
  state.gl.setRenderTarget(fbo)
  state.gl.render(state.scene, state.camera)
  state.gl.setRenderTarget(null)

  const imageData = new ImageData(pixelWidth, pixelHeight)
  state.gl.readRenderTargetPixels(
    fbo,
    0,
    0,
    pixelWidth,
    pixelHeight,
    imageData.data,
  )

  const canvas = document.createElement('canvas')
  canvas.width = pixelWidth
  canvas.height = pixelHeight
  const ctx = canvas.getContext('2d')
  ctx.putImageData(imageData, 0, 0)

  const flippedCanvas = document.createElement('canvas')
  flippedCanvas.width = pixelWidth
  flippedCanvas.height = pixelHeight
  const flippedCtx = flippedCanvas.getContext('2d')
  flippedCtx.translate(0, pixelHeight)
  flippedCtx.scale(1, -1)
  flippedCtx.drawImage(canvas, 0, 0)

  flippedCanvas.toBlob(async (blob) => {
    const responseNode = await action.getOutput('screenshot')
    await responseNode.putAndFinalize(await makeChunkFromBlob(blob!))
  }, 'image/png')
}

export const bindTakeGlScreenshotToState = (state: RootState) => {
  return (action: Action) => takeGlScreenshot(state, action)
}

export const TAKE_GL_SCREENSHOT_SCHEMA = {
  name: 'take_screenshot',
  inputs: [],
  outputs: [{ name: 'screenshot', type: 'image/png' }],
}
