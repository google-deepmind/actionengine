import { trait } from 'koota'
import * as THREE from 'three'

export declare interface Coords3D {
  x: number
  y: number
  z: number
}

export declare interface Quaternion {
  x: number
  y: number
  z: number
  w: number
}

export declare interface UIState {
  pointerIsDown: boolean
  orbitingBlocked: boolean
}

export const Scale = trait((): Coords3D => ({ x: 1, y: 1, z: 1 }))
export const Position = trait((): Coords3D => ({ x: 0, y: 0, z: 0 }))
export const Rotation = trait((): Quaternion => ({ x: 0, y: 0, z: 0, w: 1 }))
export const Mesh = trait((): THREE.Mesh => new THREE.Mesh())
export const TextureSource = trait(
  (): TexImageSource | OffscreenCanvas => new Image(),
)
export const Material = trait((): THREE.Material => new THREE.Material())
export const GlobalUIState = trait(
  (): UIState => ({
    pointerIsDown: false,
    orbitingBlocked: false,
  }),
)
export const IsVisible = trait()
export const IsActive = trait()
