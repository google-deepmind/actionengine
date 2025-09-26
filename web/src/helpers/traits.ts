import { trait } from 'koota'
import * as THREE from 'three'

export type PositionArray = [x: number, y: number, z: number]
export const PositionTrait = trait((): PositionArray => [0, 0, 0])

export const RotationTrait = trait((): Array<number> => [0, 0, 0])
export const ScaleTrait = trait((): Array<number> => [1, 1, 1])

export const Color = trait((): string => '#ffffff')

export const IsSelected = trait()
