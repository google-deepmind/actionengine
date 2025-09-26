import * as THREE from 'three'

export const getRoundedPlaneShape = (width, height, radius) => {
  const shape = new THREE.Shape()

  shape.moveTo(0, radius)
  shape.lineTo(0, height - radius)
  shape.quadraticCurveTo(0, height, radius, height)
  shape.lineTo(width - radius, height)
  shape.quadraticCurveTo(width, height, width, height - radius)
  shape.lineTo(width, radius)
  shape.quadraticCurveTo(width, 0, width - radius, 0)
  shape.lineTo(radius, 0)
  shape.quadraticCurveTo(0, 0, 0, radius)

  return shape
}
