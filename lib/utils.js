'use strict'

const b4a = require('b4a')

exports.isObject = function isObject (o) {
  return typeof o === 'object' && o !== null && !b4a.isBuffer(o)
}

exports.xorDistance = function xorDistance (a, b) {
  const out = b4a.allocUnsafe(32)
  for (let i = 0; i < 32; i++) out[i] = a[i] ^ b[i]
  return out
}

exports.compareDistance = function compareDistance (a, b) {
  for (let i = 0; i < 32; i++) {
    if (a[i] < b[i]) return -1
    if (a[i] > b[i]) return 1
  }
  return 0
}

exports.sortByDistance = function sortByDistance (consumers, msgKey) {
  const entries = []
  for (const [hex, key] of consumers) {
    entries.push({ hex, key, distance: exports.xorDistance(key, msgKey) })
  }
  entries.sort((a, b) => exports.compareDistance(a.distance, b.distance))
  return entries
}
