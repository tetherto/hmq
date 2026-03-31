'use strict'

const b4a = require('b4a')
const schema = require('../spec/hyperschema')

const TYPE_MESSAGE = 0
const TYPE_ACK = 1
const TYPE_ADD_WRITER = 2
const TYPE_REMOVE_WRITER = 3
const TYPE_REJ = 4
const TYPE_REGISTER_CONSUMER = 5
const TYPE_STATUS_UPDATE = 6

exports.TYPE_MESSAGE = TYPE_MESSAGE
exports.TYPE_ACK = TYPE_ACK
exports.TYPE_ADD_WRITER = TYPE_ADD_WRITER
exports.TYPE_REMOVE_WRITER = TYPE_REMOVE_WRITER
exports.TYPE_REJ = TYPE_REJ
exports.TYPE_REGISTER_CONSUMER = TYPE_REGISTER_CONSUMER
exports.TYPE_STATUS_UPDATE = TYPE_STATUS_UPDATE

const assertKey = (key, name) => {
  if (!b4a.isBuffer(key)) throw new TypeError(name + ' must be a Buffer')
  if (key.byteLength !== 32) throw new TypeError(name + ' must be 32 bytes')
}

function prefixType (type, payload) {
  const out = b4a.allocUnsafe(1 + payload.byteLength)
  out[0] = type
  b4a.copy(payload, out, 1)
  return out
}

exports.encodeMessage = function encodeMessage (msg) {
  return prefixType(TYPE_MESSAGE, schema.encode('@hypermq/message', msg))
}

exports.encodeAck = function encodeAck (key, consumer) {
  assertKey(key, 'ack key')
  assertKey(consumer, 'ack consumer')
  return prefixType(TYPE_ACK, schema.encode('@hypermq/action', { key, consumer }))
}

exports.encodeAddWriter = function encodeAddWriter (key) {
  assertKey(key, 'writer key')
  return prefixType(TYPE_ADD_WRITER, key)
}

exports.encodeRemoveWriter = function encodeRemoveWriter (key) {
  assertKey(key, 'writer key')
  return prefixType(TYPE_REMOVE_WRITER, key)
}

exports.encodeRej = function encodeRej (key, consumer) {
  assertKey(key, 'rej key')
  assertKey(consumer, 'rej consumer')
  return prefixType(TYPE_REJ, schema.encode('@hypermq/action', { key, consumer }))
}

exports.encodeRegister = function encodeRegister (key, producer) {
  assertKey(key, 'register key')
  return prefixType(TYPE_REGISTER_CONSUMER, schema.encode('@hypermq/register', { key, producer: !!producer }))
}

exports.encodeRegisterConsumer = function encodeRegisterConsumer (key) {
  return exports.encodeRegister(key, false)
}

exports.encodeStatusUpdate = function encodeStatusUpdate (key, state) {
  assertKey(key, 'status key')
  if (!b4a.isBuffer(state)) throw new TypeError('status state must be a Buffer')
  const payload = b4a.allocUnsafe(32 + state.byteLength)
  b4a.copy(key, payload, 0)
  b4a.copy(state, payload, 32)
  return prefixType(TYPE_STATUS_UPDATE, payload)
}

exports.decode = function decode (buf) {
  if (!b4a.isBuffer(buf)) throw new TypeError('Entry must be a Buffer')
  if (buf.byteLength < 1) throw new Error('Entry buffer is empty')

  const type = buf[0]
  const payload = buf.subarray(1)

  switch (type) {
    case TYPE_MESSAGE:
      return { type: TYPE_MESSAGE, ...schema.decode('@hypermq/message', payload) }
    case TYPE_ACK: {
      const entry = schema.decode('@hypermq/action', payload)
      return { type: TYPE_ACK, key: entry.key, ack: { consumer: entry.consumer } }
    }
    case TYPE_ADD_WRITER: {
      if (payload.byteLength !== 32) throw new Error('Invalid add-writer entry payload size')
      return { type: TYPE_ADD_WRITER, key: payload }
    }
    case TYPE_REMOVE_WRITER: {
      if (payload.byteLength !== 32) throw new Error('Invalid remove-writer entry payload size')
      return { type: TYPE_REMOVE_WRITER, key: payload }
    }
    case TYPE_REJ: {
      const entry = schema.decode('@hypermq/action', payload)
      return { type: TYPE_REJ, key: entry.key, consumer: entry.consumer }
    }
    case TYPE_REGISTER_CONSUMER: {
      const entry = schema.decode('@hypermq/register', payload)
      return { type: TYPE_REGISTER_CONSUMER, key: entry.key, producer: entry.producer }
    }
    case TYPE_STATUS_UPDATE: {
      if (payload.byteLength < 32) throw new Error('Invalid status-update entry payload size')
      return { type: TYPE_STATUS_UPDATE, key: payload.subarray(0, 32), state: payload.subarray(32) }
    }
    default:
      throw new Error('Unknown entry type: ' + type)
  }
}

exports.encodeViewRecord = function encodeViewRecord (msg) {
  return schema.encode('@hypermq/message', msg)
}

exports.decodeViewRecord = function decodeViewRecord (buf) {
  return schema.decode('@hypermq/message', buf)
}

exports.toBuffer = function toBuffer (data) {
  if (b4a.isBuffer(data)) return data
  if (typeof data === 'string') return b4a.from(data)
  try {
    return b4a.from(JSON.stringify(data))
  } catch (err) {
    throw new TypeError('data must be a Buffer, string, or JSON-serializable value')
  }
}
