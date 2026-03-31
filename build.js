'use strict'

const Hyperschema = require('hyperschema')

const schema = Hyperschema.from('./spec/hyperschema')
const mq = schema.namespace('hypermq')

mq.register({
  name: 'message',
  fields: [
    { name: 'topic', type: 'string', required: true },
    { name: 'data', type: 'buffer', required: true },
    { name: 'timestamp', type: 'uint', required: true },
    { name: 'key', type: 'fixed32', required: true },
    { name: 'consumer', type: 'fixed32' },
    { name: 'concurrent', type: 'uint' },
    { name: 'state', type: 'buffer' }
  ]
})

mq.register({
  name: 'action',
  compact: true,
  fields: [
    { name: 'key', type: 'fixed32', required: true },
    { name: 'consumer', type: 'fixed32', required: true }
  ]
})

mq.register({
  name: 'register',
  compact: true,
  fields: [
    { name: 'key', type: 'fixed32', required: true },
    { name: 'producer', type: 'bool', required: true }
  ]
})

Hyperschema.toDisk(schema)
