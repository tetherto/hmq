'use strict'

const Protomux = require('protomux')
const b4a = require('b4a')
const c = require('compact-encoding')

module.exports = function setupHandshake (mq, conn) {
  const mux = Protomux.from(conn)
  let req

  const handshake = mux.createChannel({
    protocol: '@hypermq/handshake',
    id: b4a.from('hmq!handshake'),
    onopen: () => { if (!mq.writable) req.send(mq._publicKey) },
    onclose: () => {}
  })

  req = handshake.addMessage({
    encoding: c.fixed32,
    onmessage: async (key) => { await mq.addWriter(key) }
  })

  handshake.open()
}
