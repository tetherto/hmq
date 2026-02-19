'use strict'

const crypto = require('crypto')
const Autobee = require('autobee')
const ReadyResource = require('ready-resource')
const Hyperswarm = require('hyperswarm')
const ID = require('hypercore-id-encoding')
const b4a = require('b4a')
const enc = require('./lib/encoding.js')
const { isObject } = require('./lib/utils.js')
const setupHandshake = require('./lib/handshake.js')
const PubSub = require('./lib/pub-sub.js')
const WorkQueue = require('./lib/work-queue.js')
const AckManager = require('./lib/ack-manager.js')

const VIEW_PREFIX = b4a.from('msg!')

class HyperMQ extends ReadyResource {
  constructor (corestore, key = null, opts = {}) {
    super()

    if (isObject(key)) {
      opts = key
      key = null
    }

    this.corestore = corestore
    this.key = key ? ID.decode(key) : null
    this.discoveryKey = null
    this.opts = opts

    this._keyPair = opts.keyPair || null
    this._publicKey = null
    this._publicKeyHex = null
    this._producer = !!opts.producer
    this._concurrent = opts.concurrent || 0
    this._timeout = opts.timeout || 5000
    this._maxOrphans = opts.maxOrphanAcks || 10000

    this._discovery = null
    this._ownSwarm = !opts.swarm

    this.pubSub = new PubSub(this)
    this.workQueue = new WorkQueue(this)
    this.ackManager = new AckManager(this)

    this.autobee = new Autobee(corestore, key, {
      apply: this._apply.bind(this),
      ...opts
    })

    this.swarm = opts.swarm || new Hyperswarm()
  }

  get writable () {
    return this.autobee.writable
  }

  async _open () {
    await this.autobee.ready()
    await this.pubSub.ready()
    await this.workQueue.ready()
    await this.ackManager.ready()

    this.key = this.autobee.key
    this.discoveryKey = this.autobee.discoveryKey
    this._publicKey = this._keyPair ? this._keyPair.publicKey : this.autobee.local.key
    this._publicKeyHex = b4a.toString(this._publicKey, 'hex')

    this.swarm.on('connection', (conn) => {
      this.corestore.replicate(conn)
      this.setupHandshake(conn)
    })

    this._discovery = this.swarm.join(this.discoveryKey)
    await this._discovery.flushed()

    if (!this._producer && !this.workQueue.registered) {
      if (!this.writable) {
        await new Promise(resolve => this.autobee.once('writable', resolve))
      }
      try {
        await this.autobee.append(enc.encodeRegisterConsumer(this._publicKey))
        this.workQueue.registered = true
      } catch (err) {
        this._emitWarning(new Error('Failed to register consumer', { cause: err }))
      }
    }
  }

  async _close () {
    if (this._discovery) await this.swarm.leave(this.discoveryKey)
    if (this._ownSwarm) await this.swarm.destroy()
    
    await this.pubSub.close()
    await this.workQueue.close()
    await this.ackManager.close()

    await this.autobee.close()
  }

  async _apply (batch, view, host) {
    const w = view.write()

    for (const node of batch) {
      let entry = null
      try {
        entry = enc.decode(node.value)
      } catch (err) {
        this._emitWarning(new Error('Skipping malformed log entry', { cause: err }))
        continue
      }

      switch (entry.type) {
        case enc.TYPE_ADD_WRITER:
          await this._applyWriterChange(entry, host, 'addWriter')
          break
        case enc.TYPE_REMOVE_WRITER:
          await this._applyWriterChange(entry, host, 'removeWriter')
          break
        case enc.TYPE_MESSAGE:
          this._applyMessage(entry, node, w)
          break
        case enc.TYPE_ACK:
          await this._applyAck(entry, view, w)
          break
        case enc.TYPE_REJ:
          this.workQueue.onRej(entry)
          break
        case enc.TYPE_REGISTER_CONSUMER:
          this.workQueue.onRegisterConsumer(entry)
          break
      }
    }

    try {
      await w.flush()
    } catch (err) {
      this._emitWarning(new Error('Failed to flush view writes', { cause: err }))
    }
  }

  async _applyWriterChange (entry, host, method) {
    try {
      await host[method](entry.key)
    } catch (err) {
      this._emitWarning(new Error('Failed to apply ' + method + ' entry', { cause: err }))
    }
  }

  _applyMessage (entry, node, w) {
    const hex = this._keyHex(entry.key, 'message entry')
    if (hex === null) return

    const viewKey = this._viewKey(entry.key)
    const orphanAck = this.ackManager.getOrphan(hex) || null
    if (orphanAck) this.ackManager.removeOrphan(hex)

    const record = enc.encodeViewRecord({
      topic: entry.topic,
      data: entry.data,
      timestamp: entry.timestamp,
      key: entry.key,
      ack: orphanAck,
      concurrent: entry.concurrent || 0
    })

    w.tryPut(viewKey, record)

    const concurrent = entry.concurrent || 0
    const msg = { topic: entry.topic, data: entry.data, key: entry.key, concurrent }

    if (concurrent > 0 && !this._producer) {
      this.workQueue.onMessage(msg)
      return
    }

    const isLocal = b4a.equals(node.key, this.autobee.local.key)
    this.pubSub.onMessage(msg, isLocal)
  }

  async _applyAck (entry, view, w) {
    const hex = this._keyHex(entry.key, 'ack entry')
    if (hex === null) return

    const viewKey = this._viewKey(entry.key)
    let persisted = false

    try {
      const prev = await view.get(viewKey)
      if (prev) {
        const msg = enc.decodeViewRecord(prev.value)
        msg.ack = entry.ack
        w.tryPut(viewKey, enc.encodeViewRecord(msg))
        persisted = true
      }
    } catch (err) {
      this._emitWarning(new Error('Failed to persist ack view record update', { cause: err }))
    }

    if (!persisted) {
      this.ackManager.addOrphan(hex, entry.ack)
    }

    this.ackManager.onAck(entry)
  }

  async publish (topic, data, opts = {}) {
    if (!this.opened) await this.ready()
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new TypeError('topic must be a non-empty string')
    }
    const payload = enc.toBuffer(data)
    const key = crypto.randomBytes(32)
    const concurrent = opts.concurrent != null ? opts.concurrent : this._concurrent
    const buf = enc.encodeMessage({
      topic,
      data: payload,
      timestamp: Date.now(),
      key,
      ack: null,
      concurrent
    })
    await this.autobee.append(buf)
    return key
  }

  subscribe (topic, cb) {
    this.pubSub.subscribe(topic, cb)
  }

  unsubscribe (topic, cb) {
    this.pubSub.unsubscribe(topic, cb)
  }

  async waitAck (key) {
    if (!this.opened) await this.ready()
    return this.ackManager.waitAck(key)
  }

  replicate (...args) {
    return this.autobee.replicate(...args)
  }

  setupHandshake (conn) {
    setupHandshake(this, conn)
  }

  async flush () {
    if (!this.opened) await this.ready()
    await this.autobee.flush()
  }

  async addWriter (key) {
    if (!this.opened) await this.ready()
    if (typeof key === 'string') key = ID.decode(key)
    await this.autobee.append(enc.encodeAddWriter(key))
  }

  async removeWriter (key) {
    if (!this.opened) await this.ready()
    if (typeof key === 'string') key = ID.decode(key)
    await this.autobee.append(enc.encodeRemoveWriter(key))
  }

  _appendAck (key) {
    if (!this.writable) return
    const buf = enc.encodeAck(key, { consumer: this._publicKey })
    this.autobee.append(buf).catch((err) => {
      this._emitWarning(new Error('Failed to append ack', { cause: err }))
    })
  }

  _appendRej (key) {
    if (!this.writable) return
    const buf = enc.encodeRej(key, this._publicKey)
    this.autobee.append(buf).catch((err) => {
      this._emitWarning(new Error('Failed to append rej entry', { cause: err }))
    })
  }

  _keyHex (key, label) {
    if (!b4a.isBuffer(key)) {
      if (label) this._emitWarning(new Error('Skipping ' + label + ' with invalid key type'))
      return null
    }
    return b4a.toString(key, 'hex')
  }

  _viewKey (key) {
    const out = b4a.allocUnsafe(VIEW_PREFIX.byteLength + key.byteLength)
    b4a.copy(VIEW_PREFIX, out, 0)
    b4a.copy(key, out, VIEW_PREFIX.byteLength)
    return out
  }

  _emitWarning (err) {
    this.emit('warning', err)
  }
}

module.exports = HyperMQ
