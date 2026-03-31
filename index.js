'use strict'

const crypto = require('crypto')
const Autobee = require('autobee')
const ReadyResource = require('ready-resource')
const Hyperswarm = require('hyperswarm')
const ID = require('hypercore-id-encoding')
const Protomux = require('protomux')
const b4a = require('b4a')
const c = require('compact-encoding')
const enc = require('./lib/encoding.js')
const { isObject, sortByDistance } = require('./lib/utils.js')

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

    this._subs = new Map()
    this._resolvers = new Map()
    this._wildcards = []
    this._orphans = new Map()
    this._maxOrphans = opts.maxOrphanAcks || 10000
    this._pending = []
    this._scheduled = false
    this._processing = false
    this._discovery = null
    this._ownSwarm = !opts.swarm

    this._consumers = new Map()
    this._busy = 0
    this._msgRejections = new Map()
    this._msgAckCount = new Map()
    this._pendingClaims = new Map()
    this._deferred = new Map()
    this._registered = false

    this._statusSubs = new Map()
    this._statusSubsWildcard = new Set()

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
    await this.autobee.flush()

    if (!this._producer && !this._registered) {
      if (!this.writable) {
        await new Promise(resolve => this.autobee.once('writable', resolve))
      }
      try {
        await this.autobee.append(enc.encodeRegisterConsumer(this._publicKey))
        this._registered = true
      } catch (err) {
        this._emitWarning(new Error('Failed to register consumer', { cause: err }))
      }
    }
  }

  async _close () {
    if (this._discovery) await this.swarm.leave(this.discoveryKey)
    if (this._ownSwarm) await this.swarm.destroy()
    this._subs.clear()
    this._pending.length = 0
    this._scheduled = false
    this._processing = false
    this._orphans.clear()
    for (const list of this._resolvers.values()) {
      for (const resolve of list) resolve(null)
    }
    this._resolvers.clear()
    for (const resolve of this._wildcards) resolve(null)
    this._wildcards = []

    for (const claim of this._pendingClaims.values()) clearTimeout(claim.timer)
    this._pendingClaims.clear()
    this._consumers.clear()
    this._msgRejections.clear()
    this._msgAckCount.clear()
    this._deferred.clear()
    this._busy = 0
    this._statusSubs.clear()
    this._statusSubsWildcard.clear()

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
          this._applyRej(entry)
          break
        case enc.TYPE_REGISTER_CONSUMER:
          this._applyRegisterConsumer(entry)
          break
        case enc.TYPE_STATUS_UPDATE:
          await this._applyStatusUpdate(entry, view, w)
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
    const orphanAck = this._orphans.get(hex) || null
    if (orphanAck) this._orphans.delete(hex)

    const record = enc.encodeViewRecord({
      topic: entry.topic,
      data: entry.data,
      timestamp: entry.timestamp,
      key: entry.key,
      consumer: (orphanAck && orphanAck.consumer) || entry.consumer || undefined,
      concurrent: entry.concurrent || 0,
      state: entry.state || undefined
    })

    w.tryPut(viewKey, record)

    const concurrent = entry.concurrent || 0
    const msg = { topic: entry.topic, data: entry.data, key: entry.key, concurrent }

    if (concurrent > 0 && !this._producer) {
      this._handleWorkQueueMessage(msg)
      return
    }

    const isLocal = b4a.equals(node.key, this.autobee.local.key)
    const subs = this._subs.get(entry.topic)

    if (subs && subs.size > 0) {
      if (isLocal) {
        this._invokeSubscribers(subs, msg)
      } else {
        this._pending.push(msg)
        this._scheduleDelivery()
      }
    }
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
        msg.consumer = entry.ack.consumer
        w.tryPut(viewKey, enc.encodeViewRecord(msg))
        persisted = true
      }
    } catch (err) {
      this._emitWarning(new Error('Failed to persist ack view record update', { cause: err }))
    }

    if (!persisted) {
      this._orphans.set(hex, entry.ack)
      if (this._orphans.size > this._maxOrphans) {
        const oldest = this._orphans.keys().next().value
        this._orphans.delete(oldest)
        this._emitWarning(new Error('Orphan ack evicted, cap reached'))
      }
    }

    const count = (this._msgAckCount.get(hex) || 0) + 1
    this._msgAckCount.set(hex, count)
    this._evaluateClaims(hex)

    this._resolveAcks(entry.key, entry.ack)
  }

  async _applyStatusUpdate (entry, view, w) {
    const hex = this._keyHex(entry.key, 'status entry')
    if (hex === null) return

    const viewKey = this._viewKey(entry.key)

    try {
      const prev = await view.get(viewKey)
      if (prev) {
        const msg = enc.decodeViewRecord(prev.value)
        msg.state = entry.state
        w.tryPut(viewKey, enc.encodeViewRecord(msg))
      }
    } catch (err) {
      this._emitWarning(new Error('Failed to persist status view record update', { cause: err }))
    }

    this._notifyStatusSubscribers(entry.key, entry.state)
  }

  _scheduleDelivery () {
    if (this._scheduled || this._processing) return
    this._scheduled = true
    setImmediate(() => {
      this._scheduled = false
      this._processDeliveries().catch((err) => {
        this._emitWarning(new Error('Delivery processor failed', { cause: err }))
      })
    })
  }

  async _processDeliveries () {
    if (this._processing) return
    this._processing = true

    try {
      while (this._pending.length > 0) {
        const batch = this._pending
        this._pending = []

        for (const msg of batch) {
          if (this.writable && !(msg.concurrent > 0)) {
            this._appendAck(msg.key)
          }

          const subs = this._subs.get(msg.topic)
          if (subs) {
            this._invokeSubscribers(subs, msg)
          }
        }
      }
    } finally {
      this._processing = false
      if (this._pending.length > 0) this._scheduleDelivery()
    }
  }

  _resolveAcks (key, ack) {
    const hex = b4a.toString(key, 'hex')
    const keyed = this._resolvers.get(hex)
    if (keyed) {
      this._resolvers.delete(hex)
      for (const resolve of keyed) resolve(ack)
    }

    const wildcards = this._wildcards
    this._wildcards = []
    for (const resolve of wildcards) resolve(ack)
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
      concurrent
    })
    await this.autobee.append(buf)
    return key
  }

  subscribe (topic, cb) {
    if (typeof cb !== 'function') throw new TypeError('callback must be a function')
    if (!this._subs.has(topic)) {
      this._subs.set(topic, new Set())
    }
    this._subs.get(topic).add(cb)
  }

  unsubscribe (topic, cb) {
    if (!cb) {
      this._subs.delete(topic)
      return
    }
    const subs = this._subs.get(topic)
    if (subs) {
      subs.delete(cb)
      if (subs.size === 0) this._subs.delete(topic)
    }
  }

  async waitAck (key) {
    if (!this.opened) await this.ready()
    return new Promise((resolve) => {
      if (!key) {
        this._wildcards.push(resolve)
        return
      }
      const hex = b4a.toString(key, 'hex')
      const list = this._resolvers.get(hex)
      if (list) list.push(resolve)
      else this._resolvers.set(hex, [resolve])
    })
  }

  replicate (...args) {
    return this.autobee.replicate(...args)
  }

  setupHandshake (conn) {
    const mux = Protomux.from(conn)
    let req

    const handshake = mux.createChannel({
      protocol: '@hypermq/handshake',
      id: b4a.from('hmq!handshake'),
      onopen: () => { if (!this.writable) req.send(this._publicKey) },
      onclose: () => {}
    })

    req = handshake.addMessage({
      encoding: c.fixed32,
      onmessage: async (key) => { await this.addWriter(key) }
    })

    handshake.open()
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

  onStatus (key, cb) {
    if (typeof key === 'function') {
      cb = key
      key = null
    }
    if (typeof cb !== 'function') throw new TypeError('callback must be a function')
    if (key) {
      if (typeof key === 'string') key = Buffer.from(key, 'hex')
      const hex = b4a.toString(key, 'hex')
      let set = this._statusSubs.get(hex)
      if (!set) {
        set = new Set()
        this._statusSubs.set(hex, set)
      }
      set.add(cb)
    } else {
      this._statusSubsWildcard.add(cb)
    }
  }

  offStatus (key, cb) {
    if (typeof key === 'function') {
      cb = key
      key = null
    }
    if (key) {
      if (typeof key === 'string') key = Buffer.from(key, 'hex')
      const hex = b4a.toString(key, 'hex')
      const set = this._statusSubs.get(hex)
      if (set) {
        if (cb) {
          set.delete(cb)
          if (set.size === 0) this._statusSubs.delete(hex)
        } else {
          this._statusSubs.delete(hex)
        }
      }
    } else if (cb) {
      this._statusSubsWildcard.delete(cb)
    } else {
      this._statusSubsWildcard.clear()
    }
  }

  _appendStatusUpdate (key, status) {
    if (!this.writable) {
      this._emitWarning(new Error('Status update dropped: not writable'))
      return
    }
    const state = enc.toBuffer(status)
    const buf = enc.encodeStatusUpdate(key, state)
    this.autobee.append(buf).catch((err) => {
      this._emitWarning(new Error('Failed to append status update', { cause: err }))
    })
  }

  _notifyStatusSubscribers (key, state) {
    const hex = b4a.toString(key, 'hex')
    const keyed = this._statusSubs.get(hex)
    if (keyed) {
      for (const cb of keyed) {
        try { cb(key, state) } catch (err) {
          this._emitWarning(new Error('Status subscriber callback threw', { cause: err }))
        }
      }
    }
    for (const cb of this._statusSubsWildcard) {
      try { cb(key, state) } catch (err) {
        this._emitWarning(new Error('Status subscriber callback threw', { cause: err }))
      }
    }
  }

  _applyRegisterConsumer (entry) {
    const hex = this._keyHex(entry.key, 'register-consumer entry')
    if (hex === null) return
    this._consumers.set(hex, entry.key)
  }

  _applyRej (entry) {
    const hex = this._keyHex(entry.key, 'rej entry')
    if (hex === null) return
    const consumerHex = this._keyHex(entry.consumer)
    if (consumerHex === null) return

    let rejSet = this._msgRejections.get(hex)
    if (!rejSet) {
      rejSet = new Set()
      this._msgRejections.set(hex, rejSet)
    }
    rejSet.add(consumerHex)

    this._evaluateClaims(hex)
  }

  _handleWorkQueueMessage (msg) {
    if (this._producer) return

    const hex = this._keyHex(msg.key)
    if (hex === null) return

    const subs = this._subs.get(msg.topic)
    if (!subs || subs.size === 0) return

    if (this._busy > 0) {
      this._rejectAndDefer(hex, msg)
      return
    }

    const sorted = sortByDistance(this._consumers, msg.key)
    const myIndex = sorted.findIndex((e) => e.hex === this._publicKeyHex)

    if (myIndex < 0) return

    if (myIndex === 0) {
      this._processClaim(hex, msg)
      return
    }

    const closerConsumers = sorted.slice(0, myIndex).map((e) => e.hex)
    const timer = setTimeout(() => {
      this._onClaimTimeout(hex)
    }, myIndex * this._timeout)

    this._pendingClaims.set(hex, { msg, timer, closerConsumers })
  }

  _rejectAndDefer (hex, msg) {
    this._deferred.set(hex, msg)
    if (this.writable) {
      const rejBuf = enc.encodeRej(msg.key, this._publicKey)
      this.autobee.append(rejBuf).catch((err) => {
        this._emitWarning(new Error('Failed to append rej entry', { cause: err }))
      })
    }
  }

  _onClaimTimeout (hex) {
    const claim = this._pendingClaims.get(hex)
    if (!claim) return
    this._pendingClaims.delete(hex)
    this._processClaim(hex, claim.msg)
  }

  _evaluateClaims (hex) {
    const claim = this._pendingClaims.get(hex)
    if (!claim) {
      const deferred = this._deferred.get(hex)
      if (deferred) {
        const ackCount = this._msgAckCount.get(hex) || 0
        if (ackCount >= deferred.concurrent) {
          this._deferred.delete(hex)
        }
      }
      return
    }

    const ackCount = this._msgAckCount.get(hex) || 0
    if (ackCount >= claim.msg.concurrent) {
      clearTimeout(claim.timer)
      this._pendingClaims.delete(hex)
      return
    }

    const rejSet = this._msgRejections.get(hex)
    if (!rejSet) return

    const allCloserRejected = claim.closerConsumers.every((c) => rejSet.has(c))
    if (allCloserRejected) {
      clearTimeout(claim.timer)
      this._pendingClaims.delete(hex)
      this._processClaim(hex, claim.msg)
    }
  }

  _processClaim (hex, msg) {
    const ackCount = this._msgAckCount.get(hex) || 0
    if (ackCount >= msg.concurrent) return

    if (this._busy > 0) {
      this._rejectAndDefer(hex, msg)
      return
    }

    this._deliverAndAck(hex, msg)
  }

  _deliverAndAck (hex, msg) {
    this._busy++
    this._deferred.delete(hex)
    this._appendAck(msg.key)

    const subs = this._subs.get(msg.topic)
    const done = () => {
      this._busy--
      this._drainDeferred()
    }

    if (!subs || subs.size === 0) {
      done()
      return
    }

    this._invokeSubscribers(subs, msg).then(done, done)
  }

  _drainDeferred () {
    if (this._busy > 0) return
    if (this._deferred.size === 0) return

    let best = null
    let bestHex = null

    for (const [hex, msg] of this._deferred) {
      const ackCount = this._msgAckCount.get(hex) || 0
      if (ackCount >= msg.concurrent) {
        this._deferred.delete(hex)
        continue
      }

      const sorted = sortByDistance(this._consumers, msg.key)
      const myIndex = sorted.findIndex((e) => e.hex === this._publicKeyHex)
      if (myIndex < 0) continue

      if (best === null || myIndex < best) {
        best = myIndex
        bestHex = hex
      }
    }

    if (bestHex !== null) {
      const msg = this._deferred.get(bestHex)
      if (msg) {
        this._deferred.delete(bestHex)
        this._handleWorkQueueMessage(msg)
      }
    }
  }

  _invokeSubscribers (subs, msg) {
    msg.setStatus = (status) => { this._appendStatusUpdate(msg.key, status) }
    const promises = []
    for (const cb of subs) {
      try {
        const result = cb(msg)
        if (result && typeof result.then === 'function') {
          promises.push(result.catch((err) => {
            this._emitWarning(new Error('Subscriber callback rejected', { cause: err }))
          }))
        }
      } catch (err) {
        this._emitWarning(new Error('Subscriber callback threw', { cause: err }))
      }
    }
    if (promises.length === 0) return Promise.resolve()
    return Promise.all(promises)
  }

  _appendAck (key) {
    if (!this.writable) {
      this._emitWarning(new Error('Ack dropped: not writable'))
      return
    }
    const buf = enc.encodeAck(key, this._publicKey)
    this.autobee.append(buf).catch((err) => {
      this._emitWarning(new Error('Failed to append ack', { cause: err }))
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
