'use strict'

const ReadyResource = require('ready-resource')

class PubSub extends ReadyResource {
  constructor (mq) {
    super()
    this.mq = mq
    this.subs = new Map()
    this.pending = []
    this.scheduled = false
    this.processing = false
  }

  subscribe (topic, cb) {
    if (typeof cb !== 'function') throw new TypeError('callback must be a function')
    if (!this.subs.has(topic)) {
      this.subs.set(topic, new Set())
    }
    this.subs.get(topic).add(cb)
  }

  unsubscribe (topic, cb) {
    if (!cb) {
      this.subs.delete(topic)
      return
    }
    const subs = this.subs.get(topic)
    if (subs) {
      subs.delete(cb)
      if (subs.size === 0) this.subs.delete(topic)
    }
  }

  getSubs (topic) {
    return this.subs.get(topic)
  }

  onMessage (msg, isLocal) {
    const subs = this.subs.get(msg.topic)
    if (subs && subs.size > 0) {
      if (isLocal) {
        this.invokeSubscribers(subs, msg)
      } else {
        this.pending.push(msg)
        this.scheduleDelivery()
      }
    }
  }

  scheduleDelivery () {
    if (this.scheduled || this.processing) return
    this.scheduled = true
    setImmediate(() => {
      this.scheduled = false
      this.processDeliveries().catch((err) => {
        this.mq._emitWarning(new Error('Delivery processor failed', { cause: err }))
      })
    })
  }

  async processDeliveries () {
    if (this.processing) return
    this.processing = true

    try {
      while (this.pending.length > 0) {
        const batch = this.pending
        this.pending = []

        for (const msg of batch) {
          if (this.mq.writable && !(msg.concurrent > 0)) {
            this.mq._appendAck(msg.key)
          }

          const subs = this.subs.get(msg.topic)
          if (subs) {
            this.invokeSubscribers(subs, msg)
          }
        }
      }
    } finally {
      this.processing = false
      if (this.pending.length > 0) this.scheduleDelivery()
    }
  }

  invokeSubscribers (subs, msg) {
    const promises = []
    for (const cb of subs) {
      try {
        const result = cb(msg)
        if (result && typeof result.then === 'function') {
          promises.push(result.catch((err) => {
            this.mq._emitWarning(new Error('Subscriber callback rejected', { cause: err }))
          }))
        }
      } catch (err) {
        this.mq._emitWarning(new Error('Subscriber callback threw', { cause: err }))
      }
    }
    if (promises.length === 0) return Promise.resolve()
    return Promise.all(promises)
  }

  async _close () {
    this.subs.clear()
    this.pending.length = 0
    this.scheduled = false
    this.processing = false
  }
}

module.exports = PubSub
