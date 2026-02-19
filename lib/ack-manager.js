'use strict'

const b4a = require('b4a')
const ReadyResource = require('ready-resource')

class AckManager extends ReadyResource {
  constructor (mq) {
    super()
    this.mq = mq
    this.resolvers = new Map()
    this.wildcards = []
    this.orphans = new Map()
    this.msgAckCount = new Map()
  }

  waitAck (key) {
    return new Promise((resolve) => {
      if (!key) {
        this.wildcards.push(resolve)
        return
      }
      const hex = b4a.toString(key, 'hex')
      const list = this.resolvers.get(hex)
      if (list) list.push(resolve)
      else this.resolvers.set(hex, [resolve])
    })
  }

  onAck (entry) {
    const hex = this.mq._keyHex(entry.key, 'ack entry')
    if (hex === null) return

    const count = (this.msgAckCount.get(hex) || 0) + 1
    this.msgAckCount.set(hex, count)

    this.mq.workQueue.evaluateClaims(hex)
    this.resolveAcks(entry.key, entry.ack)
  }

  addOrphan (hex, ack) {
    this.orphans.set(hex, ack)
    if (this.orphans.size > this.mq._maxOrphans) {
      const oldest = this.orphans.keys().next().value
      this.orphans.delete(oldest)
      this.mq._emitWarning(new Error('Orphan ack evicted, cap reached'))
    }
  }

  getOrphan (hex) {
    return this.orphans.get(hex)
  }

  removeOrphan (hex) {
    this.orphans.delete(hex)
  }

  getAckCount (hex) {
    return this.msgAckCount.get(hex) || 0
  }

  resolveAcks (key, ack) {
    const hex = b4a.toString(key, 'hex')
    const keyed = this.resolvers.get(hex)
    if (keyed) {
      this.resolvers.delete(hex)
      for (const resolve of keyed) resolve(ack)
    }

    const wildcards = this.wildcards
    this.wildcards = []
    for (const resolve of wildcards) resolve(ack)
  }

  async _close () {
    this.orphans.clear()
    for (const list of this.resolvers.values()) {
      for (const resolve of list) resolve(null)
    }
    this.resolvers.clear()
    for (const resolve of this.wildcards) resolve(null)
    this.wildcards = []
    this.msgAckCount.clear()
  }
}

module.exports = AckManager
