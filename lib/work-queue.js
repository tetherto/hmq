'use strict'

const ReadyResource = require('ready-resource')
const { sortByDistance } = require('./utils.js')

class WorkQueue extends ReadyResource {
  constructor (mq) {
    super()
    this.mq = mq
    this.consumers = new Map()
    this.busy = 0
    this.msgRejections = new Map()
    this.pendingClaims = new Map()
    this.deferred = new Map()
    this.registered = false
  }

  onRegisterConsumer (entry) {
    const hex = this.mq._keyHex(entry.key, 'register-consumer entry')
    if (hex === null) return
    this.consumers.set(hex, entry.key)
  }

  onRej (entry) {
    const hex = this.mq._keyHex(entry.key, 'rej entry')
    if (hex === null) return
    const consumerHex = this.mq._keyHex(entry.consumer)
    if (consumerHex === null) return

    let rejSet = this.msgRejections.get(hex)
    if (!rejSet) {
      rejSet = new Set()
      this.msgRejections.set(hex, rejSet)
    }
    rejSet.add(consumerHex)

    this.evaluateClaims(hex)
  }

  onMessage (msg) {
    if (this.mq._producer) return

    const hex = this.mq._keyHex(msg.key)
    if (hex === null) return

    const subs = this.mq.pubSub.getSubs(msg.topic)
    if (!subs || subs.size === 0) return

    if (this.busy > 0) {
      this.rejectAndDefer(hex, msg)
      return
    }

    const sorted = sortByDistance(this.consumers, msg.key)
    const myIndex = sorted.findIndex((e) => e.hex === this.mq._publicKeyHex)

    if (myIndex < 0) return

    if (myIndex === 0) {
      this.processClaim(hex, msg)
      return
    }

    const closerConsumers = sorted.slice(0, myIndex).map((e) => e.hex)
    const timer = setTimeout(() => {
      this.onClaimTimeout(hex)
    }, myIndex * this.mq._timeout)

    this.pendingClaims.set(hex, { msg, timer, closerConsumers })
  }

  rejectAndDefer (hex, msg) {
    this.deferred.set(hex, msg)
    if (this.mq.writable) {
      this.mq._appendRej(msg.key)
    }
  }

  onClaimTimeout (hex) {
    const claim = this.pendingClaims.get(hex)
    if (!claim) return
    this.pendingClaims.delete(hex)
    this.processClaim(hex, claim.msg)
  }

  evaluateClaims (hex) {
    const claim = this.pendingClaims.get(hex)
    if (!claim) {
      const deferred = this.deferred.get(hex)
      if (deferred) {
        const ackCount = this.mq.ackManager.getAckCount(hex)
        if (ackCount >= deferred.concurrent) {
          this.deferred.delete(hex)
        }
      }
      return
    }

    const ackCount = this.mq.ackManager.getAckCount(hex)
    if (ackCount >= claim.msg.concurrent) {
      clearTimeout(claim.timer)
      this.pendingClaims.delete(hex)
      return
    }

    const rejSet = this.msgRejections.get(hex)
    if (!rejSet) return

    const allCloserRejected = claim.closerConsumers.every((c) => rejSet.has(c))
    if (allCloserRejected) {
      clearTimeout(claim.timer)
      this.pendingClaims.delete(hex)
      this.processClaim(hex, claim.msg)
    }
  }

  processClaim (hex, msg) {
    const ackCount = this.mq.ackManager.getAckCount(hex)
    if (ackCount >= msg.concurrent) return

    if (this.busy > 0) {
      this.rejectAndDefer(hex, msg)
      return
    }

    this.deliverAndAck(hex, msg)
  }

  deliverAndAck (hex, msg) {
    this.busy++
    this.deferred.delete(hex)
    this.mq._appendAck(msg.key)

    const subs = this.mq.pubSub.getSubs(msg.topic)
    const done = () => {
      this.busy--
      this.drainDeferred()
    }

    if (!subs || subs.size === 0) {
      done()
      return
    }

    this.mq.pubSub.invokeSubscribers(subs, msg).then(done, done)
  }

  drainDeferred () {
    if (this.busy > 0) return
    if (this.deferred.size === 0) return

    let best = null
    let bestHex = null

    for (const [hex, msg] of this.deferred) {
      const ackCount = this.mq.ackManager.getAckCount(hex)
      if (ackCount >= msg.concurrent) {
        this.deferred.delete(hex)
        continue
      }

      const sorted = sortByDistance(this.consumers, msg.key)
      const myIndex = sorted.findIndex((e) => e.hex === this.mq._publicKeyHex)
      if (myIndex < 0) continue

      if (best === null || myIndex < best) {
        best = myIndex
        bestHex = hex
      }
    }

    if (bestHex !== null) {
      const msg = this.deferred.get(bestHex)
      if (msg) {
        this.deferred.delete(bestHex)
        this.onMessage(msg)
      }
    }
  }

  async _close () {
    for (const claim of this.pendingClaims.values()) clearTimeout(claim.timer)
    this.pendingClaims.clear()
    this.consumers.clear()
    this.msgRejections.clear()
    this.deferred.clear()
    this.busy = 0
  }
}

module.exports = WorkQueue
