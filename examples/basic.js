'use strict'

const HyperMQ = require('../')
const Corestore = require('corestore')
const b4a = require('b4a')
const tmp = require('test-tmp')

const main = async () => {
  const store1 = new Corestore(await tmp())
  const store2 = new Corestore(await tmp())

  const mq1 = new HyperMQ(store1)
  await mq1.ready()

  const mq2 = new HyperMQ(store2, mq1.key)
  await mq2.ready()

  mq2.subscribe('topic', (msg) => {
    console.log('Received message:', b4a.toString(msg.data))
    console.log('Message key:', b4a.toString(msg.key, 'hex'))
  })

  console.log('Publishing message...')
  const msgKey = await mq1.publish('topic', 'hello')

  const ack = await mq1.waitAck(msgKey)

  console.log('Message acknowledged by:', b4a.toString(ack.consumer, 'hex'))

  mq2.unsubscribe('topic')

  await mq1.close()
  await mq2.close()
}

main().catch(console.error)
