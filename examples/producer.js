'use strict'

const HyperMQ = require('../')
const Corestore = require('corestore')
const b4a = require('b4a')
const tmp = require('test-tmp')

const main = async () => {
  const store1 = new Corestore(await tmp())
  const store2 = new Corestore(await tmp())

  const producer = new HyperMQ(store1, { producer: true })
  await producer.ready()

  console.log('Producer writable:', producer.writable)

  const consumer = new HyperMQ(store2, producer.key)
  await consumer.ready()

  consumer.subscribe('tasks', async (msg) => {
    const task = b4a.toString(msg.data)
    console.log('[Consumer] Processing:', task)
    await new Promise(resolve => setTimeout(resolve, 50))
    console.log('[Consumer] Done:', task)
  })

  const tasks = ['compile', 'test', 'deploy']

  for (const task of tasks) {
    console.log('[Producer] Publishing:', task)
    const key = await producer.publish('tasks', task, { concurrent: 1 })
    const ack = await producer.waitAck(key)
    console.log('[Producer] Acked by:', b4a.toString(ack.consumer, 'hex').slice(0, 16) + '...\n')
  }

  await producer.close()
  await consumer.close()
  console.log('Done.')
}

main().catch(console.error)
