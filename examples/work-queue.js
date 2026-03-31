'use strict'

const HyperMQ = require('../')
const Corestore = require('corestore')
const b4a = require('b4a')
const tmp = require('test-tmp')

function label (mq) {
  return b4a.toString(mq.autobee.local.key, 'hex').slice(0, 8)
}

const main = async () => {
  const stores = await Promise.all([tmp(), tmp(), tmp()].map(p => p.then(d => new Corestore(d))))

  const producer = new HyperMQ(stores[0], { producer: true })
  await producer.ready()

  const consumers = []
  const streams = []

  for (let i = 1; i < stores.length; i++) {
    const consumer = new HyperMQ(stores[i], producer.key)
    await consumer.ready()
    consumers.push(consumer)
  }

  console.log('Producer:', label(producer))
  consumers.forEach((c, i) => console.log(`Consumer ${i + 1}: ${label(c)}`))
  console.log()

  consumers.forEach((c, i) => {
    c.subscribe('jobs', async (msg) => {
      const task = b4a.toString(msg.data)
      console.log(`[Consumer ${i + 1}] Processing: ${task}`)
      await new Promise(resolve => setTimeout(resolve, 5000))
      console.log(`[Consumer ${i + 1}] Finished:   ${task}`)
    })
  })

  const tasks = [
    'resize-image',
    'send-email',
    'generate-report',
    'compress-video',
    'run-backup'
  ]

  const ackPromises = []

  for (const task of tasks) {
    const key = await producer.publish('jobs', task, { concurrent: 1 })
    console.log('Submitted:', task)

    producer.waitAck(key).then(ack => {
      const id = b4a.toString(ack.consumer, 'hex').slice(0, 8)
      console.log(`  -> "${task}" acked by ${id}`)
    })

    ackPromises.push(producer.waitAck(key))
  }

  await Promise.all(ackPromises)
  console.log('\nAll tasks processed.')

  for (const c of consumers) await c.close()
  await producer.close()
  for (const s of streams) s.destroy()
}

main().catch(console.error)
