'use strict'

const HyperMQ = require('../')
const Corestore = require('corestore')
const b4a = require('b4a')
const tmp = require('test-tmp')

function label (mq) {
  return b4a.toString(mq.autobee.local.key, 'hex').slice(0, 8)
}

const main = async () => {
  const stores = await Promise.all([tmp(), tmp()].map(p => p.then(d => new Corestore(d))))

  const producer = new HyperMQ(stores[0], { producer: true })
  await producer.ready()

  const consumer = new HyperMQ(stores[1], producer.key)
  await consumer.ready()

  console.log('Producer:', label(producer))
  console.log('Consumer:', label(consumer))
  console.log()

  // Consumer subscribes to 'jobs' and reports status via msg.setStatus
  consumer.subscribe('jobs', async (msg) => {
    const task = b4a.toString(msg.data)
    console.log(`[Consumer] Starting: ${task}`)

    msg.setStatus({ status: 'running', progress: 0 })
    await new Promise(resolve => setTimeout(resolve, 500))

    msg.setStatus({ status: 'running', progress: 50 })
    await new Promise(resolve => setTimeout(resolve, 500))

    msg.setStatus({ status: 'done', progress: 100 })
    console.log(`[Consumer] Finished: ${task}`)
  })

  // Producer watches all status updates (wildcard)
  producer.onStatus((key, state) => {
    const hex = b4a.toString(key, 'hex').slice(0, 8)
    const status = JSON.parse(b4a.toString(state))
    console.log(`[Producer] Status for ${hex}:`, status)
  })

  const tasks = ['resize-image', 'send-email', 'generate-report']

  for (const task of tasks) {
    console.log(`\n[Producer] Publishing: ${task}`)
    const key = await producer.publish('jobs', task, { concurrent: 1 })

    // Also subscribe to this specific job's status
    producer.onStatus(key, (k, state) => {
      const status = JSON.parse(b4a.toString(state))
      if (status.progress === 100) {
        console.log(`[Producer] Job ${task} completed!`)
      }
    })

    const ack = await producer.waitAck(key)
    const id = b4a.toString(ack.consumer, 'hex').slice(0, 8)
    console.log(`[Producer] "${task}" acked by ${id}`)
  }

  console.log('\nAll tasks processed.')

  await consumer.close()
  await producer.close()
}

main().catch(console.error)
