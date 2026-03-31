'use strict'

const HyperMQ = require('../')
const Corestore = require('corestore')
const b4a = require('b4a')
const tmp = require('test-tmp')

const main = async () => {
  const store = new Corestore(await tmp())

  const mq = new HyperMQ(store)
  await mq.ready()

  const results = []
  let count = 0
  const total = 5

  const done = new Promise(resolve => {
    mq.subscribe('data', (msg) => {
      results.push(msg.data)
      if (++count === total) resolve()
    })
  })

  console.log('Publishing different data types...\n')

  await mq.publish('data', 'plain string')
  await mq.publish('data', b4a.from([0xDE, 0xAD, 0xBE, 0xEF]))
  await mq.publish('data', { name: 'Alice', age: 30 })
  await mq.publish('data', [1, 2, 3])
  await mq.publish('data', 42)

  await done

  console.log('String:       ', b4a.toString(results[0]))
  console.log('Buffer (hex):  ', b4a.toString(results[1], 'hex'))
  console.log('JSON object:  ', JSON.parse(b4a.toString(results[2])))
  console.log('JSON array:   ', JSON.parse(b4a.toString(results[3])))
  console.log('JSON number:  ', JSON.parse(b4a.toString(results[4])))

  await mq.close()
  console.log('\nDone.')
}

main().catch(console.error)
