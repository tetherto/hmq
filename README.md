# HyperMQ

Simple message queue backed by [Autobee](https://github.com/holepunchto/autobee).

HyperMQ is a peer-to-peer message queue that uses [Hyperswarm](https://github.com/holepunchto/hyperswarm) for discovery and Autobee for persistent, replicated storage. It supports **pub/sub** (fan-out) and **work queue** (competing consumer) messaging patterns.

## Install

```
npm install hypermq
```

You will also need [corestore](https://github.com/holepunchto/corestore) for storage:

```
npm install corestore
```

## Quick Start

```js
const Corestore = require('corestore')
const HyperMQ = require('hypermq')
const b4a = require('b4a')

const store = new Corestore('./my-storage')

const mq = new HyperMQ(store)
await mq.ready()

mq.subscribe('greetings', (msg) => {
  console.log(b4a.toString(msg.data)) // 'hello world'
})

await mq.publish('greetings', 'hello world')

await mq.close()
```

## API

### `const mq = new HyperMQ(corestore, key?, opts?)`

Create a new HyperMQ instance.

- `corestore` - a [Corestore](https://github.com/holepunchto/corestore) instance
- `key` - optional Buffer or hex string to join an existing queue (omit to create a new one)
- `opts` - optional configuration object

| Option | Type | Default | Description |
|---|---|---|---|
| `producer` | boolean | `false` | Producer-only mode. Will not consume work queue messages. |
| `concurrent` | number | `0` | Default concurrent workers for published messages. `0` means pub/sub mode. |
| `timeout` | number | `5000` | Timeout (ms) for work queue claim evaluation. |
| `keyPair` | object | `null` | Custom key pair for this instance. |
| `swarm` | Hyperswarm | `null` | Custom Hyperswarm instance. Creates its own if not provided. |
| `maxOrphanAcks` | number | `10000` | Maximum orphan acks to keep in memory. |

### `await mq.ready()`

Ensure the instance is initialized. Called automatically by `publish`, `flush`, `addWriter`, and `removeWriter`.

### `const key = await mq.publish(topic, data, opts?)`

Publish a message.

- `topic` - non-empty string
- `data` - payload (string, Buffer, object, array, number, boolean, or null)
- `opts.concurrent` - override the instance-level `concurrent` setting for this message

Returns a 32-byte Buffer key identifying the message.

### `mq.subscribe(topic, callback)`

Subscribe to messages on a topic.

The callback receives a message object:

```js
{
  topic,      // string
  data,       // Buffer
  key,        // Buffer (32 bytes)
  concurrent, // number
  setStatus   // function (see below)
}
```

#### `msg.setStatus(status)`

Every message object includes a `setStatus` helper. Calling it appends a status update for the message to the replicated log. The status is persisted in the view and propagated to all peers reactively.

- `status` - Buffer, string, or JSON-serializable value

Only has effect when the instance is writable. Last write wins in the view.

For work queue messages (`concurrent > 0`), the ack is sent in parallel to the callback execution.

### `mq.unsubscribe(topic, callback?)`

Remove a subscription. If `callback` is omitted, all subscribers for the topic are removed.

### `const ack = await mq.waitAck(key?)`

Wait for an acknowledgment.

- `key` - 32-byte Buffer from `publish()`. If omitted, waits for the next ack on any message.

Returns `{ consumer }` where `consumer` is a Buffer identifying the acknowledging peer, or `null` if the instance closes before an ack arrives.

### `await mq.addWriter(key)`

Authorize a remote peer to write to the queue.

- `key` - 32-byte Buffer or hex string (the remote peer's writer key)

### `await mq.removeWriter(key)`

Revoke write access for a peer.

### `mq.onStatus(key?, callback)`

Subscribe to status updates. The callback is invoked whenever a status update is applied (locally or via replication).

- `key` - optional 32-byte Buffer or hex string. If provided, only status updates for that message trigger the callback. If omitted, all status updates trigger it.
- `callback(key, status)` - `key` is the 32-byte message Buffer, `status` is a Buffer.

### `mq.offStatus(key?, callback?)`

Remove a status subscription. If `key` is omitted, removes from the wildcard set. If `callback` is also omitted, removes all subscriptions for that key (or all wildcard subscriptions).

### `await mq.flush()`

Flush all pending operations to the underlying Autobee.

### `await mq.close()`

Close the instance and release all resources. Pending `waitAck` promises resolve to `null`.

### Properties

| Property | Type | Description |
|---|---|---|
| `mq.key` | Buffer | 32-byte queue key (available after `ready()`) |
| `mq.discoveryKey` | Buffer | Discovery key used for Hyperswarm (available after `ready()`) |
| `mq.writable` | boolean | Whether this instance can write to the queue |

### Events

#### `mq.on('warning', err)`

Emitted on non-fatal errors (malformed entries, failed callbacks, etc.).

## Messaging Patterns

### Pub/Sub

When `concurrent` is `0` (the default), messages are delivered to **all** subscribers on a topic. This is a fan-out broadcast pattern.

```js
mq.subscribe('news', (msg) => {
  console.log('Subscriber A:', b4a.toString(msg.data))
})

mq.subscribe('news', (msg) => {
  console.log('Subscriber B:', b4a.toString(msg.data))
})

await mq.publish('news', 'breaking story')
// Both subscribers receive the message
```

### Work Queue

When `concurrent` is greater than `0`, messages are delivered to a **single** consumer at a time. Consumers are selected by XOR distance from the message key, with timeout-based fallback. The ack is sent automatically after the subscriber callback completes.

```js
mq.subscribe('jobs', async (msg) => {
  await processTask(b4a.toString(msg.data))
  // Ack is sent after this callback resolves
})

const key = await mq.publish('jobs', 'resize-image-42', { concurrent: 1 })
const ack = await mq.waitAck(key)
console.log('Processed by:', b4a.toString(ack.consumer, 'hex'))
```

### Job Status Updates

Any subscriber (pub/sub or work queue) can update the status of a message by calling `msg.setStatus(status)`. Status updates are appended to the replicated log, persisted in the view, and propagated to all peers reactively -- no polling required.

Producers (or any peer) can subscribe to status changes with `mq.onStatus()`:

```js
// Consumer reports progress
consumer.subscribe('jobs', async (msg) => {
  msg.setStatus({ status: 'running', progress: 0 })
  await doWork()
  msg.setStatus({ status: 'running', progress: 50 })
  await doMoreWork()
  msg.setStatus({ status: 'done', progress: 100 })
})

// Producer subscribes to a specific job's status
const key = await producer.publish('jobs', 'resize-image', { concurrent: 1 })
producer.onStatus(key, (k, state) => {
  console.log('Status:', JSON.parse(b4a.toString(state)))
})

// Or subscribe to all status updates
producer.onStatus((key, state) => {
  console.log('Any status:', JSON.parse(b4a.toString(state)))
})
```

See `examples/job-status.js` for a full working example.

## Data Types

`publish()` accepts strings, Buffers, objects, arrays, numbers, booleans, and `null`. Non-Buffer values are JSON-encoded before storage. The subscriber always receives `msg.data` as a Buffer:

```js
await mq.publish('topic', { hello: 'world' })

mq.subscribe('topic', (msg) => {
  const obj = JSON.parse(b4a.toString(msg.data))
  // { hello: 'world' }
})
```

## License

Apache-2.0
