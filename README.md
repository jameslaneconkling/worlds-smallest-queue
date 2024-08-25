# World's Smallest Queue
A stupid-simple Postgres-backed queue for Typescript

## Goals
- **messages partitioning**: balance message throughput across each partition. efficiently expose all messages or message count per partition.
- **scalable enqueueing**: reliably enqueue bulk messages transactionally
- **lightweight management**: minimize operational overhead
- **minimal surface area**: under 200 LOC, minimal dependencies (postgres has already [done all](https://www.postgresql.org/docs/current/sql-notify.html) [the work](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/))


## Non-Goal
- **orchestrate queue instances**: queues are easy to spin up and tear down. orchestrating those queue instances is left to the application, whether those instances are run in the same Node process, across Node workers, or across different servers.
- **maximize message throughput or minimize latency**: this library is intended for situations where the queue is fronting expensive business logic like a complex query to a resource-constrained system. the queue can handle significant message spikes and effectively rate limit message processing, but is not intended for scenarios where queue throughput itself is the main bottleneck. that said, the queue _is_ fast, because Postgres is fast: messages have millisecond latency and throughputs of thousands of messages per second.


## Terminology
- **message**: a single serializable datastructure to enqueue and dequeue
- **partition**: an arbitrary subset of messages defined during message creation. as long as partitions remain reletively small/high cardinality, retrieving counts and lists of unprocessed messages per partition should remain efficient. potential partition values could be a user id, an ip address, or anything property that is distributed evenly across all messages. partitioning is not required.
- **queue instance**: a single lightweight instance of the queue that dequeues messages, processes them, and returns them to the queue instance on error. queue instances can be created and destroyed efficiently and can run anywhere that has access to the backing Postgres database. multiple instances can run efficiently in the same process or distributed across multiple processes.


## Usage
```ts
import { Pool } from 'pg'
import { enqueue, Queue } from 'worlds-smallest-queue'

/*
 * Instantiate Queue
 */
const pool = new Pool()
const QUEUE_INSTANCE_COUNT = 10
const CONFIG = { pool, messageTimeout: 60000, messageErrorRetryInterval: 2000, maxMessageRetryCount: 10 }

for (let i = 0; i < QUEUE_INSTANCE_COUNT; i++) {
  const queue = Queue(
    (message) => {
      // process message, returning a promise that resolves on success and rejects on error
      new Promise((resolve) => setTimeout(resolve, 2000))
    },
    CONFIG
  )

  queue.catch((error) => {
    console.error('queue failure', error)
    process.exit(1)
  })
}

/**
 * Elsewhere, in the same application or not, enqueue messages
 */
const messages = [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }]
const partition = 1
let client: PoolClient | undefined
try {
  client = await pool.connect()
  await enqueue(client, messages, partition)
} catch (error) {
  console.error(error)
} finally {
  client?.release()
}
```

## Installation
```sh
npm install pg worlds-smallest-queue
```


## Documentation

### Queue
`Queue(handler: Handler, config: Config) => CancellablePromise<void>`

Create a new queue instance that dequeues messages, processes them via the `hander` argument, and removes them from the queue on success or retries them with an exponential backoff on failure. Returns a promise that rejects if the queue errors out and ends.
```ts
import { Pool } from 'pg'
import { Queue } from 'worlds-smallest-queue'

const pool = new Pool()
const queue = Queue(async (message) => { await doWorkBeCool(message) }, { pool })

queue.catch((error) => {
  console.error('queue failure', error)
  process.exit(1)
})
```

To process multiple messages at the same time, create multiple queues. Ensure the pool connection count is at least equal to the number of queue instances, and greater if other parts of the application need to query Postgres as well.
```ts
import { Pool } from 'pg'
import { Queue } from 'worlds-smallest-queue'

const pool = new Pool()

for (let i = 0; i < 5; i++) {
  Queue(async (message) => { await lookAliveMoveFast(message) }, { pool, instanceId: `${id}` })
}
```

If message processing is CPU bound, distribute the queue instances across multiple Node workers or multiple machines.

The `Queue` constructor function returns a promise with a `teardown` method to manually tear down the queue, e.g. to resize the number of running queues, or to destroy the queue after running integration tests. You should not need to manually destroy the queue under most operating patterns.

```ts
import { Pool } from 'pg'
import { Queue } from 'worlds-smallest-queue'

const pool = new Pool()
const queue = Queue(async (message) => { await doWorkBeCool(message) }, { pool })

queue.catch((error) => {
  console.error('queue failure', error)
  process.exit(1)
})

// manually stop the queue from consuming new messages
await queue.teardown()
```

The queue will reject if it fails to dequeue a message `Config.maxQueueRetryCount [default: 30]` times in a row, including if it takes more than `Config.queueTimeout [default: 5min]` to dequeue a message. Persistent errors like this suggest that either Postgres is down or unreachable, or that the client pool has leaked all available clients. Typically the best course of action in cases like this is to crash and restart the server using some type of process manager. Disable this behavior by setting `Config.maxQueueRetryCount` to `Infinity`.


### enqueue
`enqueue(client: ClientBase, messages: Message<Body>[], partition: number, awaitMessage?: number) => Promise<{ ids: number[], awaited?: boolean }>`

Enqueue messages to partition. Returns message ids and whether or not awaiting the message processing timed out.

```ts
import { Pool } from 'pg'
import { enqueue } from 'worlds-smallest-queue'

const pool = new Pool()
const messages = [{ work: 'it' }]
const partitions = 1

let client: PoolClient | undefined
try {
  client = await pool.connect()
  await enqueue(client, messages, partition)
} catch (error) {
  console.error(error)
} finally {
  client?.release()
}
```

If the 4th optional `awaitMessage` argument is passed, waits for up to `awaitMessage` ms for all messages to be processed successfully. If the queue partition is backed up, or messages are processed slowly, or repeated errors slow or fail message processing, enqueue promise is more likely to resolve on timeout with `awaited: false`. But if the partition is not congested, messages are processed quickly, and there are no errors, then awaiting enqueued messages allows clients to know when those messages have been processed successfully.

```ts
import { Pool } from 'pg'
import { enqueue } from 'worlds-smallest-queue'

const pool = new Pool()
const messages = [{ work: 'it' }, { work: 'it' }]
const partitions = 1

let client: PoolClient | undefined
try {
  client = await pool.connect()
  const { ids, awaited } = await enqueue(client, messages, partition, 10_000)
  console.log(awaited ? `successfully processed messages ${ids.join(',')}` : `still waiting to process messages ${ids.join(',')}`)
} catch (error) {
  console.error(error)
} finally {
  client?.release()
}
```


### setupPGQueue
`setupPGQueue() => string`

Return a SQL query string to set up the queue tables. The queue schema is trivial so you can also create the tables by hand via any schema migration tool.

```ts
import { Pool } from 'pg'
import { setupPGQueue } from 'worlds-smallest-queue'

const pool = new Pool()
await pool.query(setupPGQueue())
```


### teardownPGQueue
`teardownPGQueue() => string`

Return a SQL query string to tear down queue tables. Similarly, the `DROP TABLES` command is trivial, meaning you don't necessarily need to use Typescript to manage the schema.

```ts
import { Pool } from 'pg'
import { teardownPGQueue } from 'worlds-smallest-queue'

const pool = new Pool()
await pool.query(teardownPGQueue())
```


## Types

### Config
Queue configuration
```ts
type Config = {
  // [required] a pg Pool instance.
  pool: Pool
  // [default: 1min] ms timeout to dequeue and process a message. messages that timeout fail and are re-enqueued.
  messageTimeout: number
  // [default: 15] number of times to retry a message before adding it to the `dead_messages` table.
  maxMessageRetryCount: number
  // [default: 2sec] ms retry interval after a failed message. if a message fails multiple times, each subsequent retry interval doubles.
  messageErrorRetryInterval: number
  // [default: 5min] ms timeout for the queue. setting this to a lower interval than messageTimeout, dequeueInterval, or messageErrorRetryInterval will set messageErrorRetryInterval to the maximum of all three.
  queueTimeout: number
  // [default: 30] number of times to retry the queue loop before it errors out.
  maxQueueRetryCount: number
  // [default 2sec] ms retry interval after a failed queue loop.
  queueErrorRetryInterval: number
  // [default: 30sec] ms interval to wait for new messages in the queue.
  dequeueInterval: number
  // [optional] queue instance id. used for error logging when there are multiple queues.
  instanceId: string
  // [default: 'ERROR'] whether to log all logs or only error logs.
  logLevel: 'ERROR' | 'INFO'
  // [optional] custom logger.
  logger: Logger
}
```


### Message
Dequeued message, with the enqueued body payload under the `body` field.
```ts
type Message<Body = unknown> = {
  id: number,
  partition: number,
  body: Body
  retry_count: number,
  created_at: Date,
  retry_at: Date,
}
```


### Handler
Async message processing handler. Receives the dequeued message and the pg client session for the dequeue transaction. Returns a promise should resolve if the message was successfully processed, and reject if not.
```ts
type Handler<Body> = (message: Message<Body>, client: ClientBase) => Promise<unknown>
```


### CancellablePromise
Promise with a teardown method. Used by the `Queue` constructor function to manually destroy the queue instance.
```ts
export type CancellablePromise<T> = Promise<T> & { teardown: () => Promise<T> }
```


### Logger
Library logger interface. Pass a custom logger to the Queue Config via the `logger` field to customize logging.
```ts
export type Logger = {
  info: (key: string, message?: Message, instanceId?: string) => void
  error: (key: string, error: unknown, message?: Message, instanceId?: string) => void
}
```


## See Also
- [PGQ](https://wiki.postgresql.org/wiki/PGQ_Tutorial)
- [Graphile Worker](https://worker.graphile.org/)
- [PGMQ](https://pgt.dev/extensions/pgmq)
