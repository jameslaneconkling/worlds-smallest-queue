# World's Smallest Queue
A stupid-simple Postgres-backed queue for Typescript

## Goals
- **messages partitioning**: balance message throughput across each partition. efficiently expose all messages or message count per partition.
- **scalable enqueueing**: reliably enqueue bulk messages transactionally
- **lightweight management**: minimize operational overhead
- **minimal surface area**: under 200 LOC, minimal dependencies (postgres has already [done all the work](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/))


## Non-Goal
- **maximize message throughput or minimize latency**: this library is intended for situations where the queue is fronting expensive business logic like a complex query to a resource-constrained system. the queue can handle significant message spikes and effectively rate limit message processing, but is not intended for scenarios where queue throughput itself is the main bottleneck. that said, this library _is_ fast, because Postgres is fast, and is capable of processing thousands of messages per second.


## Terminology
- **message**: a single serializable datastructure to enqueue and dequeue
- **partition**: an arbitrary subset of messages defined during message creation. as long as partitions remain reletively small/high cardinality, retrieving counts and lists of unprocessed messages per partition should remain efficient. potential partition values could be a user id, an ip address, or anything property that is distributed evenly across all messages. partitioning is not required.
- **queue**: a lightweight Queue instance that dequeues messages, processes them, and returns them to the queue on error. Queue instances can be created and destroyed efficiently and can run anywhere that has access to the backing Postgres database. Multiple instances can run efficiently in the same process or distributed across multiple processes.


## Usage
```ts
import { Pool } from 'pg'
import { enqueue, Queue } from 'worlds-smallest-queue'

/*
 * Instantiate Queue
 */
const pool = new Pool()
const QUEUE_INSTANCE_COUNT = 10
const CONFIG = { pool, messageTimeout: 60000, errorRetryInterval: 2000, maxRetryCount: 10 }

for (let i = 0; i < QUEUE_INSTANCE_COUNT; i++) {
  Queue(
    (message) => {
      // process message, returning a promise that resolves on success and rejects on error
      new Promise((resolve) => setTimeout(resolve, 2000))
    },
    CONFIG
  )
}

/**
 * Elsewhere, in the same application or not, enqueue messages
 */
const messages = [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }]
const partition = 1
await pool.query(enqueue(messages, partition))
```

## Installation
```sh
npm install pg worlds-smallest-queue
```


## Documentation

### Queue
`Queue(handler: Handler, config: Config) => () => Promise<void>`

Create a new queue instance that dequeues messages, processes them via the `hander` argument, and removes them from the queue on success or retries them with an exponential backoff on failure. Returns a teardown function to destroy the queue.
```ts
import { Pool } from 'pg'
import { Queue } from 'worlds-smallest-queue'

const pool = new Pool()

const teardown = Queue(async (message) => { await doWorkBeCool(message) }, { pool })

// to destroy the queue
await destroy()
```

To process multiple messages at the same time, create multiple queues. Ensure the pool connection count is at least equal to the number of queue instances, and greater if other parts of the application need to query Postgres as well.
```ts
import { Pool } from 'pg'
import { Queue } from 'worlds-smallest-queue'

const pool = new Pool({ max: 20 })

for (let i = 0; i < 10; i++) {
  Queue(async (message) => { await lookAliveMoveFast(message) }, { pool, instanceId: `${id}` })
}
```

If message processing is CPU bound, distribute the queue instances across multiple Node workers or multiple machines.

### enqueue
`enqueue(messages: Message<Body>[], partition: number) => string`

Return a SQL query string to enqueue messages to a single partition.

```ts
import { Pool } from 'pg'
import { enqueue } from 'worlds-smallest-queue'

const pool = new Pool()
const messages = [{ work: 'it' }, { work: 'it' }]
const partitions = 1

await pool.query(enqueue(messages, partition))
```

### setupPGQueue
`setupPGQueue() => string`

Return a SQL query string to set up the queue tables. The queue schema is trivial, so you can also create the tables by hand via any schema migration tool.

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
  // [default: 60_000ms] ms timeout to dequeue and process a message. messages that timeout fail and are re-enqueued.
  messageTimeout: number
  // [default: 15] number of times to retry a message before adding it to the `dead_messages` table.
  maxRetryCount: number
  // [default: 2_000ms] ms retry interval after a failed message. if a message fails multiple times, each subsequent retry interval doubles.
  errorRetryInterval: number
  // [default: 30_000ms] ms interval to wait for new messages in the queue.
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
