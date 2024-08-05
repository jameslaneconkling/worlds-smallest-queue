# World's Smallest Queue
A stupid-simple Postgres-backed queue

## Goals
- **partitionable messages**: customizable partitioning of messages. transparently balance message throughput across each partition. efficiently expose all messages or message counts per partition.
- **scalable enqueueing**: reliably handle large message spikes
- **lightweight management**: minimize operational overhead
- **minimal surface area**: under 200 LOC, minimal dependencies (postgres has already [done all the work](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/))

## Non-Goal
- **maximize message throughput or latency**: this library is intended for situations where the queue is fronting expensive business logic like a complex query to a resource-constrained system. the queue should handle significant message spikes and effectively rate limit message processing, but is not intended for scenarios where queue throughput itself is expected to be the main bottleneck.

## Terminology
- **message**: a single serializable datastructure to enqueue and dequeue
- **partition**: an arbitrary subset of messages defined during message creation. as long as partitions remain reletively small/high cardinality, retrieving counts and lists of unprocessed messages per partition should remain efficient

## Usage
```ts
import { pool } from 'pg'
import { messenger, Message } from 'worlds-smallest-queue'

type MessageBody = { ... }

const MESSENGER_COUNT = 10

const CONFIG = { errorRetryInterval: 2000, pollInterval: 2000, maxRetryCount: 10, jobTimeout: 36000 }

const run = async () => {
  const pool = new Pool({ max: MESSENGER_COUNT })

  await createQueue()

}

run()
  .then()

for (let messengerId = 0; messengerId < MESSENGER_COUNT; messengerId++) {
  messenger<Message<MessageBody>>(
    (message) => new Promise((resolve) => setTimeout(resolve, 2000)),
    messengerId,
    pool,
    CONFIG
  )
}
```
