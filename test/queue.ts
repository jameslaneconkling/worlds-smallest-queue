import test from 'tape'
import pg from 'pg'
import { enqueue, Queue, setupPGQueue } from '../src/index.js'
import { poll, sleep, sortFlatObjectList as sort } from '../src/utils.js'
import { Message } from '../src/types.js'


if (process.env.PG_PORT === undefined) {
  console.error('missing PG_PORT env variable')
  process.exit(1)
} else if (isNaN(parseInt(process.env.PG_PORT, 10))) {
  console.error(`malformed PG_PORT env variable. expected a number. received ${parseInt(process.env.PG_PORT, 10)}`)
  process.exit(1)
}


const pool = new pg.Pool({
  user: 'postgres',
  database: 'worlds-smallest-queue-test',
  password: 'test',
  port: parseInt(process.env.PG_PORT!, 10),
  max: 25,
})


test('setup queue tests', async (t) => {
  t.plan(1)

  try {
    await pool.query('DROP TABLE messages')
  } catch (error) {
    if (error !== null && typeof error === 'object' && 'routine' in error && error.routine !== 'DropErrorMsgNonExistent') {
      console.error(error)
    }
  }
  try {
    await pool.query('DROP TABLE dead_messages')
  } catch (error) {
    if (error !== null && typeof error === 'object' && 'routine' in error && error.routine !== 'DropErrorMsgNonExistent') {
      console.error(error)
    }
  }
  
  await pool.query(setupPGQueue())

  t.pass('setup complete')
})


test('enqueues messages', async (t) => {
  t.plan(1)

  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc', count: 1, vip: true }, { event: 'def', count: 1, vip: true }, { event: 'abc', count: 1, vip: true }], 1)
  await enqueue(client, [{ event: 'ghi', count: 1, vip: true }, { event: 'def', count: 1, vip: true }], 2)
  client.release()

  t.deepEquals(
    sort(
      (await pool.query<{ partition: number, event: string }>(`SELECT partition, body->'event' AS event FROM messages`)).rows,
    ),
    sort([
      { event: 'abc', partition: 1 },
      { event: 'def', partition: 1 },
      { event: 'abc', partition: 1 },
      { event: 'ghi', partition: 2 },
      { event: 'def', partition: 2 }
    ]),
    'enqueued all 5 messages'
  )

  await pool.query('TRUNCATE messages')
})


test('dequeues messages', async (t) => {
  t.plan(2)

  const messages: { partition: number, event: string }[] = []

  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], 1)

  const teardown = Queue(
    async (message: Message<{ event: string }>) => {
      messages.push({ partition: message.partition, event: message.body.event })
    },
    { pool, dequeueInterval: 1000 }
  )

  await enqueue(client, [{ event: 'ghi' }, { event: 'def' }], 2)
  client.release()

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 25, 40)

  t.deepEquals(
    sort(messages),
    sort([
      { event: 'abc', partition: 1 },
      { event: 'def', partition: 1 },
      { event: 'abc', partition: 1 },
      { event: 'ghi', partition: 2 },
      { event: 'def', partition: 2 }
    ]),
    'dequeued all 5 messages'
  )

  t.equals(
    (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count,
    0
  )

  await teardown()
})


test('handles async message processing', async (t) => {
  t.plan(1)

  const messages: { partition: number, event: string }[] = []

  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], 1)

  const teardown = Queue(
    async (message: Message<{ event: string }>) => {
      await sleep(100)
      messages.push({ partition: message.partition, event: message.body.event })
    },
    { pool, dequeueInterval: 1000 }
  )

  await enqueue(client, [{ event: 'ghi' }, { event: 'def' }], 2)
  client.release()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 25, 40)
  await teardown()

  t.deepEquals(
    sort(messages),
    sort([
       { partition: 1, event: 'abc' },
       { partition: 1, event: 'abc' },
       { partition: 1, event: 'def' },
       { partition: 2, event: 'def' },
       { partition: 2, event: 'ghi' }
    ]),
    `processed all ${messages.length} messages in queue`
  )
})


test('distributes work across multiple concurrent messengers', async (t) => {
  t.plan(6)

  const expectedMessageCount = 250
  const expectedQueueInstanceMessageCount: Record<number, number> = {}

  const client = await pool.connect()
  await enqueue(client, Array.from({ length: 50 }).map(() => ({ event: 'abc' })), 1)
  await enqueue(client, Array.from({ length: 50 }).map(() => ({ event: 'def' })), 2)

  const queues = Array.from({ length: 5 }).map((_, i) => {
    expectedQueueInstanceMessageCount[i] = 0
    return Queue(
      async () => {
        await sleep(10)
        expectedQueueInstanceMessageCount[i]++
      },
      { pool, dequeueInterval: 1000, instanceId: `${i}` }
    )
  })

  await sleep(10)
  await enqueue(client, Array.from({ length: 50 }).map(() => ({ event: 'ghi' })), 3)
  await sleep(10)
  await enqueue(client, Array.from({ length: 50 }).map(() => ({ event: 'def' })), 1)
  await sleep(10)
  await enqueue(client, Array.from({ length: 50 }).map(() => ({ event: 'abc' })), 4)
  client.release()

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 100)
  await Promise.all(queues.map((teardown) => teardown()))

  t.equals(
    expectedMessageCount,
    Object.values(expectedQueueInstanceMessageCount).reduce((a, b) => a + b),
    `processed all ${expectedMessageCount} messages in queue`
  )

  Object.entries(expectedQueueInstanceMessageCount).map(([instanceId, count]) => {
    t.ok(count > 0, `queue ${instanceId} processed ${count} messages`)
  })
})


test('distributes work across partitions', async (t) => {
  t.plan(1)

  let i = 0
  const partitions = new Set<number>()

  const client = await pool.connect()
  await enqueue(client, Array.from({ length: 100 }).map(() => ({})), 1)
  await enqueue(client, Array.from({ length: 100 }).map(() => ({})), 2)
  await enqueue(client, Array.from({ length: 100 }).map(() => ({})), 3)
  await enqueue(client, Array.from({ length: 100 }).map(() => ({})), 4)
  await enqueue(client, Array.from({ length: 100 }).map(() => ({})), 5)
  client.release()

  const teardown = Queue(
    async ({ partition }: Message) => {
      if (i++ < 50) {
        partitions.add(partition)
      }
    },
    { pool, dequeueInterval: 1000 }
  )

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 40)
  await teardown()
  t.equals(partitions.size, 5, 'queue processed at least one message from all 5 partitions after processing 50 messages')
})


test('retries failed messages with exponential backoff', async (t) => {
  t.plan(3)

  let retryCount = 0
  let createdAt: number | undefined
  let processedAt: number | undefined
  const retryInterval = 50
  const retryTime = (retryInterval * (2**0)) + (retryInterval * (2**1)) + (retryInterval * (2**2)) + (retryInterval * (2**3)) + (retryInterval * (2**4)) + (retryInterval * (2**5))

  const teardown = Queue(
    async (message) => {
      if (retryCount < 6) {
        retryCount++
        throw new Error('message_error')
      }
      createdAt = +message.created_at
      processedAt = Date.now()
    },
    { pool, dequeueInterval: 25, errorRetryInterval: retryInterval, maxRetryCount: 6 }
  )

  const client = await pool.connect()
  await enqueue(client, [{}], 1)
  client.release()
  await sleep(retryTime)
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 100)
  await teardown()

  const dt = processedAt! - createdAt!
  t.equals(retryCount, 6, 'retried message 6 times')
  t.ok(dt >= retryTime, `processed message in ${dt}ms after ${retryTime}ms backoff retry time`)
  t.ok(dt <= retryTime + 500, `processed message in ${dt}ms within 500ms of ${retryTime}ms backoff retry time`)
})


test('handles temporary message processing errors', async (t) => {
  t.plan(1)

  const messages: { partition: number, event: string }[] = []

  const client = await pool.connect()
  enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], 1)

  const queues = Array.from({ length: 2 }).map((_, i) => {
    return Queue(
      async (message: Message<{ event: string }>) => {
        await sleep(10)
        if (Math.random() < 0.2) {
          throw new Error('message_error')
        }
        messages.push({ partition: message.partition, event: message.body.event })
      },
      { pool, dequeueInterval: 1000, errorRetryInterval: 100, instanceId: `${i}` }
    )
  })

  await enqueue(client, [{ event: 'ghi' }, { event: 'def' }, { event: 'abc' }], 2)
  await sleep(20)
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], 3)
  await sleep(60)
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'ghi' }], 1)
  client.release()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 400)
  await Promise.all(queues.map((teardown) => teardown()))

  t.deepEquals(
    sort(messages),
    sort([
       { partition: 1, event: 'abc' },
       { partition: 1, event: 'abc' },
       { partition: 1, event: 'abc' },
       { partition: 2, event: 'abc' },
       { partition: 3, event: 'abc' },
       { partition: 3, event: 'abc' },
       { partition: 1, event: 'def' },
       { partition: 1, event: 'def' },
       { partition: 2, event: 'def' },
       { partition: 3, event: 'def' },
       { partition: 1, event: 'ghi' },
       { partition: 2, event: 'ghi' }
    ]),
    `processed all ${messages.length} messages in queue`
  )
})


test('handles persistent message processing errors, sending messages to the dead_messages table when retry limit is reached', async (t) => {
  t.plan(3)

  const aliveMessages: { event: string }[] = []
  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], 1)
  client.release()

  const teardown = Queue(
    async (message: Message<{ event: string }>) => {
      if (message.body.event === 'abc') {
        throw new Error('message_error')
      } else {
        aliveMessages.push(message.body)
      }
    },
    { pool, dequeueInterval: 1000, errorRetryInterval: 10, maxRetryCount: 5 }
  )

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 100)
  await teardown()

  const deadMessages = (await pool.query<{ retry_count: number }>('SELECT retry_count FROM dead_messages')).rows
  t.equals(deadMessages.length, 2, '2 messages died')
  t.ok(deadMessages.every(({ retry_count }) => retry_count === 5), 'retried each dead message 5 times')
  t.deepEquals(aliveMessages, [{ event: 'def' }], '1 message succeeded')
})


test('handles network connection failure', async (t) => {
  t.plan(1)

  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }], 1)
  client.release()

  const brokenPool = new pg.Pool({ user: 'broken', database: 'broken', password: 'broken', port: 0 })
  const teardownBrokenQueue = Queue(
    async () => {},
    { pool: brokenPool, dequeueInterval: 1000 }
  )

  await teardownBrokenQueue()
  await brokenPool.end()
  await pool.query('DELETE FROM messages')
  t.pass('queue can recover from bad connection pool')
})


test('handles temporary network partition from PG', async (t) => {
  t.plan(1)

  t.ok('yup', 'TODO')
})


test('handles processing transaction timeouts', async (t) => {
  t.plan(1)

  let i = 0
  const messages: Message<{ event: string }>[] = []
  const client = await pool.connect()
  await enqueue(client, [{ event: 'abc' }, { event: 'def' }], 1)
  client.release()

  const teardown = Queue(
    async (message: Message<{ event: string }>) => {
      if (i++ === 1) {
        await sleep(1000)
      } else {
        messages.push(message)
      }
    },
    { pool, dequeueInterval: 1000, messageTimeout: 500, errorRetryInterval: 500 }
  )

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 1000, 10)
  await teardown()

  t.deepEquals(messages.map(({ body }) => body), [{ event: 'abc' }, { event: 'def' }], 'processed 2 messages after timeouts')
})


test('detect new messages on the queue within the polling interval', async (t) => {
  t.plan(1)

  const dequeueTimes: number[] = []
  const teardown = Queue(
    async (message: Message) => {
      const dequeueTime = Date.now() - +message.created_at
      console.log(`message dequeue time of ${dequeueTime}ms`)
      dequeueTimes.push(dequeueTime)
    },
    { pool, dequeueInterval: 1000 }
  )

  const client = await pool.connect()
  for (let i = 0; i < 20; i++) {
    await enqueue(client, [{}], 1)
    await sleep(10)
  }
  client.release()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 50, 10)
  await teardown()

  const averageDequeueTime = dequeueTimes.reduce((a, b) => a + b, 0) / dequeueTimes.length
  t.ok(averageDequeueTime < 1000, `average message dequeue time of ${averageDequeueTime}ms is less than 1s`)
})


test('waits for queue to process awaited messages when enqueueing with awaitMessage argument', async (t) => {
  t.plan(2)

  const teardown = Queue(async () => sleep(10), { pool, dequeueInterval: 2000 })

  const client = await pool.connect()
  await enqueue(client, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}], 1)
  const t0 = Date.now()
  const messageIds = await enqueue(client, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}], 1, 1000)
  const dt = Date.now() - t0
  await enqueue(client, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}], 1)
  const messageCount = (await client.query<{ count: number }>('SELECT count(*)::int FROM messages WHERE id = ANY($1)', [messageIds])).rows[0].count
  client.release()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 20)
  await teardown()

  t.ok(dt < 1000, `time to dequeue awaited messages of ${dt}ms is less than under 1s`)
  t.equals(messageCount, 0, 'waited for the queue to process all 15 messages')
})


test('times out waiting for queue to process awaited messages when message processing takes longer than awaitMessage', async (t) => {
  t.plan(5)

  const messageIds: number[] = []

  const teardown = Queue(
    async (message) => {
      messageIds.push(message.id)
      return sleep(200)
    },
    { pool, dequeueInterval: 2000 }
  )

  const client = await pool.connect()
  await enqueue(client, [{}, {}, {}, {}], 1)
  const t0 = Date.now()
  const awaitedMessageIds = await enqueue(client, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}], 1, 2000)
  const dt = Date.now() - t0
  const remainingMessageIds = (await client.query<{ id: number }>('SELECT id FROM messages WHERE id = ANY($1)', [awaitedMessageIds])).rows.map(({ id }) => id)
  const processedMessageIds = messageIds.filter((id) => awaitedMessageIds.includes(id))
  await enqueue(client, [{}, {}, {}], 1)
  client.release()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 20)
  await teardown()

  t.equals(awaitedMessageIds.length, 10, 'enqueued 10 messages')
  t.ok(dt >= 2000 && dt < 2050, `awaited messages timed out in ${dt}ms`)
  t.ok(
    processedMessageIds.length > 0 && processedMessageIds.length < awaitedMessageIds.length,
    `processed ${processedMessageIds.length} of ${awaitedMessageIds.length} messages`
  )
  t.ok(
    remainingMessageIds.length > 0 && remainingMessageIds.length < awaitedMessageIds.length,
    `timed out before processing remaining ${remainingMessageIds.length} of ${awaitedMessageIds.length} messages`
  )
  t.ok(
    new Set([...processedMessageIds, ...remainingMessageIds]).size === awaitedMessageIds.length,
    `processed message count ${processedMessageIds.length} plus remaining message count of ${remainingMessageIds.length} is equal to total message count of ${awaitedMessageIds.length}`
  )
})



test('handles high throughput sync message processing without dropping messages', async (t) => {
  t.plan(1)

  const expectedMessages: { partition: number, event: string }[] = []
  const actualMessages: { partition: number, event: string }[] = []

  const messages = Array.from({ length: 20000 }).map(() => ({ event: 'abc' }))
  expectedMessages.push(...messages.map(({ event }) => ({ partition: 1, event })))
  
  const client = await pool.connect()
  const t0 = Date.now()
  await enqueue(client, messages, 1)
  console.log(`Enqueued ${expectedMessages.length} messages. Elapsed Time: ${Date.now() - t0}ms`)
  client.release()

  const queues = Array.from({ length: 20 }).map((_, i) => {
    return Queue(
      async (message: Message<{ event: string }>) => {
        actualMessages.push({ partition: message.partition, event: message.body.event })
      },
      { pool, dequeueInterval: 1000, instanceId: `${i}` }
    )
  })

  const t1 = Date.now()
  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 500, 2 * 30) // 30sec timeout
  console.log(`Processed ${actualMessages.length} messages. Elapsed Time: ${Date.now() - t1}ms`)
  await Promise.all(queues.map((teardown) => teardown()))

  t.deepEquals(
    sort(actualMessages),
    sort(expectedMessages),
    `processed all ${actualMessages.length} messages in queue`
  )
})

test('handles high throughput async message processing without dropping messages', async (t) => {
  t.plan(1)

  const expectedMessages: { partition: number, event: string }[] = []
  const actualMessages: { partition: number, event: string }[] = []

  const queues = Array.from({ length: 20 }).map((_, i) => {
    return Queue(
      async (message: Message<{ event: string }>) => {
        await sleep(Math.random() * 50)
        actualMessages.push({ partition: message.partition, event: message.body.event })
      },
      { pool, dequeueInterval: 1000, instanceId: `${i}` }
    )
  })

  const client = await pool.connect()
  const t1 = Date.now()
  for (let i = 0; i < 10; i++) {
    const messages = Array.from({ length: 500 }).map(() => ({ event: 'abc' }))
    expectedMessages.push(...messages.map(({ event }) => ({ partition: i, event })))
    await enqueue(client, messages, i)
    await sleep(Math.random() * 50)
  }
  client.release()

  await poll(async () => (await pool.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0, 100, 500)
  console.log(`Processed ${actualMessages.length} messages. Elapsed Time: ${Date.now() - t1}ms`)
  await Promise.all(queues.map((teardown) => teardown()))

  t.deepEquals(
    sort(actualMessages),
    sort(expectedMessages),
    `processed all ${actualMessages.length} messages in queue`
  )
})


test('is not vulnerable to sql injection', async (t) => {
  t.plan(1)

  const injection = [{ x: `1"}'); DROP TABLE messages --` }]
  
  const client = await pool.connect()
  try {
    await enqueue(client, injection, 1)
  } catch (error) {
    if (error !== null && typeof error === 'object' && 'routine' in error && error.routine !== 'json_errsave_error') {
      console.error(error)
    } else {
      t.fail(error)
    }
  }
  client.release()

  const messageTableExists = (await pool.query<{ exists: boolean }>('SELECT to_regclass(\'messages\') IS NOT NULL AS exists;')).rows[0].exists
  t.ok(messageTableExists, 'attempted sql injection does not succeed at dropping messages table')
})


test('teardown queue test', async (t) => {
  t.plan(1)

  try {
    await pool.query('DROP TABLE messages')
  } catch (error) {
    if (error !== null && typeof error === 'object' && 'routine' in error && error.routine !== 'DropErrorMsgNonExistent') {
      console.error(error)
    }
  }
  try {
    await pool.query('DROP TABLE dead_messages')
  } catch (error) {
    if (error !== null && typeof error === 'object' && 'routine' in error && error.routine !== 'DropErrorMsgNonExistent') {
      console.error(error)
    }
  }
  await pool.end()

  t.pass('teardown queue test complete')
})
