import test from 'tape'
import pg from 'pg'
import { createQueue, destroyQueue, enqueue, flush, Message, dequeue } from '../src'

if (process.env.PG_PORT === undefined) {
  console.error('missing PG_PORT env variable')
  process.exit(1)
} else if (isNaN(parseInt(process.env.PG_PORT, 10))) {
  console.error(`malformed PG_PORT env variable. expected a number. received ${parseInt(process.env.PG_PORT, 10)}`)
  process.exit(1)
}

const PG_CONFIG = {
  user: 'postgres',
  database: 'worlds-smallest-queue-test',
  password: 'test',
  port: parseInt(process.env.PG_PORT!, 10)
}

try {
  await destroyQueue(PG_CONFIG)
} catch (e) {
  if (e && e.routine !== 'DropErrorMsgNonExistent') {
    console.error(e)
  }
}
await createQueue(PG_CONFIG)

test('enqueue messages', async (t) => {
  const partition = 1
  const pool = new pg.Pool(PG_CONFIG)
  let messageCounter = 0
  let client: pg.PoolClient | undefined
  
  // enqueue 3 messages
  try {
    client = await pool.connect()
    await enqueue([{ event: 'abc' }, { event: 'def' }, { event: 'abc' }], partition, client)
  } finally {
    client?.release()
  }

  // begin dequeueing messages
  const destroy = dequeue(
    async (message: Message<{ event: string }>) => {
      messageCounter++
      t.ok(message.body.event === 'abc' || message.body.event === 'def' || message.body.event === 'ghi', 'message body is correct')
    },
    { pool }
  )

  // enqueue more messages
  try {
    client = await pool.connect()
    await enqueue([{ event: 'ghi' }, { event: 'def' }], partition, client)
  } finally {
    client?.release()
  }

  // clean up
  await flush(PG_CONFIG)
  t.equals(messageCounter, 5, 'processed correct number of enqueued messages')
  destroy()
  await pool.end()
})

test.onFinish(async () => {
  try {
    await destroyQueue(PG_CONFIG)
  } catch (e) {
    console.error('onFinish Error', e)
  }
})
