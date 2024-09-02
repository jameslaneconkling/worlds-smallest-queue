import test from 'tape'
import pg from 'pg'
import { enqueue, setupPGQueue } from '../src/index.js'


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
})


test('setup sql tests', async (t) => {
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


test('returns all partitions via a loose index scan', async (t) => {
  t.plan(4)

  const query = `
    WITH RECURSIVE partitions (partition) AS (
      (SELECT partition FROM messages ORDER BY partition ASC LIMIT 1)
      UNION ALL
      SELECT (SELECT partition FROM messages WHERE partition > partitions.partition ORDER BY partition ASC LIMIT 1)
      FROM partitions WHERE partitions.partition IS NOT NULL
    )
    SELECT * FROM partitions WHERE partition IS NOT NULL
  `

  let partitions: { partition: number }[] = []
  let totalDuration = 0
  for (let i = 0; i < 100; i++) {
    const t0 = performance.now()
    partitions = (await pool.query(query)).rows
    totalDuration += performance.now() - t0
  }
  let averageDuration = totalDuration / 100

  t.ok(averageDuration < 2, `loose index scan time of ${averageDuration}ms is less than 2ms`)
  t.deepEquals(partitions, [], 'returns no partitions')

  const client = await pool.connect()
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 4)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 1)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 8)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 10)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 2)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 12)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 3)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 2000)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 40)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 399)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 25)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 21)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 100000000)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 455)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 512)
  await client.release()

  totalDuration = 0
  for (let i = 0; i < 100; i++) {
    const t0 = performance.now()
    partitions = (await client.query(query)).rows
    totalDuration += performance.now() - t0
  }
  averageDuration = totalDuration / 100

  t.ok(averageDuration < 2, `loose index scan time of ${averageDuration}ms is less than 2ms`)
  t.deepEquals(partitions.map(({ partition }) => partition), [1, 2, 3, 4, 8, 10, 12, 21, 25, 40, 399, 455, 512, 2000, 100000000], 'returns all partitions')

  await client.query('TRUNCATE messages')
})


test('returns a message from a random partition', async (t) => {
  t.plan(1)

  const query = `
    BEGIN;
    WITH RECURSIVE partitions (partition) AS (
      (SELECT partition FROM messages ORDER BY partition LIMIT 1)
      UNION ALL
      SELECT (SELECT partition FROM messages WHERE partition > partitions.partition ORDER BY partition LIMIT 1)
      FROM partitions WHERE partitions.partition IS NOT NULL
    )
    DELETE FROM messages
    WHERE id = (
      SELECT id FROM messages
      WHERE partition = (SELECT partition FROM partitions WHERE partition IS NOT NULL ORDER BY RANDOM() LIMIT 1)
        AND (retry_at IS NULL OR retry_at <= NOW())
      ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
    )
    RETURNING *;
    COMMIT;
  `

  const client = await pool.connect()
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 4)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 1)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 8)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 10)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 2)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 12)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 3)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 2000)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 40)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 399)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 25)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 21)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 100000000)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 455)
  await enqueue(client, Array.from({ length: 1000 }).map(() => ({})), 512)
  client.release()

  let totalDuration = 0 
  for (let i = 0; i < 100; i++) {
    const t0 = performance.now()
    await client.query(query)
    totalDuration += performance.now() - t0
  }
  const averageDuration = totalDuration / 100

  t.ok(averageDuration < 4, `returning a message in ${averageDuration}ms is less than 4ms`)

  await client.query('TRUNCATE messages')
})


test('teardown sql test', async (t) => {
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
