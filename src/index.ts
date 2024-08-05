import pg from 'pg'
import { logError, logInfo, sleep } from 'utils'


/* types */
export type Message<Body extends Record<string, string> = Record<string, never>> = {
  id: number,
  partition: number,
  body: Body
  created_at: number,
  retry_count: number,
}

export type Handler<Body extends Record<string, string> = Record<string, never>> = (message: Message<Body>) => Promise<void>

export type Config = {
  pool: pg.Pool,
  errorRetryInterval: number,
  pollInterval: number,
  maxRetryCount: number,
  jobTimeout: number,
  messengerId: string
}

/* private functions */
const dequeue = (client: pg.PoolClient) => {
  return client.query(`
WITH RECURSIVE partitions AS (
  -- get all distinct partitions for enqueued rows: https://wiki.postgresql.org/wiki/Loose_indexscan
  (SELECT partition FROM messages ORDER BY partition ASC LIMIT 1)
  UNION ALL
  SELECT (SELECT partition FROM messages WHERE partition > partitions.partition ORDER BY partition ASC LIMIT 1)
  FROM partitions
  WHERE partitions.partition IS NOT NULL
)
DELETE FROM messages
WHERE id = (
  SELECT id
  FROM messages
  WHERE partition = (
    SELECT partition
    FROM partitions
    WHERE partition IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 1
  )
  ORDER BY id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *
  `)
}

const processMessage = async <Body extends Record<string, string> = Record<string, never>>(client: pg.PoolClient, handler: Handler<Body>, config: Config) => {
  let message: Message<Body> | undefined

  try {
    logInfo('dequeue_job', config.messengerId)
    await client.query('SET idle_in_transaction_session_timeout=' + config.jobTimeout)
    await client.query('BEGIN')
    message = (await dequeue(client)).rows[0]
  } catch (error) {
    logError('dequeue_job_error', config.messengerId, error)
    await client.query('ROLLBACK')
    await sleep(config.errorRetryInterval)
    return
  }

  if (message === undefined) {
    logInfo('queue_empty', config.messengerId)
    await client.query('COMMIT')
    await sleep(config.pollInterval)
    return
  }

  try {
    logInfo('job_start', config.messengerId)
    await handler(message)
    await client.query('COMMIT')
    logInfo('job_success', config.messengerId)
    return
  } catch (error) {
    if (message.retry_count < config.maxRetryCount) {
      logError('job_error', config.messengerId, error)
      await sleep(config.errorRetryInterval)
      await client.query('ROLLBACK')
      await client.query(`
        UPDATE messages
        SET retry_count = retry_count + 1
        WHERE partition = $1 AND id = $2
      `, [message.partition, message.id])
      return
    } else {
      logError('job_max_error_retry', config.messengerId, error)
      try {
        client.query(`
          INSERT INTO upload_row_errors (id, partition, body, error)
          VALUES ($1, $2, $3, $4)
        `, [message.id, message.partition, message.body, error])
        await client.query('COMMIT')
      } catch (error) {
        logError('upload_row_error_insert_failed', config.messengerId, error)
        await client.query('ROLLBACK')
      }
      return
    }
  }
}

/* public functions */
export const createQueue = async (client: pg.Client) => {
  await client.connect()
  await client.query(`
CREATE TABLE messages (
  id bigserial NOT NULL,
  partition integer NOT NULL,
  body JSONB NOT NULL,
  retry_count integer NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (partition, id)
);

CREATE TABLE dead_messages (
  id bigint NOT NULL,
  partition integer NOT NULL,
  body JSONB NOT NULL,
  error text NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (partition, id)
);
  `)
  await client.end()
}

export const destroyQueue = async (client: pg.Client) => {
  await client.connect()
  await client.query('DROP TABLE messages; DROP TABLE dead_messages')
  await client.end()
}

export const enqueue = async <MessageBody = Record<string, never>>(messages: MessageBody[], partition: number, client: pg.PoolClient) => {
  await client.query(`INSERT INTO messages (partition, body) VALUES ${
     // TODO - escape
    messages.map((message) => `(${partition}, '${JSON.stringify(message)}')`).join(', ')
  }`)
}

export const flush = async (partition: number, client: pg.Client) => {
  let empty = false
  await client.connect()

  while (!empty) {
    await sleep(500)
    empty = (await client.query<{ count: number }>('SELECT count(*)::int FROM messages WHERE partition = $1', [partition])).rows[0].count === 0
  }

  await client.end()
}

export const messenger = <Body extends Record<string, string> = Record<string, never>>(handler: Handler<Body>, config?: Partial<Config>) => {
  let running = true

  const _config: Config = {
    pool: config?.pool ?? new pg.Pool(),
    errorRetryInterval: config?.errorRetryInterval ?? 2000,
    pollInterval: config?.pollInterval ?? 2000,
    maxRetryCount: config?.maxRetryCount ?? 10,
    jobTimeout: config?.jobTimeout ?? 1000 * 60 * 60 * 10, // 10 min
    messengerId: config?.messengerId ?? '1'
  }

  ;(async () => {
    while (running) {
      let client: pg.PoolClient | undefined
      try {
        client = await _config.pool.connect()
        await processMessage(client, handler, _config)
      } catch (error) {
        logError('unhandled_error', _config.messengerId, error)
        await sleep(_config.errorRetryInterval)
      } finally {
        client?.release()
      }
    }
  })()

  return () => { running = false }
}
