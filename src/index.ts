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
  processMessageTimeout: number,
  messengerId: string
}

/* private functions */
const dequeueMessage = async <Body extends Record<string, string> = Record<string, never>>(client: pg.PoolClient) => {
  return (await client.query<Message<Body>>(`
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
  `)).rows[0]
}

const processMessage = async <Body extends Record<string, string> = Record<string, never>>(client: pg.PoolClient, handler: Handler<Body>, config: Config) => {
  let message: Message<Body> | undefined

  try {
    await client.query('SET idle_in_transaction_session_timeout=' + config.processMessageTimeout)
    await client.query('BEGIN')
    message = await dequeueMessage(client)
  } catch (error) {
    logError('dequeue_message_error', config.messengerId, error)
    await client.query('ROLLBACK')
    await sleep(config.errorRetryInterval)
    return
  }

  if (message === undefined) {
    await client.query('COMMIT')
    await sleep(config.pollInterval)
    return
  }

  try {
    logInfo('process_message', config.messengerId)
    await handler(message)
    await client.query('COMMIT')
    return
  } catch (error) {
    if (message.retry_count < config.maxRetryCount) {
      logError('process_message_error', config.messengerId, error)
      await sleep(config.errorRetryInterval)
      await client.query('ROLLBACK')
      await client.query(`
        UPDATE messages
        SET retry_count = retry_count + 1
        WHERE partition = $1 AND id = $2
      `, [message.partition, message.id])
      return
    } else {
      logError('process_message_error_max_retry', config.messengerId, error)
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
export const createQueue = async (config: pg.ClientConfig | string) => {
  const client = new pg.Client(config)
  let error: unknown | undefined

  try {
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
  } catch (e) {
    error = e
  } finally {
    await client.end()
    if (error !== undefined) {
      throw error
    }
  }
}

export const destroyQueue = async (pgConfig: pg.ClientConfig | string) => {
  const client = new pg.Client(pgConfig)
  let error: unknown | undefined

  try {
    await client.connect()
    await client.query('DROP TABLE messages; DROP TABLE dead_messages')
  } catch (e) {
    error = e
  } finally {
    await client.end()
    if (error !== undefined) {
      throw error
    }
  }
}

export const flush = async (pgConfig: pg.ClientConfig | string) => {
  const client = new pg.Client(pgConfig)
  let error: unknown | undefined

  try {
    await client.connect()
    while (true) {
      if ((await client.query<{ count: number }>('SELECT count(*)::int FROM messages')).rows[0].count === 0) {
        break
      } else {
        sleep(100)
      }
    }
  } catch (e) {
    error = e
  } finally {
    // if client.connect() fails, then client.end() will fail too. pooling is safer here, b/c the client is undefined if it's not successfully retrieved from the pool
    await client.end()
    if (error !== undefined) {
      throw error
    }
  }
}

export const enqueue = async <Body = Record<string, never>>(messages: Body[], partition: number, client: pg.ClientBase) => {
  await client.query(`INSERT INTO messages (partition, body) VALUES ${
      // TODO - escape
    messages.map((message) => `(${partition}, '${JSON.stringify(message)}')`).join(', ')
  }`)
}

export const dequeue = <Body extends Record<string, string> = Record<string, never>>(handler: Handler<Body>, queueConfig?: Partial<Config>) => {
  const _queueConfig: Config = {
    pool: queueConfig?.pool ?? new pg.Pool(),
    errorRetryInterval: queueConfig?.errorRetryInterval ?? 2000,
    pollInterval: queueConfig?.pollInterval ?? 2000,
    maxRetryCount: queueConfig?.maxRetryCount ?? 10,
    processMessageTimeout: queueConfig?.processMessageTimeout ?? 1000 * 60 * 60 * 10, // 10 min
    messengerId: queueConfig?.messengerId ?? '1'
  }
  let running = true

  ;(async () => {
    while (running) {
      let client: pg.PoolClient | undefined
      try {
        client = await _queueConfig.pool.connect()
        await processMessage(client, handler, _queueConfig)
      } catch (error) {
        logError('unhandled_error', _queueConfig.messengerId, error)
        await sleep(_queueConfig.errorRetryInterval)
      } finally {
        client?.release()
      }
    }
  })()

  return () => { running = false }
}
