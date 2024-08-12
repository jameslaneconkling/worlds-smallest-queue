import { type Pool, type ClientBase, type PoolClient, type Notification } from 'pg'
import { createLogger, sleep, loop, asyncEvent } from './utils.js'


export type Message<Body = unknown> = {
  id: number,
  partition: number,
  body: Body
  retry_count: number,
  created_at: Date,
  retry_at: Date,
}

export type Handler<Body> = (message: Message<Body>, client: ClientBase) => Promise<unknown>

export type Logger = {
  info: (key: string, message?: Message, instanceId?: string) => void
  error: (key: string, error: unknown, message?: Message, instanceId?: string) => void
}

export type Config = {
  pool: Pool,
  errorRetryInterval?: number,
  dequeueInterval?: number,
  maxRetryCount?: number,
  messageTimeout?: number,
  instanceId?: string,
  logLevel?: 'INFO' | 'ERROR',
  logger?: Logger
}

export const setupPGQueue = () => `
  CREATE TABLE messages (
    id bigserial NOT NULL,
    partition integer NOT NULL,
    body JSONB NOT NULL,
    retry_count integer NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_at TIMESTAMPTZ,
    PRIMARY KEY (partition, id)
  );
  CREATE TABLE dead_messages (
    id bigint NOT NULL,
    partition integer NOT NULL,
    body JSONB NOT NULL,
    retry_count integer NOT NULL,
    error text NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    errored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (partition, id)
  );
`

export const teardownPGQueue = () => 'DROP TABLE messages; DROP TABLE dead_messages'

export const enqueue = <Body>(messages: Body[], partition: number) => `
  INSERT INTO messages (partition, body) VALUES ${messages.map((message) => `(${partition}, '${JSON.stringify(message)}')`).join(', ')};
  NOTIFY enqueue, '${partition}';
`

const _dequeue = async <Body>(handler: Handler<Body>, client: ClientBase, _config: Required<Config>): Promise<void> => {
  let message: Message<Body> | undefined
  const dequeueInterval = _config.dequeueInterval + ((Math.random() - 0.5) * 0.5 * _config.dequeueInterval)

  // dequeue message
  try {
    await client.query(`SET idle_in_transaction_session_timeout=${_config.messageTimeout}`)
    // await client.query(`SET idle_session_timeout=${dequeueInterval + 2000}`)
    await client.query('BEGIN')
    message = (await client.query<Message<Body>>(`
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
          AND (retry_at IS NULL OR retry_at <= NOW())
        ORDER BY partition ASC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      RETURNING *
    `)).rows[0]
    if (message === undefined) {
      await client.query('COMMIT')
      if (dequeueInterval === 0) {
        return sleep(2000)
      } else {
        await client.query('LISTEN enqueue')
        return await asyncEvent(client, 'notification', ({ channel }: Notification) => channel === 'enqueue', dequeueInterval)
      }
    }
  } catch (error) {
    _config.logger.error('dequeue_message_error', error, undefined, _config.instanceId)
    await client.query('ROLLBACK')
    return await sleep(_config.errorRetryInterval + ((Math.random() - 0.5) * 0.5 * _config.errorRetryInterval))
  }

  // process message
  try {
    _config.logger.info('processing_message', message, _config.instanceId)
    await handler(message, client)
    await client.query('COMMIT')
  } catch (error) {
    try {
      if (error instanceof Error && error.message === 'Client has encountered a connection error and is not queryable') {
        _config.logger.error('message_failure_connection_error', error, message, _config.instanceId)
        await sleep(_config.errorRetryInterval)
      } else if (message.retry_count < _config.maxRetryCount) {
        _config.logger.error('message_failure', error, message, _config.instanceId)
        await client.query('ROLLBACK')
        await client.query(`
          UPDATE messages
          SET retry_at = NOW() + ($1 * (2 ^ retry_count)) * INTERVAL '1 ms',
            retry_count = retry_count + 1
          WHERE partition = $2 AND id = $3
        `, [_config.errorRetryInterval, message.partition, message.id])
      } else {
        _config.logger.error('message_failure_max_retry', error, message, _config.instanceId)
        client.query(`
          INSERT INTO dead_messages (id, partition, body, retry_count, error, created_at)
          VALUES ($1, $2, $3, $4, $5, $6)
        `, [message.id, message.partition, message.body, message.retry_count, error, message.created_at])
        await client.query('COMMIT')
      }
    } catch (error) {
      _config.logger.error('message_failure_connection_error', error, message, _config.instanceId)
      await sleep(_config.errorRetryInterval)
    }
  }
}

export const Queue = <Body>(handler: Handler<Body>, config: Config): () => Promise<void> => {
  const _config: Required<Config> = {
    pool: config.pool,
    messageTimeout: config.messageTimeout ?? 60_000,
    maxRetryCount: config.maxRetryCount ?? 15,
    errorRetryInterval: config.errorRetryInterval ?? 2_000,
    dequeueInterval: config.dequeueInterval ?? 30_000,
    instanceId: config.instanceId ?? '1',
    logLevel: config.logLevel ?? 'ERROR',
    logger: config.logger ?? createLogger(config.logLevel ?? 'ERROR')
  }

  return loop(async () => {
    let client: PoolClient | undefined
    try {
      client = await _config.pool.connect()
      client.on('error', (error) => _config.logger.error('pg_client_error', error, undefined, _config.instanceId))
      await _dequeue(handler, client, _config)
    } catch (error) {
      _config.logger.error('unhandled_error', error, undefined, _config.instanceId)
      await sleep(_config.errorRetryInterval + ((Math.random() - 0.5) * 0.5 * _config.errorRetryInterval))
    } finally {
      client?.removeAllListeners()
      client?.release()
    }
  })
}
