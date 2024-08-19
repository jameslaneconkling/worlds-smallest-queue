import { type ClientBase, type PoolClient, type Notification, type Pool } from 'pg'
import { createLogger, sleep, loop, asyncEvent } from './utils.js'
import { type Config, type Message, type Handler, type Logger } from './types.js'


export const setupPGQueue = () => `
  CREATE TABLE messages (
    id bigserial NOT NULL,
    partition integer NOT NULL,
    body jsonb NOT NULL,
    await boolean NOT NULL DEFAULT False,
    retry_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    retry_at timestamptz,
    PRIMARY KEY (partition, id)
  );
  CREATE TABLE dead_messages (
    id bigint NOT NULL,
    partition integer NOT NULL,
    body jsonb NOT NULL,
    retry_count integer NOT NULL,
    error text NOT NULL,
    created_at timestamptz NOT NULL,
    errored_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (partition, id)
  );
`

export const teardownPGQueue = () => 'DROP TABLE messages; DROP TABLE dead_messages'

export const enqueue = async <Body>(client: ClientBase, messages: Body[], partition: number, awaitMessage?: number) => {
  const values = messages.map((message) => `(${partition}, '${JSON.stringify(message)}', ${awaitMessage !== undefined})`).join(', ')
  const messageIds = (await client.query<{ id: number }>(`
    INSERT INTO messages (partition, body, await) VALUES ${values} RETURNING (id)
  `)).rows.map(({ id }) => id)
  await client.query(`NOTIFY enqueue, '${partition}'`)

  if (awaitMessage) {
    client.setMaxListeners(client.getMaxListeners() + messageIds.length)
    await client.query(`LISTEN dequeue`)
    await Promise.all(messageIds.map((id) => (
      asyncEvent(client, 'notification', ({ channel, payload }: Notification) => channel === 'dequeue' && payload === `${partition}_${id}`, awaitMessage)
    )))
    client.setMaxListeners(client.getMaxListeners() - messageIds.length)
  }

  return messageIds
}

const dequeue = async <Body>(client: ClientBase, handler: Handler<Body>, config: Required<Config>): Promise<void> => {
  let message: Message<Body> | undefined
  const dequeueInterval = config.dequeueInterval + ((Math.random() - 0.5) * 0.5 * config.dequeueInterval)

  // dequeue message
  try {
    await client.query(`SET idle_in_transaction_session_timeout=${config.messageTimeout}`) // TODO set and handle idle_session_timeout
    await client.query('BEGIN')
    message = (await client.query<Message<Body>>(`
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
      RETURNING *
    `)).rows[0]
    if (message === undefined) {
      await client.query('COMMIT')
      await client.query('LISTEN enqueue')
      return await asyncEvent(client, 'notification', ({ channel }: Notification) => channel === 'enqueue', dequeueInterval)
    }
  } catch (error) {
    config.logger.error('dequeue_message_error', error, undefined, config.instanceId)
    await client.query('ROLLBACK')
    return await sleep(config.errorRetryInterval + ((Math.random() - 0.5) * 0.5 * config.errorRetryInterval))
  }

  // process message
  try {
    config.logger.info('processing_message', message, config.instanceId)
    await handler(message, client)
    await client.query('COMMIT')
    if (message.await) client.query(`NOTIFY dequeue, '${message.partition}_${message.id}'`)
  } catch (error) {
    try {
      if (error instanceof Error && error.message === 'Client has encountered a connection error and is not queryable') {
        config.logger.error('message_failure_connection_error', error, message, config.instanceId)
        await sleep(config.errorRetryInterval)
      } else if (message.retry_count < config.maxRetryCount) {
        config.logger.error('message_failure', error, message, config.instanceId)
        await client.query('ROLLBACK')
        await client.query(`
          UPDATE messages
          SET retry_at = NOW() + ($1 * (2 ^ retry_count)) * INTERVAL '1 ms', retry_count = retry_count + 1
          WHERE partition = $2 AND id = $3
        `, [config.errorRetryInterval, message.partition, message.id])
      } else {
        config.logger.error('message_failure_max_retry', error, message, config.instanceId)
        client.query(`
          INSERT INTO dead_messages (id, partition, body, retry_count, error, created_at) VALUES ($1, $2, $3, $4, $5, $6)
        `, [message.id, message.partition, message.body, message.retry_count, error, message.created_at])
        await client.query('COMMIT')
        if (message.await) client.query(`NOTIFY dequeue_error, '${message.partition}_${message.id}'`)
      }
    } catch (error) {
      config.logger.error('message_failure_connection_error', error, message, config.instanceId)
      await sleep(config.errorRetryInterval)
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
      await dequeue(client, handler, _config)
    } catch (error) {
      _config.logger.error('unhandled_error', error, undefined, _config.instanceId)
      await sleep(_config.errorRetryInterval + ((Math.random() - 0.5) * 0.5 * _config.errorRetryInterval))
    } finally {
      client?.removeAllListeners().release()
    }
  })
}

export { Config, Message, Handler, Logger, ClientBase, Pool }
