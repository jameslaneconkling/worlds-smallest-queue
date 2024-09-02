import { type ClientBase, type PoolClient, type Notification } from 'pg'
import { createLogger, loop, awaitEvent, sleep, escapeIdentifier } from './utils.js'
import { type Config, type Message, type Handler, type Logger, type CancellablePromise } from './types.js'

export const setupPGQueue = (schema = 'public') => `
  SET search_path TO ${escapeIdentifier(schema)};
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

export const teardownPGQueue = (schema = 'public') => `SET search_path TO ${escapeIdentifier(schema)}; DROP TABLE messages; DROP TABLE dead_messages;`

export const enqueue = async <Body>(client: ClientBase, messages: Body[], partition: number, options?: { awaitMessage?: number, schema?: string }): Promise<{ ids: number[], awaited?: boolean }> => {
  await client.query(`SET search_path TO ${escapeIdentifier(options?.schema ?? 'public')}`)
  const messageIds = (await client.query<{ id: number }>(`
    INSERT INTO messages (partition, body, await) VALUES ${messages.map((_, idx) => `(${Number(partition)}, $${idx + 1}, ${options?.awaitMessage !== undefined})`).join()} RETURNING (id)
  `, messages)).rows
  let awaited: boolean | undefined
  await client.query(`NOTIFY enqueue, '${Number(partition)}'`)

  if (options?.awaitMessage !== undefined) {
    client.setMaxListeners(client.getMaxListeners() + messageIds.length)
    await client.query('LISTEN dequeue')
    awaited = (await Promise.all(messageIds.map(({ id }) => (
      awaitEvent(client, 'notification', ({ channel, payload }: Notification) => channel === 'dequeue' && payload === `${partition}_${id}`, options?.awaitMessage as number)
    )))).every((awaitSuccessful) => awaitSuccessful)
    client.setMaxListeners(client.getMaxListeners() - messageIds.length)
  }

  return { ids: messageIds.map(({ id }) => id), awaited }
}

const dequeue = async <Body>(client: ClientBase, handler: Handler<Body>, config: Required<Config>): Promise<void> => {
  // dequeue message
  await client.query(`SET idle_in_transaction_session_timeout=${Number(config.messageTimeout)}; SET search_path TO ${escapeIdentifier(config.schema)}; BEGIN;`)
  const message = (await client.query<Message<Body>>(`
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
    await client.query('COMMIT; LISTEN enqueue;')
    await awaitEvent(client, 'notification' as const, ({ channel }) => channel === 'enqueue', config.dequeueInterval)
    return
  }

  // process message
  try {
    config.logger.info('processing_message', message, config.instanceId)
    await handler(message, client)
    await client.query('COMMIT')
    if (message.await) await client.query(`NOTIFY dequeue, '${message.partition}_${message.id}'`)
  } catch (error) {
    if (message.retry_count < config.maxMessageRetryCount) {
      config.logger.error('message_failure', error, message, config.instanceId)
      await client.query('ROLLBACK')
      await client.query(`
        UPDATE messages
        SET retry_at = NOW() + ($1 * (2 ^ retry_count)) * INTERVAL '1 ms', retry_count = retry_count + 1
        WHERE partition = $2 AND id = $3
      `, [config.errorRetryInterval, message.partition, message.id])
    } else {
      config.logger.error('message_failure_max_retry', error, message, config.instanceId)
      await client.query(`
        INSERT INTO dead_messages (id, partition, body, retry_count, error, created_at) VALUES ($1, $2, $3, $4, $5, $6)
      `, [message.id, message.partition, message.body, message.retry_count, error, message.created_at])
      await client.query('COMMIT')
    }
  }
}

export const Queue = <Body>(handler: Handler<Body>, config: Config): CancellablePromise<void> => {
  let errorCount = 0
  const _config: Required<Config> = {
    pool: config.pool,
    schema: config.schema ?? 'public',
    messageTimeout: config.messageTimeout ?? 60_000,
    maxMessageRetryCount: config.maxMessageRetryCount ?? 15,
    maxQueueRetryCount: config.maxQueueRetryCount ?? 20,
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
      client.removeAllListeners().release()
      errorCount = 0
    } catch (error) {
      _config.logger.error('queue_error', error, undefined, _config.instanceId)
      client?.removeAllListeners().release(true)
      if (++errorCount >= _config.maxQueueRetryCount) throw error
      await sleep(_config.errorRetryInterval)
    }
  })
}

export { Config, Message, Handler, Logger, CancellablePromise }
