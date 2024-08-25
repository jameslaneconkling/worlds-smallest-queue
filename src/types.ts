import { type Pool, type ClientBase } from 'pg'


export type Config = {
  pool: Pool,
  messageTimeout?: number,
  maxMessageRetryCount?: number,
  messageErrorRetryInterval?: number,
  queueTimeout?: number,
  maxQueueRetryCount?: number
  queueErrorRetryInterval?: number,
  dequeueInterval?: number,
  instanceId?: string,
  logLevel?: 'INFO' | 'ERROR',
  logger?: Logger
}

export type Message<Body = unknown> = {
  id: number,
  partition: number,
  body: Body,
  await: boolean,
  retry_count: number,
  created_at: Date,
  retry_at: Date,
}

export type Handler<Body> = (message: Message<Body>, client: ClientBase) => Promise<unknown>

export type CancellablePromise<T> = Promise<T> & { teardown: () => Promise<T> }

export type Logger = {
  info: (key: string, message?: Message, instanceId?: string) => void
  error: (key: string, error: unknown, message?: Message, instanceId?: string) => void
}
