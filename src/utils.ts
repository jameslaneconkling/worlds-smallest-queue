import { type Message, type Logger, type CancellablePromise } from './types.js'

export const createLogger = (level: 'INFO' | 'ERROR'): Logger => ({
  info: level === 'INFO' ? (key: string, message?: Message, instanceId?: string) => (
    console.log(`[${instanceId}] ${key}${message === undefined ? '' : ` id: ${message.id}, retry_count: ${message.retry_count}, elapsed_time: ${Date.now() - +message.created_at}`}`)
  ) : () => {},
  error: level === 'ERROR' || level === 'INFO' ? (key: string, error: unknown, message?: Message, instanceId?: string) => (
    console.error(`[${instanceId}] ${key}${message === undefined ? '' : ` id: ${message.id}, retry_count: ${message.retry_count}, elapsed_time: ${Date.now() - +message.created_at},`}`, error)
  ) : () => {}
})

export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

export const loop = (fn: () => Promise<void>): CancellablePromise<void> => {
  let looping = true

  const promise = (async () => {
    while (looping) await fn()
  })() as CancellablePromise<void>

  promise.teardown = () => { looping = false; return promise }

  return promise
}

export const awaitEvent = <Arguments extends unknown[]>(emitter: NodeJS.EventEmitter, event: string, predicate: (...args: Arguments) => boolean, ms: number) => {
  return new Promise<boolean>((resolve) => {
    const handler = (...args: Arguments) => {
      if (predicate(...args)) {
        emitter.off(event, handler)
        resolve(true)
      }
    }

    emitter.on(event, handler)

    setTimeout(() => { emitter.off(event, handler); resolve(false) }, ms)
  })
}

// Ported from node-postgres 8.12 - https://github.com/brianc/node-postgres/blob/54eb0fa216aaccd727765641e7d1cf5da2bc483d/packages/pg/lib/utils.js#L175-L177
export const escapeIdentifier = (str: string) => `"${str.replace(/"/g, '""')}"`
