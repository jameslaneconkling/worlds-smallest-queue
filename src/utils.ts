import type { Message, Logger } from './index.js'

export const noop = () => {}

const _info = (key: string, message?: Message, instanceId?: string) => (
  console.log(`[${instanceId}] ${key} ${message === undefined ? '' : `id: ${message.id}, retry_count: ${message.retry_count}, elapsed_time: ${Date.now() - +message.created_at}`}`)
)

const _error = (key: string, error: unknown, message?: Message, instanceId?: string) => (
  console.error(`[${instanceId}] ${key} ${message === undefined ? '' : `id: ${message.id}, retry_count: ${message.retry_count}, elapsed_time: ${Date.now() - +message.created_at}`}`, error)
)

export const createLogger = (level: 'INFO' | 'ERROR'): Logger => ({
  info: level === 'INFO' ? _info : noop,
  error: level === 'ERROR' || level === 'INFO' ? _error : noop
})

export const sleep = async (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export const poll = async (asyncPredicate: () => Promise<boolean>, pollInterval: number, maxRetry: number) => {
  let retry = 0
  while (true) {
    if (retry++ >= maxRetry) {
      throw new Error('poll_max_retry')
    }
    
    try {
      if (await asyncPredicate()) {
        return
      }
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (_) { /* empty */ }

    await sleep(pollInterval)
  }
}

export const loop = (fn: () => Promise<void>) => {
  let looping = true

  const loopComplete = (async () => {
    while (looping) {
      await fn()
    }
  })()

  return async () => {
    looping = false
    await loopComplete
  }
}

export const sortFlatObjectList = (list: Record<string, string | number>[]) => {
  const keys = Object.keys(list[0]).sort()
  return Array.from(list).sort((a, b) => {
    for (const key of keys) {
      if (a[key] !== b[key]) {
        return a[key].toString().localeCompare(b[key].toString())
      }
    }

    return 0
  })
}
