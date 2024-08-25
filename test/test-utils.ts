import { sleep } from '../src/utils'


export const poll = async (asyncPredicate: () => Promise<boolean>, pollInterval: number, maxRetry: number) => {
  let retry = 0
  while (true) {
    if (retry++ >= maxRetry) throw new Error('max_poll_retry')
    
    try {
      if (await asyncPredicate()) return
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (_) { /* empty */ }

    await sleep(pollInterval)
  }
}

export const sort = (list: Record<string, string | number>[]) => {
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
