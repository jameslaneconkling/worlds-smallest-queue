import test from 'tape'
import { loop, sleep, timeout } from '../src/utils.js'
import { poll, sort } from './test-utils.js'


test('timeout resolves when wrapped function resolves', async (t) => {
  t.plan(1)

  const t0 = Date.now()
  await timeout(() => sleep(20), 30)
  const dt = Date.now() - t0
  t.ok(dt >= 20 && dt <= 25, 'timeout resolved')
})


test('timeout rejects when wrapped function rejects', async (t) => {
  t.plan(1)

  try {
    await timeout(() => { sleep(10); throw new Error('system_fubr') }, 15)
    t.fail('timeout should error out')
  } catch (error) {
    t.ok(error instanceof Error && error.message === 'system_fubr')
  }
})


test('timeout rejects when time is exceeded', async (t) => {
  t.plan(1)

  try {
    await timeout(() => sleep(20), 15)
    t.fail('timeout should error out')
  } catch (error) {
    t.ok(error instanceof Error && error.message === 'timeout')
  }
})


test('loops until cancel handler is called', async (t) => {
  t.plan(1)

  let count = 0
  const { teardown } = loop(async () => { count++; await sleep(10) })

  await sleep(50)
  await teardown()
  t.equals(count, 5, 'loops for 50ms and stops')
})


test('loop cancel handler doesnt resolve until current loop completes', async (t) => {
  t.plan(1)

  let count = 0
  const { teardown } = loop(async () => { await sleep(10); count++ })

  await teardown()
  t.equals(count, 1, 'awaiting cancel handler waits until current loop complete')
})


test('loop rejects on error', async (t) => {
  t.plan(2)

  let count = 0
  const promise = loop(async () => {
    await sleep(10)
    if (++count >= 5) {
      throw new Error('loop_failure')
    }
  })

  try {
    await promise
  } catch (error) {
    t.ok(error instanceof Error && error.message === 'loop_failure', 'loop errored out')
    t.equals(count, 5, 'loop errored out after looping 5 times')
  }
})


test('poll utility resolves once asyncPredicate resolves to true', async (t) => {
  t.plan(1)
  let i = 0
  await poll(async () => ++i === 4, 10, 10)

  t.equals(i, 4, 'polls function 4 times before resolving')
})


test('poll utility errors when retryMax is exceeded', async (t) => {
  t.plan(2)
  let i = 0

  try {
    await poll(async () => { i++; return false }, 10, 10)
    t.fail('poll should not resolve when exceeding maxRetry')
  } catch (e) {
    t.ok(e instanceof Error && e.message === 'max_poll_retry')
    t.equals(i, 10, 'poll should reject after exceeding maxRetry')
  }
})


test('poll utility retries on asyncPrecicate errors', async (t) => {
  t.plan(1)
  let i = 0
  await poll(
    async () => {
      if (++i < 10) {
        throw new Error('simulate_poll_async_predicate_function_failure')
      }

      return true
    },
    10,
    10
  )

  t.equals(i, 10, 'poll completes when errors dont exceed maxRetry count')
})


test('deterministically sort flat objects', async (t) => {
  t.plan(1)
  t.deepEquals(
    sort([
      { y: 'd', x: 'b' },
      { y: 'c', x: 'c' },
      { y: 'a', x: 'e' },
      { y: 'b', x: 'g' },
      { y: 'g', x: 'd' },
      { y: 'c', x: 'b' },
      { y: 'a', x: 'c' },
      { y: 'g', x: 'b' },
      { y: 'd', x: 'g' },
      { y: 'b', x: 'g' },
      { y: 'a', x: 'e' }
    ]),
    [
      { x: 'b', y: 'c' },
      { x: 'b', y: 'd' },
      { x: 'b', y: 'g' },
      { x: 'c', y: 'a' },
      { x: 'c', y: 'c' },
      { x: 'd', y: 'g' },
      { x: 'e', y: 'a' },
      { x: 'e', y: 'a' },
      { x: 'g', y: 'b' },
      { x: 'g', y: 'b' },
      { x: 'g', y: 'd' },
    ]
  )
})
