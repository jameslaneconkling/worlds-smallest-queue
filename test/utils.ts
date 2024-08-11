import test from 'tape'
import { loop, poll, sleep, sortFlatObjectList as sort } from '../src/utils.js'


test('poll utility resolves once asyncPredicate resolves to true', async (t) => {
  t.plan(1)
  let i = 0
  await poll(
    async () => ++i === 4,
    10,
    10
  )

  t.equals(i, 4, 'polls function 4 times before resolving')
})


test('poll utility errors when retryMax is exceeded', async (t) => {
  t.plan(2)
  let i = 0

  try {
    await poll(
      async () => {
        i++
        return false
      },
      10,
      10
    )
    t.fail('poll should not resolve when exceeding maxRetry')
  } catch (e) {
    t.ok(e instanceof Error && e.message === 'poll_max_retry')
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


test('loops until cancel handler is called', async (t) => {
  t.plan(1)

  let count = 0
  const cancel = loop(async () => {
    count++
    await sleep(10)
  })

  await sleep(50)
  await cancel()
  t.equals(count, 5, 'loops for 50ms and stops')
})


test('loop cancel handler doesnt resolve until current loop completes', async (t) => {
  t.plan(1)

  let count = 0
  const cancel = loop(async () => {
    await sleep(10)
    count++
  })

  await cancel()
  t.equals(count, 1, 'awaiting cancel handler waits until current loop complete')
})
