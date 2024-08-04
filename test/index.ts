import test from 'tape'
import { hello } from '../src'


test('timing test', (t) => {
  t.plan(2)

  t.equal(typeof hello, 'function')

  setTimeout(function () {
      t.equal(hello('werld'), 'Hello werld')
  }, 100)
})
