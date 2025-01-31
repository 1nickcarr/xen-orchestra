import assert from 'node:assert'
import { suite, test } from 'node:test'
import { Forker } from './duplicate.mts'

async function* makeRangeGenerator(end = Infinity, progress = { yielded: 0 }, onYielded = (val: unknown) => {}) {
  let iterationCount = 0
  for (let i = 0; i < end; i++) {
    iterationCount++
    await new Promise(resolve => setTimeout(resolve, 10))
    yield i
    onYielded(i)
    progress.yielded = i
  }
  return progress
}

async function consume(
  iterable: AsyncGenerator,
  label: string,
  delay = 2500,
  onConsumed = (val: unknown, iterable: AsyncGenerator) => true
) {
  for await (const val of iterable) {
    console.log({ label, val })
    await new Promise(resolve => setTimeout(resolve, delay))
    if (!onConsumed(val, iterable)) {
      iterable.return(undefined)
      break
    }
  }
}

suite('success', t => {
  test('if works with multiple consumer', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(3, progress)
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')
    await Promise.all([consume(first, 'first', 50), consume(second, 'second', 500)])
    assert.strictEqual(progress.yielded, 2)
  })

  test('if works with one consumer returning', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(10, progress)
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')
    await new Promise(resolve => setTimeout(resolve, 500))
    await Promise.all([
      consume(first, 'first', 1),
      consume(second, 'second', 1, (val: unknown) => {
        return val !== 2
      }),
    ])
    assert.strictEqual(progress.yielded, 9)
  })
  test('if works with all consumers returning', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(10, progress)
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')
    await new Promise(resolve => setTimeout(resolve, 500))
    await Promise.all([
      consume(first, 'first', 1, (val: unknown) => {
        return val !== 5
      }),
      consume(second, 'second', 1, (val: unknown) => {
        return val !== 2
      }),
    ])
    assert.strictEqual(progress.yielded, 4)
  })
})

suite('error', () => {
  test('it fails all if error on source', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(5, progress, (val: unknown) => {
      if (val === 2) {
        throw new Error('source stop')
      }
    })
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')
    await assert.rejects(Promise.all([consume(first, 'first', 1), consume(second, 'second', 1)]))

    assert.strictEqual(progress.yielded, 1)
  })
  test('it flows until the end if one consumer throws', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(5, progress)
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')

    const firstConsume = consume(first, 'first', 1)
    const secondConsume = consume(second, 'second', 1, (val: unknown, iterable) => {
      if (val === 1) {
        iterable.throw(new Error('error on second'))
      }
      return true
    }).catch(err => {})
    await Promise.allSettled([firstConsume, secondConsume])
    assert.strictEqual(progress.yielded, 4)
  })
  test('it throw if ALL consumer throws', async () => {
    const progress = { yielded: 0 }
    const generator = makeRangeGenerator(5, progress)
    const forker = new Forker(generator)
    const first = forker.fork('first')
    const second = forker.fork('second')

    const firstConsume = consume(first, 'first', 1, (val: unknown, iterable) => {
      if (val === 3) {
        iterable.throw(new Error('error on first'))
      }
      return true
    })
    const secondConsume = consume(second, 'second', 1, (val: unknown, iterable) => {
      if (val === 1) {
        iterable.throw(new Error('error on second'))
      }
      return true
    })

    assert.rejects(Promise.all([firstConsume, secondConsume]))
    assert.strictEqual(progress.yielded, 2)
  })
})
