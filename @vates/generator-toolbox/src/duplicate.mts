import assert from 'node:assert'

export class Forker<T> {
  // uid => waiting
  #forks = new Map<string, boolean>()
  #source: AsyncGenerator<T>

  #started = false

  #promiseWaitingForForks?: {
    promise: Promise<void>
    resolve(): void
    reject(err: Error): void
  }
  #promiseWaitingForNext?: Promise<IteratorResult<T>>

  get nbWaiting(): number {
    let nbWaiting = 0
    this.#forks.forEach(v => (nbWaiting += v ? 1 : 0))
    return nbWaiting
  }
  constructor(source: AsyncGenerator<T>) {
    if (typeof source[Symbol.asyncIterator] !== 'function') {
      throw new Error('Source must be an async iterator')
    }
    this.#source = source
  }

  fork(uid: string): AsyncGenerator<T> {
    assert.notEqual(this.#started, true, 'Can t be forked once started')
    assert.equal(this.#forks.get(uid), undefined, 'Can t have uid duplicated')
    const fork = new DebugGenerator<T>(this, uid)
    this.#forks.set(uid, false)
    return fork
  }

  async #waitForAllForks(): Promise<void> {
    // first fork waiting for the data
    if (this.#promiseWaitingForForks === undefined) {
      let resolve = () => {}
      let reject: (err: Error) => void = () => {}
      const promise = new Promise<void>(function (_resolve, _reject) {
        resolve = _resolve
        reject = _reject
      })

      this.#promiseWaitingForForks = { promise, resolve, reject }
    }
    // all the forks are waiting
    const { promise, resolve } = this.#promiseWaitingForForks

    if (this.nbWaiting === this.#forks.size) {
      // reset data
      this.#promiseWaitingForForks = undefined
      this.#forks.forEach((v, k) => this.#forks.set(k, false))
      resolve() // mark the wait of the other forks as over
    }
    return promise
  }

  async next(uid: string): Promise<IteratorResult<T>> {
    // ensure a fork can't wait twice
    assert.strictEqual(this.#forks.has(uid), true, 'fork is not from this source')
    assert.strictEqual(this.#forks.get(uid), false, 'fork is already waiting')
    this.#forks.set(uid, true)
    this.#started = true

    // ask for value only once for the first fork asking
    if (this.#promiseWaitingForNext === undefined) {
      this.#promiseWaitingForNext = this.#source.next()
    }

    // keep a copy of the promise locally since it may be removed from other forks
    const promise = this.#promiseWaitingForNext
    await this.#waitForAllForks()
    const nextValue = await promise
    console.log({ nextValue })
    this.#promiseWaitingForNext = undefined
    return nextValue
  }

  remove(uid: string, error?: Error) {
    console.log('remove', uid, error, [...this.#forks.keys()], this.#forks.has(uid))
    assert.ok(this.#forks.has(uid))
    this.#forks.delete(uid)

    if (this.#forks.size === 0) {
      console.log('no more forks')
      if (error === undefined) {
        this.#source.return(undefined)
      } else {
        this.#promiseWaitingForForks?.reject(error)
        this.#source.throw(error)
      }
      this.#promiseWaitingForForks = undefined
      this.#promiseWaitingForNext = undefined
    } else {
      console.log('still have forks forks')
      // removing a fork can free the data to flow again
      if (this.nbWaiting === this.#forks.size && this.#promiseWaitingForForks !== undefined) {
        const { resolve } = this.#promiseWaitingForForks

        // reset data
        this.#promiseWaitingForForks = undefined
        this.#forks.forEach((v, k) => this.#forks.set(k, false))
        resolve() // mark the wait of the other forks as over
      }
    }
  }
}

class DebugGenerator<T> implements AsyncGenerator {
  #parent: Forker<T>
  #uid: string
  constructor(parent: Forker<T>, uid: string) {
    this.#parent = parent
    this.#uid = uid
  }
  next(...[value]: [] | [any]): Promise<IteratorResult<T, any>> {
    return this.#parent.next(this.#uid)
  }
  async return(value: any): Promise<IteratorResult<T, any>> {
    this.#parent.remove(this.#uid)
    return { done: true, value: undefined }
  }
  async throw(e: Error): Promise<IteratorResult<T, any>> {
    this.#parent.remove(this.#uid, e)
    return { done: true, value: undefined }
  }
  async *[Symbol.asyncIterator](): AsyncGenerator<T, any, any> {
    while (true) {
      let next = await this.next()
      console.log({ next })
      if (next.done) {
        break
      }
      yield next.value
    }
  }
}
