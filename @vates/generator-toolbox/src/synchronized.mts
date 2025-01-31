export class SynchronizedAsyncGenerator<T> {
  private source: AsyncGenerator<T>
  private forks: Set<AsyncGenerator<T>> = new Set()
  private sourceDone: boolean = false
  private sourceError: Error | null = null
  private currentValue: T | null = null
  private waitingResolvers: ((value: IteratorResult<T>) => void)[] = []
  private isConsuming: boolean = false
  private sourceTimeout: number
  private forkTimeout: number

  constructor(
    source: AsyncGenerator<T>,
    sourceTimeout: number = 5 * 60 * 1000, // Default: 5 minutes
    forkTimeout: number = 5 * 60 * 1000 // Default: 5 minutes
  ) {
    this.source = source
    this.sourceTimeout = sourceTimeout
    this.forkTimeout = forkTimeout
  }

  private async consumeSource() {
    if (this.isConsuming) return
    this.isConsuming = true

    try {
      while (true) {
        // Fetch the next value from the source with a timeout
        const nextValue = await Promise.race([
          this.source.next(),
          new Promise<IteratorResult<T>>((_, reject) =>
            setTimeout(() => reject(new Error('Source timeout')), this.sourceTimeout)
          ),
        ])

        if (nextValue.done) {
          this.sourceDone = true
          break
        }

        this.currentValue = nextValue.value
        this.notifyForks()
        await this.waitForForksToCatchUp()
      }
    } catch (error) {
      this.sourceError = error as Error
    } finally {
      this.sourceDone = true
      this.notifyForks()
    }
  }

  private notifyForks() {
    while (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift()!
      if (this.sourceError) {
        resolver({ done: true, value: undefined }) // Signal error
      } else if (this.currentValue !== null) {
        resolver({ done: false, value: this.currentValue })
      } else if (this.sourceDone) {
        resolver({ done: true, value: undefined })
      }
    }
  }

  private async waitForForksToCatchUp() {
    // Wait until all forks have consumed the current value
    while (this.waitingResolvers.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 10)) // Small delay
    }
  }

  async *fork(): AsyncGenerator<T> {
    if (this.sourceError) {
      throw this.sourceError
    }

    while (true) {
      if (this.sourceDone) {
        break
      } else if (this.sourceError) {
        throw this.sourceError
      } else {
        // Request the next value from the source with a timeout
        const nextValue = await Promise.race([
          new Promise<IteratorResult<T>>(resolve => {
            this.waitingResolvers.push(resolve)
            this.consumeSource()
          }),
          new Promise<IteratorResult<T>>((_, reject) =>
            setTimeout(() => reject(new Error('Fork timeout')), this.forkTimeout)
          ),
        ])

        if (nextValue.done) {
          break
        }
        yield nextValue.value
      }
    }
  }
}
