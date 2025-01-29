export type DiskBlockData = Buffer
export type DiskBlock = {
  index: number
  data: DiskBlockData
}

export type BytesLength = number
export type Uuid = string

export abstract class PortableDifferencingDisk {
  #virtualSize: number
  public get virtualSize(): number {
    return this.#virtualSize
  }
  public set virtualSize(value: number) {
    this.#virtualSize = value
  }

  #parentUuid?: Uuid
  public get parentUuid(): Uuid {
    return this.#parentUuid
  }
  public set parentUuid(value: Uuid) {
    this.#parentUuid = value
  }

  #parentPath?: string
  public get parentPath(): string {
    return this.#parentPath
  }
  public set parentPath(value: string) {
    this.#parentPath = value
  }

  #blockSize: number
  public get blockSize(): number {
    return this.#blockSize
  }
  public set blockSize(value: number) {
    this.#blockSize = value
  }

  generatedDiskBlocks = 0
  yieldedDiskBlocks = 0

  abstract init(): Promise<void>
  abstract close(): Promise<void>
  // return
  abstract getBlockIndexes(): Array<number>
  abstract buildDiskBlockGenerator(): Promise<AsyncGenerator<DiskBlock>> | AsyncGenerator<DiskBlock>

  async *diskBlocks(): AsyncGenerator<DiskBlock> {
    const generator = await this.buildDiskBlockGenerator()
    try {
      for await (const block of generator) {
        this.generatedDiskBlocks++
        yield block
        this.yieldedDiskBlocks++
      }
    } catch (error) {
      console.error({ error, generatedDIskBlocks: this.generatedDiskBlocks, yieldedDiskBlocks: this.yieldedDiskBlocks })
      throw error
    } finally {
      this.close()
    }
  }
}

export class ForkedDifferencingDisk extends PortableDifferencingDisk {
  // some properties only exists on the source disk

  get virtualSize(): number {
    return this.#source.virtualSize
  }
  set virtualSize(value: number) {
    throw new Error(`Can't set the virtualSize of a ForkedDifferencingDisk`)
  }

  get parentUuid(): string {
    return this.#source.parentUuid
  }
  set parentUuid(value: string) {
    throw new Error(`Can't set the parentUuid of a ForkedDifferencingDisk`)
  }

  get parentPath(): string | undefined {
    return this.#source.parentPath
  }
  set parentPath(value: string) {
    throw new Error(`Can't set the parentPath of a ForkedDifferencingDisk`)
  }
  get blockSize(): number {
    return this.#source.blockSize
  }
  set blockSize(value: number) {
    throw new Error(`Can't set the blockSize of a ForkedDifferencingDisk`)
  }

  #source: PortableDifferencingDisk

  constructor(source: PortableDifferencingDisk) {
    super()
    this.#source = source
  }

  buildDiskBlockGenerator(): Promise<AsyncGenerator<DiskBlock>> | AsyncGenerator<DiskBlock> {
    // @todo here : fork the source iteraror
    throw new Error('Method not implemented.')
  }
  init(): Promise<void> {
    return this.#source.init()
  }
  close(): Promise<void> {
    return this.#source.close()
  }
  getBlockIndexes(): Array<number> {
    return this.#source.getBlockIndexes()
  }
}
