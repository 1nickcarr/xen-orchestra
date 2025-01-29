import { PortableDifferencingDisk, type DiskBlock } from '../PortableDisk.mts'
import { readChunkStrict, skipStrict } from '@vates/read-chunk'

import { unpackFooter, unpackHeader } from 'vhd-lib/Vhd/_utils.js'
import { BLOCK_UNUSED, FOOTER_SIZE, HEADER_SIZE, SECTOR_SIZE } from 'vhd-lib/_constants.js'
import { type Readable } from 'node:stream'
import assert from 'node:assert'

export class XapiVhdStreamSource extends PortableDifferencingDisk {
  #vhdStream: Readable
  #busy = false
  #streamOffset = 0
  #blocks: Array<{ index: number; offset: number }> = []

  #initDone = false

  constructor(vhdStream: Readable) {
    super()
    this.#vhdStream = vhdStream
  }

  async #read(length: number): Promise<Buffer> {
    if (this.#busy) {
      throw new Error("Can't read/skip multiple block in parallel")
    }
    this.#busy = true
    const data = (await readChunkStrict(this.#vhdStream, length)) as Buffer
    this.#streamOffset += length
    this.#busy = false
    return data
  }

  async #skip(length: number): Promise<void> {
    if (this.#busy) {
      throw new Error("Can't read/skip multiple block in parallel")
    }
    this.#busy = true
    await skipStrict(this.#vhdStream, length)
    this.#streamOffset += length
    this.#busy = false
  }

  async init(): Promise<void> {
    const footer = unpackFooter(await this.#read(FOOTER_SIZE))
    this.virtualSize = footer.currentSize
    const header = unpackHeader(await this.#read(HEADER_SIZE))
    this.blockSize = header.blockSize
    const batSize = Math.ceil((header.maxTableEntries * 4) / SECTOR_SIZE) * SECTOR_SIZE
    // skip space between header and beginning of the table
    console.log({ header, already: FOOTER_SIZE + HEADER_SIZE })
    await this.#skip(header.tableOffset - (FOOTER_SIZE + HEADER_SIZE))
    const bat = await this.#read(batSize)
    const blocks = []
    for (let index = 0; index < header.maxTableEntries; index++) {
      const offset = bat.readUInt32BE(index * 4)
      if (offset !== BLOCK_UNUSED) {
        blocks.push({ index, offset: (offset + 1) /* skip block bitmap */ * SECTOR_SIZE })
      }
    }
    // ensure we will generate the blocks in the same order as the stream
    blocks.sort((b1, b2) => b1.offset - b2.offset)
    console.log(blocks)
    this.#blocks = blocks
    this.#initDone = true
    console.log('init done')
  }

  async close() {
    assert.strictEqual(this.#initDone, true, 'init must be done to call close')
    this.#vhdStream.destroy()
  }

  getBlockIndexes(): Array<number> {
    assert.strictEqual(this.#initDone, true, 'init must be done to call getBlockIndexes')
    return this.#blocks.map(({ index }) => index)
  }
  async *buildDiskBlockGenerator(): AsyncGenerator<DiskBlock> {
    assert.strictEqual(this.#initDone, true, 'init must be done to call buildDiskGenerator')
    const blockIndexes = this.#blocks
    for (const { offset, index } of blockIndexes) {
      // if this fails, that means an error on source
      await this.#skip(offset - this.#streamOffset) // this will skip the bitmap
      const data = await this.#read(2 * 1024 * 1024)

      // if this fails that means a error from consumer
      yield {
        index,
        data,
      }
    }
  }
}
