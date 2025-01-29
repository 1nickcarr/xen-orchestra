import { Readable } from 'stream'
import { BaseVhd } from './BaseVhd.mts'
import { SECTOR_SIZE } from 'vhd-lib/_constants.js'

export interface VhdStream extends Readable {
  length: number
}

export class VhdStream extends BaseVhd {
  toStream(): Readable {
    const footer = this.computeVhdFooter()
    const header = this.computeVhdHeader()
    const { bat, fileSize } = this.computeVhdBatAndFileSize() // the bat contains the calculated position of the futures blocks
    const blocks = this.source.diskBlocks()
    const FULL_BLOCK_BITMAP = Buffer.alloc(SECTOR_SIZE, 255)
    async function* generator(): AsyncGenerator<Buffer> {
      yield footer
      yield header
      yield bat
      for await (const { data } of blocks) {
        yield Buffer.concat([FULL_BLOCK_BITMAP, data])
      }
      yield footer
    }
    const stream = Readable.from(generator()) as VhdStream
    stream.length = fileSize
    return stream
  }
}
