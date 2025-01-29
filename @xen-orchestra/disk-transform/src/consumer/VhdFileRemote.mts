import type { PortableDifferencingDisk } from '../PortableDisk.mts'
import { VhdStream } from './VhdStream.mts'

type VhdRemoteTarget = {
  handler: any
  path: string
}

export class VhdFileRemote extends VhdStream {
  #target: VhdRemoteTarget
  constructor(source: PortableDifferencingDisk, target: VhdRemoteTarget) {
    super(source)
    this.#target = target
  }

  async write() {
    const stream = await this.toStream()
    await this.#target.handler.outputStream(stream)
  }
}
