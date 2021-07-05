const mutexify = require('mutexify')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')

const DEFAULT_BLOCK_SIZE = 2 ** 16

module.exports = class Hyperblobs {
  constructor (core, opts = {}) {
    this.core = core
    this.blockSize = opts.blockSize || DEFAULT_BLOCK_SIZE

    this._lock = mutexify()
    this._core = core
  }

  get feed () {
    return this.core
  }

  get locked () {
    return this._lock.locked
  }

  async put (blob, opts) {
    if (!Buffer.isBuffer(blob)) blob = Buffer.from(blob)
    const blockSize = (opts && opts.blockSize) || this.blockSize

    const stream = this.createWriteStream(opts)
    for (let i = 0; i < blob.length; i += blockSize) {
      stream.write(blob.slice(i, i + blockSize))
    }
    stream.end()

    return new Promise((resolve, reject) => {
      stream.once('error', reject)
      stream.once('close', () => resolve(stream.id))
    })
  }

  async get (id, opts) {
    const res = []
    for await (const block of this.createReadStream(id, opts)) {
      res.push(block)
    }
    return Buffer.concat(res)
  }

  createReadStream (id, opts) {
    return new BlobReadStream(this._core, id, opts)
  }

  createWriteStream (opts) {
    return new BlobWriteStream(this._core, this._lock, opts)
  }
}
