const mutexify = require('mutexify')
const b4a = require('b4a')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')
const Monitor = require('./lib/monitor')

const DEFAULT_BLOCK_SIZE = 2 ** 16

class HyperBlobsBatch {
  constructor (blobs) {
    this.blobs = blobs
    this.blocks = []
    this.bytes = 0
  }

  ready () {
    return this.blobs.ready()
  }

  async put (buffer) {
    if (!this.blobs.core.opened) await this.blobs.core.ready()

    const blockSize = this.blobs.blockSize
    const result = {
      blockOffset: this.blobs.core.length + this.blocks.length,
      blockLength: 0,
      byteOffset: this.blobs.core.byteLength + this.bytes,
      byteLength: 0
    }

    let offset = 0
    while (offset < buffer.byteLength) {
      const blk = buffer.subarray(offset, offset + blockSize)
      offset += blockSize

      result.blockLength++
      result.byteLength += blk.byteLength
      this.bytes += blk.byteLength
      this.blocks.push(blk)
    }

    return result
  }

  async get (id) {
    if (id.blockOffset < this.blobs.core.length) {
      return this.blobs.get(id)
    }

    const bufs = []

    for (let i = id.blockOffset - this.blobs.core.length; i < id.blockOffset + id.blockLength; i++) {
      if (i >= this.blocks.length) return null
      bufs.push(this.blocks[i])
    }

    return bufs.length === 1 ? bufs[0] : b4a.concat(bufs)
  }

  async flush () {
    await this.blobs.core.append(this.blocks)
    this.blocks = []
    this.bytes = 0
  }

  close () {
    // noop, atm nothing to unlink
  }
}

class Hyperblobs {
  constructor (core, opts = {}) {
    this.core = core
    this.blockSize = opts.blockSize || DEFAULT_BLOCK_SIZE

    this._lock = mutexify()
    this._core = core
    this._monitors = new Set()

    this._boundUpdatePeers = this._updatePeers.bind(this)
    this._boundOnUpload = this._onUpload.bind(this)
    this._boundOnDownload = this._onDownload.bind(this)
  }

  get feed () {
    return this.core
  }

  get locked () {
    return this._lock.locked
  }

  ready () {
    return this.core.ready()
  }

  close () {
    return this.core.close()
  }

  batch () {
    return new HyperBlobsBatch(this)
  }

  snapshot () {
    return new Hyperblobs(this.core.snapshot())
  }

  async put (blob, opts) {
    if (!b4a.isBuffer(blob)) blob = b4a.from(blob)
    const blockSize = (opts && opts.blockSize) || this.blockSize

    const stream = this.createWriteStream(opts)
    for (let i = 0; i < blob.length; i += blockSize) {
      stream.write(blob.subarray(i, i + blockSize))
    }
    stream.end()

    return new Promise((resolve, reject) => {
      stream.once('error', reject)
      stream.once('close', () => resolve(stream.id))
    })
  }

  async _getAll (id, opts) {
    if (id.blockLength === 1) return this.core.get(id.blockOffset, opts)

    const promises = new Array(id.blockLength)
    for (let i = 0; i < id.blockLength; i++) {
      promises[i] = this.core.get(id.blockOffset + i, opts)
    }

    const blocks = await Promise.all(promises)
    for (let i = 0; i < id.blockLength; i++) {
      if (blocks[i] === null) return null
    }
    return b4a.concat(blocks)
  }

  async get (id, opts) {
    const all = !opts || (!opts.start && opts.length === undefined && opts.end === undefined && !opts.core)
    if (all) return this._getAll(id, opts)

    const res = []
    try {
      for await (const block of this.createReadStream(id, opts)) {
        res.push(block)
      }
    } catch (error) {
      if (error.code === 'BLOCK_NOT_AVAILABLE') return null
      throw error
    }

    if (res.length === 1) return res[0]
    return b4a.concat(res)
  }

  async clear (id, opts) {
    return this.core.clear(id.blockOffset, id.blockOffset + id.blockLength, opts)
  }

  createReadStream (id, opts) {
    const core = (opts && opts.core) ? opts.core : this._core
    return new BlobReadStream(core, id, opts)
  }

  createWriteStream (opts) {
    const core = (opts && opts.core) ? opts.core : this._core
    return new BlobWriteStream(core, this._lock, opts)
  }

  monitor (id) {
    const monitor = new Monitor(this, id)
    if (this._monitors.size === 0) this._startListening()
    this._monitors.add(monitor)
    return monitor
  }

  _removeMonitor (mon) {
    this._monitors.delete(mon)
    if (this._monitors.size === 0) this._stopListening()
  }

  _updatePeers () {
    for (const m of this._monitors) m._updatePeers()
  }

  _onUpload (index, bytes, from) {
    for (const m of this._monitors) m._onUpload(index, bytes, from)
  }

  _onDownload (index, bytes, from) {
    for (const m of this._monitors) m._onDownload(index, bytes, from)
  }

  _startListening () {
    this.core.on('peer-add', this._boundUpdatePeers)
    this.core.on('peer-remove', this._boundUpdatePeers)
    this.core.on('upload', this._boundOnUpload)
    this.core.on('download', this._boundOnDownload)
  }

  _stopListening () {
    this.core.off('peer-add', this._boundUpdatePeers)
    this.core.off('peer-remove', this._boundUpdatePeers)
    this.core.off('upload', this._boundOnUpload)
    this.core.off('download', this._boundOnDownload)
  }
}

module.exports = Hyperblobs
