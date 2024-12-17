const mutexify = require('mutexify')
const b4a = require('b4a')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')
const Monitor = require('./lib/monitor')

const DEFAULT_BLOCK_SIZE = 2 ** 16

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

  async get (id, opts) {
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
