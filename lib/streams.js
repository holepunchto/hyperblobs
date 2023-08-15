const { Readable, Writable } = require('streamx')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')

class SparsePrefetcher {
  constructor (opts = {}) {
    this._linear = opts.linear !== false
    this._length = Math.min(16, opts.length || 10) // max 16 until ranges can be aborted efficiently
  }

  prefetch (bounds, index) {
    const startBlock = bounds.blockOffset
    const endBlock = startBlock + bounds.blockLength

    return {
      start: Math.max(index, startBlock),
      end: Math.min(index + this._length, endBlock),
      linear: this._linear
    }
  }
}

class BlobWriteStream extends Writable {
  constructor (core, lock, opts) {
    super(opts)

    this.core = core
    this.id = { byteOffset: 0, blockOffset: 0, blockLength: 0, byteLength: 0 }

    this._lock = lock
    this._release = null
    this._batch = []
  }

  _open (cb) {
    this._openp().then(cb, cb)
  }

  _write (data, cb) {
    this._writep(data).then(cb, cb)
  }

  _final (cb) {
    this._finalp().then(cb, cb)
  }

  _destroy (cb) {
    if (this._release) this._release()
    cb(null)
  }

  async _openp () {
    await this.core.ready()

    this._release = await this._lock()

    this.id.byteOffset = this.core.byteLength
    this.id.blockOffset = this.core.length
  }

  async _writep (data) {
    this._batch.push(data)

    if (this._batch.length >= 16) await this._append()
  }

  async _finalp () {
    await this._append()

    this.id.blockLength = this.core.length - this.id.blockOffset
    this.id.byteLength = this.core.byteLength - this.id.byteOffset
  }

  async _append () {
    if (!this._batch.length) return

    try {
      await this.core.append(this._batch)
    } finally {
      this._batch = []
    }
  }
}

class BlobReadStream extends Readable {
  constructor (core, id, opts = {}) {
    super(opts)

    this.core = core.session({ wait: opts.wait, timeout: opts.timeout })
    this.id = id

    this._prefetch = opts.wait === false ? noPrefetch : opts.prefetch
    this._lastPrefetch = null
    if (!this._prefetch) {
      const prefetcher = new SparsePrefetcher(opts)
      this._prefetch = prefetcher.prefetch.bind(prefetcher)
    }

    this._pos = opts.start !== undefined ? id.byteOffset + opts.start : id.byteOffset

    if (opts.length !== undefined) this._end = this._pos + opts.length
    else if (opts.end !== undefined) this._end = id.byteOffset + opts.end + 1
    else this._end = id.byteOffset + id.byteLength

    this._index = 0
    this._relativeOffset = 0
    this._bytesRead = 0
  }

  _open (cb) {
    this._openp().then(cb, cb)
  }

  _read (cb) {
    this._readp().then(cb, cb)
  }

  _predestroy () {
    this.core.close().then(noop, noop)
  }

  _destroy (cb) {
    this.core.close().then(cb, cb)
  }

  async _openp (cb) {
    if (this._pos === this.id.byteOffset) {
      this._index = this.id.blockOffset
      this._relativeOffset = 0
      return
    }

    const seek = await this.core.seek(this._pos, {
      start: this.id.blockOffset,
      end: this.id.blockOffset + this.id.blockLength
    })

    if (!seek) throw BLOCK_NOT_AVAILABLE()

    this._index = seek[0]
    this._relativeOffset = seek[1]
  }

  async _readp (cb) {
    if (this._pos >= this._end) {
      this.push(null)
      return
    }

    const prefetch = this._prefetch(this.id, this._index)
    if (prefetch) {
      if (this._lastPrefetch) this.core.undownload(this._lastPrefetch)
      this._lastPrefetch = this.core.download(prefetch)
    }

    let block = await this.core.get(this._index)
    if (!block) throw BLOCK_NOT_AVAILABLE()

    const remainder = this._end - this._pos
    if (this._relativeOffset || (remainder < block.length)) {
      block = block.subarray(this._relativeOffset, this._relativeOffset + remainder)
    }

    this._index++
    this._relativeOffset = 0
    this._pos += block.length
    this._bytesRead += block.length

    this.push(block)
  }
}

module.exports = {
  BlobReadStream,
  BlobWriteStream
}

function noop () {}

function noPrefetch () {
  return null
}
