const { Readable, Writable } = require('streamx')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const Prefetcher = require('./prefetcher')

class BlobWriteStream extends Writable {
  constructor (core, lock, opts) {
    super(opts)
    this.id = {}
    this.core = core
    this._lock = lock
    this._release = null
    this._batch = []
  }

  _open (cb) {
    this.core.ready().then(() => {
      this._lock(release => {
        this._release = release
        this.id.byteOffset = this.core.byteLength
        this.id.blockOffset = this.core.length
        return cb(null)
      })
    }, err => cb(err))
  }

  _final (cb) {
    this._append(err => {
      if (err) return cb(err)
      this.id.blockLength = this.core.length - this.id.blockOffset
      this.id.byteLength = this.core.byteLength - this.id.byteOffset
      return cb(null)
    })
  }

  _destroy (cb) {
    if (this._release) this._release()
    cb(null)
  }

  _append (cb) {
    if (!this._batch.length) return cb(null)
    return this.core.append(this._batch).then(() => {
      this._batch = []
      return cb(null)
    }, err => {
      this._batch = []
      return cb(err)
    })
  }

  _write (data, cb) {
    this._batch.push(data)
    if (this._batch.length >= 16) return this._append(cb)
    return cb(null)
  }
}

class BlobReadStream extends Readable {
  constructor (core, id, opts = {}) {
    super(opts)
    this.id = id
    this.core = core.session({ wait: opts.wait, timeout: opts.timeout })

    const start = id.blockOffset
    const end = id.blockOffset + id.blockLength

    this._prefetch = (opts.wait === false || opts.prefetch === false) ? null : new Prefetcher(this.core, { max: opts.prefetch, start, end })
    this._lastPrefetch = null

    this._pos = opts.start !== undefined ? id.byteOffset + opts.start : id.byteOffset

    if (opts.length !== undefined) this._end = this._pos + opts.length
    else if (opts.end !== undefined) this._end = id.byteOffset + opts.end + 1
    else this._end = id.byteOffset + id.byteLength

    this._index = 0
    this._relativeOffset = 0
    this._bytesRead = 0
  }

  _open (cb) {
    if (this._pos === this.id.byteOffset) {
      this._index = this.id.blockOffset
      this._relativeOffset = 0
      return cb(null)
    }

    this.core.seek(this._pos, {
      start: this.id.blockOffset,
      end: this.id.blockOffset + this.id.blockLength
    }).then(result => {
      if (!result) return cb(BLOCK_NOT_AVAILABLE())

      this._index = result[0]
      this._relativeOffset = result[1]
      return cb(null)
    }, err => cb(err))
  }

  _predestroy () {
    if (this._prefetch) this._prefetch.destroy()
    this.core.close().then(noop, noop)
  }

  _destroy (cb) {
    if (this._prefetch) this._prefetch.destroy()
    this.core.close().then(cb, cb)
  }

  _read (cb) {
    if (this._pos >= this._end) {
      this.push(null)
      return cb(null)
    }

    if (this._prefetch) this._prefetch.update(this._index)

    this.core.get(this._index).then(block => {
      if (!block) return cb(BLOCK_NOT_AVAILABLE())

      const remainder = this._end - this._pos
      if (this._relativeOffset || (remainder < block.length)) {
        block = block.subarray(this._relativeOffset, this._relativeOffset + remainder)
      }

      this._index++
      this._relativeOffset = 0
      this._pos += block.length
      this._bytesRead += block.length

      this.push(block)
      return cb(null)
    }, err => cb(err))
  }
}

module.exports = {
  BlobReadStream,
  BlobWriteStream
}

function noop () {}
