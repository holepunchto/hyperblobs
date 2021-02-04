const AbortController = require('abort-controller')
const { Readable, Writable } = require('streamx')
const { toCallbacks } = require('hypercore-promisifier')

class SparsePrefetcher {
  constructor (opts = {}) {
    this._linear = opts.linear !== false
    this._length = opts.length || 10
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

/* eslint-disable no-unused-vars */
class EagerPrefetcher {
  constructor (opts = {}) {
    this._linear = opts.linear !== false
    this._done = false
  }

  prefetch (bounds) {
    if (this._done) return null
    this._done = true
    const startBlock = bounds.blockOffset
    const endBlock = startBlock + bounds.blockLength
    return { start: startBlock, end: endBlock, linear: this._linear }
  }
}

class BlobWriteStream extends Writable {
  constructor (core, lock, opts) {
    super(opts)
    this.id = {}
    this.core = toCallbacks(core)
    this._lock = lock
    this._release = null
  }

  _open (cb) {
    this.core.ready(err => {
      if (err) return cb(err)
      this._lock(release => {
        this._release = release
        this.id.byteOffset = this.core.byteLength
        this.id.blockOffset = this.core.length
        return cb(null)
      })
    })
  }

  _final (cb) {
    this.id.blockLength = this.core.length - this.id.blockOffset
    this.id.byteLength = this.core.byteLength - this.id.byteOffset
    this._release()
    return cb(null)
  }

  _write (data, cb) {
    return this.core.append(data, err => {
      if (err) return cb(err)
      return cb(null)
    })
  }
}

class BlobReadStream extends Readable {
  constructor (core, id, opts = {}) {
    super(opts)
    this.id = id
    this.core = toCallbacks(core)

    this._controller = new AbortController()
    this._prefetch = opts.prefetch
    this._lastPrefetch = null
    if (!this._prefetch) {
      const prefetcher = new SparsePrefetcher(opts)
      this._prefetch = prefetcher.prefetch.bind(prefetcher)
    }

    this._pos = opts.start !== undefined ? id.byteOffset + opts.start : id.byteOffset

    if (opts.length !== undefined) this._end = this._pos + opts.length
    else if (opts.end !== undefined) this._end = id.byteOffset + opts.end + 1
    else this._end = id.byteOffset + id.byteLength

    this._index = null
    this._relativeOffset = null
    this._bytesRead = 0
  }

  _open (cb) {
    this.core.seek(this._pos, {
      start: this.id.blockOffset,
      end: this.id.blockOffset + this.id.blockLength
    }, (err, result) => {
      if (err) return cb(err)
      this._index = result[0]
      this._relativeOffset = result[1]
      return cb(null)
    })
  }

  _read (cb) {
    if (this._pos >= this._end) {
      this.push(null)
      return cb(null)
    }

    const prefetch = this._prefetch(this.id, this._index)
    if (prefetch) {
      if (this._lastPrefetch) this.core.undownload(this._lastPrefetch)
      this._lastPrefetch = this.core.download(prefetch)
    }

    this.core.get(this._index, { signal: this._controller.signal }, (err, block) => {
      if (err) return cb(err)
      const remainder = this._end - this._pos
      if (this._relativeOffset || (remainder < block.length)) {
        block = block.slice(this._relativeOffset, this._relativeOffset + remainder)
      }

      this._index++
      this._relativeOffset = 0
      this._pos += block.length
      this._bytesRead += block.length

      this.push(block)
      return cb(null)
    })
  }
}

module.exports = {
  BlobReadStream,
  BlobWriteStream
}
