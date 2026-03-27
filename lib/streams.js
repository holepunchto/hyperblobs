const { Readable, Writable } = require('streamx')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const Prefetcher = require('./prefetcher')
const blockMap = require('./block-map')

class BlobWriteStream extends Writable {
  constructor(core, lock, opts = {}) {
    super(opts)
    this.id = { blockOffset: 0, byteOffset: 0, blockLength: 0, byteLength: 0 }
    this.core = core

    this._dedup = !!opts.dedup
    this._blob = opts.blob || null
    this._hashes = null
    this._addBlockMap = !!opts.blockMap || this._dedup
    this._blockMap = this._addBlockMap ? { version: 0, blocks: [] } : null
    this._lock = lock
    this._release = null
    this._batch = []

    if (this._addBlockMap) this.id.blockMap = true
  }

  async _openp() {
    await this.core.ready()
    const release = await new Promise((resolve) => this._lock(resolve))

    this._release = release
    this.id.byteOffset = this.core.byteLength
    this.id.blockOffset = this.core.length

    if (!this._dedup) return

    if (this._blob) {
      const map = await blockMap.get(this.core, this._blob, { hashes: true })
      this._hashes = map.hashes
    } else {
      this._hashes = new Map()
    }
  }

  _open(cb) {
    this._openp().then(cb, cb)
  }

  async _finalp() {
    await this._append()

    if (this._blockMap) {
      const buffers = await blockMap.encode(this._blockMap)
      this.id.blockOffset = this.core.length
      this.id.byteOffset = this.core.byteLength
      await this.core.append(buffers)
    }

    this.id.blockLength = this.core.length - this.id.blockOffset
    this.id.byteLength = this.core.byteLength - this.id.byteOffset
  }

  _final(cb) {
    this._finalp().then(cb, cb)
  }

  _destroy(cb) {
    if (this._release) this._release()
    cb(null)
  }

  async _append() {
    if (!this._batch.length) return

    const batch = this._batch
    this._batch = []

    await this.core.append(batch)
  }

  _write(data, cb) {
    let dup = false

    if (this._blockMap) {
      let entry = {
        index: this.core.length + this._batch.length,
        byteLength: data.byteLength
      }

      if (this._hashes) {
        const id = blockMap.hash(data)
        const existing = this._hashes.get(id)

        if (existing) {
          entry = existing
          dup = true
        } else {
          this._hashes.set(id, entry)
        }
      }

      this._blockMap.blocks.push(entry)
    }

    if (dup) return cb()

    this._batch.push(data)

    if (this._batch.length >= 16) {
      this._append().then(cb, cb)
      return
    }

    return cb()
  }
}

class BlockMapReadStream extends Readable {
  constructor(core, id, opts = {}) {
    super(opts)
    this.id = id
    this.core = core.session({ wait: opts.wait, timeout: opts.timeout })

    const noPrefetch = opts.wait === false || opts.prefetch === false || !core.core
    const start = opts.start || 0
    const end =
      opts.end === undefined ? (opts.length === undefined ? -1 : start + opts.length) : opts.end + 1

    this._blockMap = null

    this._rangeStart = start
    this._rangeEnd = end

    this._startIndex = 0
    this._startOffset = 0
    this._endIndex = -1
    this._endOffset = -1

    this._range = null
    this._noPrefetch = noPrefetch
  }

  async _openp() {
    this._blockMap = await blockMap.get(this.core, this.id)

    const [startIndex, startOffset, endIndex, endLength] = seekBlockMap(
      this._blockMap,
      this._rangeStart,
      this._rangeEnd
    )

    this._startIndex = startIndex
    this._startOffset = startOffset
    this._endIndex = endIndex
    this._endOffset = endLength

    if (this._endIndex === -1) {
      this._endIndex = this._blockMap.blocks.length
      this._endOffset = 0
    }
  }

  _open(cb) {
    this._openp().then(cb, cb)
  }

  _predestroy() {
    if (this._range) this._range.destroy()
    this.core.close().then(noop, noop)
  }

  _destroy(cb) {
    if (this._range) this._range.destroy()
    this.core.close().then(cb, cb)
  }

  _prefetch(index) {
    const blocks = []
    for (; index < this._endIndex; index++) blocks.push(this._blockMap.blocks[index].index)
    this._range = this.core.download({ blocks })
  }

  async _readp() {
    if (this._startIndex >= this._endIndex) {
      this.push(null)
      return
    }

    let block = null

    const index = this._startIndex++
    const b = this._blockMap.blocks[index]

    if (!this._range && !this._noPrefetch) {
      block = await this.core.get(b.index, { wait: false })
      if (!block) this._prefetch(index)
    }

    if (!block) {
      block = await this.core.get(b.index)
    }

    if (!block) throw BLOCK_NOT_AVAILABLE()

    if (this._startOffset) {
      block = block.subarray(this._startOffset)
      this._startOffset = 0
    }

    if (this._startIndex === this._endIndex && this._endOffset) {
      block = block.subarray(0, block.byteLength - this._endOffset)
    }

    this.push(block)
  }

  _read(cb) {
    this._readp().then(cb, cb)
  }
}

class BlobReadStream extends Readable {
  constructor(core, id, opts = {}) {
    super(opts)
    this.id = id
    this.core = core.session({ wait: opts.wait, timeout: opts.timeout })

    const start = id.blockOffset
    const end = id.blockOffset + id.blockLength
    const noPrefetch = opts.wait === false || opts.prefetch === false || !core.core

    this._prefetch = noPrefetch
      ? null
      : new Prefetcher(this.core, { max: opts.prefetch, start, end })
    this._lastPrefetch = null

    this._pos = opts.start !== undefined ? id.byteOffset + opts.start : id.byteOffset

    if (opts.length !== undefined) this._end = this._pos + opts.length
    else if (opts.end !== undefined) this._end = id.byteOffset + opts.end + 1
    else this._end = id.byteOffset + id.byteLength

    this._index = 0
    this._relativeOffset = 0
    this._bytesRead = 0
  }

  async _openp() {
    if (this._pos === this.id.byteOffset) {
      this._index = this.id.blockOffset
      this._relativeOffset = 0
      return
    }

    const result = await this.core.seek(this._pos, {
      start: this.id.blockOffset,
      end: this.id.blockOffset + this.id.blockLength
    })

    if (!result) throw BLOCK_NOT_AVAILABLE()

    this._index = result[0]
    this._relativeOffset = result[1]
  }

  _open(cb) {
    this._openp().then(cb, cb)
  }

  _predestroy() {
    if (this._prefetch) this._prefetch.destroy()
    this.core.close().then(noop, noop)
  }

  _destroy(cb) {
    if (this._prefetch) this._prefetch.destroy()
    this.core.close().then(cb, cb)
  }

  async _readp() {
    if (this._pos >= this._end) {
      this.push(null)
      return
    }

    if (this._prefetch) this._prefetch.update(this._index)

    let block = await this.core.get(this._index)
    if (!block) throw BLOCK_NOT_AVAILABLE()

    const remainder = this._end - this._pos
    if (this._relativeOffset || remainder < block.length) {
      block = block.subarray(this._relativeOffset, this._relativeOffset + remainder)
    }

    this._index++
    this._relativeOffset = 0
    this._pos += block.length
    this._bytesRead += block.length

    this.push(block)
  }

  _read(cb) {
    this._readp().then(cb, cb)
  }
}

module.exports = {
  BlockMapReadStream,
  BlobReadStream,
  BlobWriteStream
}

function noop() {}

function seekBlockMap(map, start, end) {
  let s = -1
  let so = -1
  let e = -1
  let eo = -1

  for (let i = 0; i < map.blocks.length; i++) {
    const b = map.blocks[i]

    if (s === -1) {
      if (start < b.byteLength) {
        s = i
        so = start
      } else {
        start -= b.byteLength
      }
    }

    if (e === -1 && end > -1) {
      if (end <= b.byteLength) {
        e = i + 1
        eo = b.byteLength - end
      } else {
        end -= b.byteLength
      }
    }
  }

  return [s, so, e, eo]
}
