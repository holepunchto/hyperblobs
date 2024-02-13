// should move to hypercore itself

module.exports = class Prefetcher {
  constructor (core, { max = 64, start = 0, end = core.length, linear = true } = {}) {
    this.core = core
    this.max = max
    this.range = null
    this.startBound = start
    this.endBound = end

    this.start = start
    this.end = start
    this.linear = linear
    this.missing = 0

    this._ondownloadBound = this._ondownload.bind(this)
    this.core.on('download', this._ondownloadBound)
  }

  _ondownload (index) {
    if (this.range && index < this.end && this.start <= index) {
      this.missing--
      this._update()
    }
  }

  destroy () {
    this.core.off('download', this._ondownloadBound)
    if (this.range) this.range.destroy()
    this.range = null
    this.max = 0
  }

  update (position) {
    this.start = position
    if (!this.range) this._update()
  }

  _update () {
    if (this.missing >= this.max) return
    if (this.range) this.range.destroy()

    let end = this.end

    while (end < this.endBound && this.missing < this.max) {
      end = this.core.core.bitfield.firstUnset(end) + 1
      if (end >= this.endBound) break
      this.missing++
    }

    if (end >= this.endBound) end = this.endBound

    this.end = end

    if (this.start >= this.end) return

    this.range = this.core.download({
      start: this.start,
      end: this.end,
      linear: this.linear
    })
  }
}
