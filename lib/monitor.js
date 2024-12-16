const ReadyResource = require('ready-resource')
const speedometer = require('speedometer')

module.exports = class Monitor extends ReadyResource {
  constructor (blobs, id) {
    super()
    if (!blobs) throw new Error('blobs is required')
    if (!id) throw new Error('id is required')

    this.blobs = blobs
    this.id = id
    this.peers = 0
    this.uploadSpeedometer = null
    this.downloadSpeedometer = null

    this._boundOnUpload = this._onUpload.bind(this)
    this._boundOnDownload = this._onDownload.bind(this)
    this._boundPeerUpdate = this._updatePeers.bind(this)

    const stats = {
      startTime: 0,
      percentage: 0,
      peers: 0,
      speed: 0,
      blocks: 0,
      totalBytes: 0, // local + bytes loaded during monitoring
      monitoringBytes: 0, // bytes loaded during monitoring
      targetBytes: 0,
      targetBlocks: 0
    }

    this.uploadStats = { ...stats }
    this.downloadStats = { ...stats }
    this.uploadStats.targetBytes = this.downloadStats.targetBytes = this.id.byteLength
    this.uploadStats.targetBlocks = this.downloadStats.targetBlocks = this.id.blockLength
    this.uploadStats.peers = this.downloadStats.peers = this.peers = this.blobs.core.peers.length

    this.uploadSpeedometer = speedometer()
    this.downloadSpeedometer = speedometer()

    // Handlers
    this.blobs.core.on('peer-add', this._boundPeerUpdate)
    this.blobs.core.on('peer-remove', this._boundPeerUpdate)
    this.blobs.core.on('upload', this._boundOnUpload)
    this.blobs.core.on('download', this._boundOnDownload)
  }

  _close () {
    this.blobs.core.off('peer-add', this._boundPeerUpdate)
    this.blobs.core.off('peer-remove', this._boundPeerUpdate)
    this.blobs.core.off('upload', this._boundOnUpload)
    this.blobs.core.off('download', this._boundOnDownload)

    this.blobs.monitors.delete(this)
  }

  _onUpload (index, bytes, from) {
    this._updateStats(this.uploadSpeedometer, this.uploadStats, index, bytes, from)
  }

  _onDownload (index, bytes, from) {
    this._updateStats(this.downloadSpeedometer, this.downloadStats, index, bytes, from)
  }

  _updatePeers () {
    this.uploadStats.peers = this.downloadStats.peers = this.peers = this.blobs.core.peers.length
    this.emit('update')
  }

  _updateStats (speed, stats, index, bytes) {
    if (this.closing) return
    if (!isWithinRange(index, this.id)) return

    if (!stats.startTime) stats.startTime = Date.now()

    stats.speed = speed(bytes)
    stats.blocks++
    stats.totalBytes += bytes
    stats.monitoringBytes += bytes
    stats.percentage = toFixed(stats.blocks / stats.targetBlocks * 100)

    this.emit('update')
  }

  downloadSpeed () {
    return this.downloadSpeedometer ? this.downloadSpeedometer() : 0
  }

  uploadSpeed () {
    return this.uploadSpeedometer ? this.uploadSpeedometer() : 0
  }
}

function isWithinRange (index, { blockOffset, blockLength }) {
  return index >= blockOffset && index < blockOffset + blockLength
}

function toFixed (n) {
  return Math.round(n * 100) / 100
}
