const c = require('compact-encoding')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const block = {
  preencode(state, m) {
    c.uint.preencode(state, m.index)
    c.uint.preencode(state, m.byteLength)
  },
  encode(state, m) {
    c.uint.encode(state, m.index)
    c.uint.encode(state, m.byteLength)
  },
  decode(state) {
    return {
      index: c.uint.decode(state),
      byteLength: c.uint.decode(state)
    }
  }
}

const list = c.array(block)

const map = {
  preencode(state, m) {
    c.uint.preencode(state, 0)
    list.preencode(state, m.blocks)
  },
  encode(state, m) {
    c.uint.encode(state, 0)
    list.encode(state, m.blocks)
  },
  decode(state) {
    const version = c.uint.decode(state)
    if (version > 0) throw new Error('Unsupported block map version')

    return {
      version,
      blocks: list.decode(state)
    }
  }
}

exports.get = getBlockMap

async function getBlockMap(core, id, opts = {}) {
  if (id.blockLength > 64) {
    throw new Error('Block map is too large')
  }

  const hashes = !!opts.hash

  const map = {
    hashes: hashes ? new Map() : null,
    blocks: null
  }

  const promises = []

  for (let i = id.blockOffset; i < id.blockOffset + id.blockLength; i++) {
    promises.push(core.get(i))
  }

  const buffers = await Promise.all(promises)
  const m = decodeBlockMap(buffers)
  if (!m) return null

  map.blocks = m.blocks

  if (hashes && !core.writable) {
    const blocks = []
    for (let i = 0; i < map.blocks.length; i++) blocks.push(map.blocks[i].index)
    core.download({ blocks })
  }

  if (hashes) {
    for (let i = 0; i < map.blocks.length; i++) {
      const blk = await core.get(map.blocks[i])
      if (blk === null) return null
      map.hashes.set(b4a.toString(crypto.hash(blk), 'hex'), map.blocks[i])
    }
  }

  return map
}

exports.encode = encodeBlockMap

function encodeBlockMap(header) {
  const result = []

  for (let i = 0; i < header.blocks.length; i += 8192) {
    const blocks = i === 0 && header.blocks.length < 8192
      ? header.blocks
      : header.blocks.slice(i, i + 8192)

    const state = { start: 0, end: 0, buffer: null }
    const m = { version: 0, blocks }

    map.preencode(state, m)
    state.buffer = b4a.allocUnsafe(state.end)
    map.encode(state, m)

    result.push(state.buffer)
  }

  return result
}

exports.decode = decodeBlockMap

function decodeBlockMap(buffers) {
  const result = {
    version: 0,
    blocks: null
  }

  for (let i = 0; i < buffers.length; i++) {
    if (!buffers[i]) return null

    const state = { start: 0, end: buffers[i].byteLength, buffer: buffers[i] }
    const r = map.decode(state)

    result.version = r.version

    if (result.blocks) result.blocks.push(...r.blocks)
    else result.blocks = r.blocks
  }

  return result
}
