const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('hypercore')
const RAM = require('random-access-memory')

const Hyperblobs = require('..')

test('can get/put a large blob', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id)

  t.alike(result, buf)
})

test('can put/get two blobs in one core', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core)

  {
    const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)

    t.alike(res, buf)
  }

  {
    const buf = b4a.alloc(5 * blobs.blockSize, 'hijklmn')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)

    t.alike(res, buf)
  }
})

test('can seek to start/length within one blob, one block', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, length: 2 })

  t.alike(b4a.toString(result, 'utf-8'), 'cd')
})

test('can seek to start/length within one blob, multiple blocks', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core, { blockSize: 10 })

  const buf = b4a.concat([b4a.alloc(10, 'a'), b4a.alloc(10, 'b')])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })

  t.is(b4a.toString(result, 'utf-8'), 'aabb')
})

test('can seek to start/length within one blob, multiple blocks, multiple blobs', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core, { blockSize: 10 })

  {
    const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)

    t.alike(res, buf)
  }

  const buf = b4a.concat([b4a.alloc(10, 'a'), b4a.alloc(10, 'b')])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })

  t.is(b4a.toString(result, 'utf-8'), 'aabb')
})

test('can seek to start/end within one blob', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive

  t.is(b4a.toString(result, 'utf-8'), 'cde')
})

test('basic seek', async t => {
  const core = new Hypercore(RAM)
  const blobs = new Hyperblobs(core)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const start = blobs.blockSize + 424
  const result = await blobs.get(id, { start })

  t.alike(result, buf.subarray(start))
})

test('can pass in a custom core', async t => {
  const core1 = new Hypercore(RAM)
  const core2 = new Hypercore(RAM)
  const blobs = new Hyperblobs(core1)
  await core1.ready()

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf, { core: core2 })
  const result = await blobs.get(id, { core: core2 })

  t.alike(result, buf)
  t.is(core1.length, 0)
})
