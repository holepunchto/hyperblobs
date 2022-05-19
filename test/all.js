const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const Hyperblobs = require('..')

test('can get/put a large blob', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id)
  t.true(result.equals(buf))

  t.end()
})

test('can put/get two blobs in one core', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core)

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('hijklmn')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  t.end()
})

test('can seek to start/length within one blob, one block', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, length: 2 })
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test('can seek to start/length within one blob, multiple blocks', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core, { blockSize: 10 })

  const buf = Buffer.concat([Buffer.alloc(10).fill('a'), Buffer.alloc(10).fill('b')])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })
  t.true(result.toString('utf-8'), 'aabb')

  t.end()
})

test('can seek to start/length within one blob, multiple blocks, multiple blobs', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core, { blockSize: 10 })

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  const buf = Buffer.concat([Buffer.alloc(10).fill('a'), Buffer.alloc(10).fill('b')])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })
  t.true(result.toString('utf-8'), 'aabb')

  t.end()
})

test('can seek to start/end within one blob', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test('basic seek', async t => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobs(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test('can pass in a custom core', async t => {
  const core1 = new Hypercore(ram)
  const core2 = new Hypercore(ram)
  const blobs = new Hyperblobs(core1)
  await core1.ready()

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf, { core: core2 })
  const result = await blobs.get(id, { core: core2 })
  t.true(result.equals(buf))

  t.same(core1.length, 0)

  t.end()
})
