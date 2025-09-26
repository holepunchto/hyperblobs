const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('hypercore')

const Hyperblobs = require('..')

async function create (t, opts) {
  const core = new Hypercore(await t.tmp())
  const blobs = new Hyperblobs(core)

  await blobs.ready()

  t.teardown(() => blobs.close(), { order: 1 })

  return blobs
}

test('can get/put a large blob', async t => {
  const blobs = await create(t)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id)

  t.alike(result, buf)
})

test('can put/get two blobs in one core', async t => {
  const blobs = await create(t)

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
  const blobs = await create(t)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, length: 2 })

  t.alike(b4a.toString(result, 'utf-8'), 'cd')
})

test('can seek to start/length within one blob, multiple blocks', async t => {
  const blobs = await create(t, { blockSize: 10 })

  const buf = b4a.concat([b4a.alloc(10, 'a'), b4a.alloc(10, 'b')])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })

  t.is(b4a.toString(result, 'utf-8'), 'aabb')
})

test('can seek to start/length within one blob, multiple blocks, multiple blobs', async t => {
  const blobs = await create(t, { blockSize: 10 })

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
  const blobs = await create(t)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive

  t.is(b4a.toString(result, 'utf-8'), 'cde')
})

test('basic seek', async t => {
  const blobs = await create(t)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)
  const start = blobs.blockSize + 424
  const result = await blobs.get(id, { start })

  t.alike(result, buf.subarray(start))
})

test('can pass in a custom core', async t => {
  const core1 = new Hypercore(await t.tmp())
  const core2 = new Hypercore(await t.tmp())

  t.teardown(async () => {
    await core1.close()
    await core2.close()
  })

  const blobs = new Hyperblobs(core1)
  await core1.ready()

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf, { core: core2 })
  const result = await blobs.get(id, { core: core2 })

  t.alike(result, buf)
  t.is(core1.length, 0)
})

test('two write streams does not deadlock', async t => {
  t.plan(2)

  const blobs = await create(t)

  const ws = blobs.createWriteStream()

  ws.on('open', () => ws.destroy())
  ws.on('drain', () => t.comment('ws drained'))
  ws.on('close', () => t.pass('ws closed'))

  ws.on('close', function () {
    const ws2 = blobs.createWriteStream()
    ws2.write(b4a.from('hello'))
    ws2.end()
    ws2.on('close', () => t.pass('ws2 closed'))
  })
})

test('append error does not deadlock', async t => {
  t.plan(2)

  const core = new Hypercore(await t.tmp())
  const blobs = new Hyperblobs(core)
  await core.ready()

  const ws = blobs.createWriteStream()

  ws.on('open', async function () {
    await core.close()

    ws.write(b4a.from('hello'))
    ws.end()
  })

  ws.on('drain', () => t.comment('ws drained'))
  ws.on('error', (err) => t.comment('ws error: ' + err.message))
  ws.on('close', () => t.pass('ws closed'))

  ws.on('close', async function () {
    const core2 = new Hypercore(await t.tmp())
    t.teardown(() => core2.close())
    const ws2 = blobs.createWriteStream({ core: core2 })
    ws2.write(b4a.from('hello'))
    ws2.end()
    ws2.on('close', () => t.pass('ws2 closed'))
  })
})

test('can put/get a blob and clear it', async t => {
  const blobs = await create(t)

  const buf = b4a.alloc(5 * blobs.blockSize, 'abcdefg')
  const id = await blobs.put(buf)

  t.alike(await blobs.get(id), buf)

  await blobs.clear(id)

  for (let i = 0; i < id.blockLength; i++) {
    const block = id.blockOffset + i
    t.absent(await blobs.core.has(block), `block ${block} cleared`)
  }
})

test('get with timeout', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  try {
    const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }
    await blobs.get(id, { timeout: 1 })
    t.fail('should have failed')
  } catch (error) {
    t.is(error.code, 'REQUEST_TIMEOUT')
  }
})

test('seek with timeout', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  try {
    const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }
    await blobs.get(id, { start: 100, timeout: 1 })
    t.fail('should have failed')
  } catch (error) {
    t.is(error.code, 'REQUEST_TIMEOUT')
  }
})

test('get without waiting', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }
  const blob = await blobs.get(id, { wait: false })
  t.is(blob, null)
})

test('seek without waiting', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }
  const blob = await blobs.get(id, { start: 100, wait: false })
  t.is(blob, null)
})

test('read stream with timeout', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }

  try {
    for await (const block of blobs.createReadStream(id, { timeout: 1 })) {
      t.fail('should not get any block: ' + block.toString())
    }
  } catch (error) {
    t.is(error.code, 'REQUEST_TIMEOUT')
  }
})

test('read stream without waiting', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }

  try {
    for await (const block of blobs.createReadStream(id, { wait: false })) {
      t.fail('should not get any block: ' + block.toString())
    }
  } catch (error) {
    t.is(error.code, 'BLOCK_NOT_AVAILABLE')
  }
})

test('seek stream without waiting', async function (t) {
  t.plan(1)

  const [, b] = await createPair(t)
  const blobs = new Hyperblobs(b)

  const id = { byteOffset: 5, blockOffset: 1, blockLength: 1, byteLength: 5 }

  try {
    for await (const block of blobs.createReadStream(id, { start: 100, wait: false })) {
      t.fail('should not get any block: ' + block.toString())
    }
  } catch (error) {
    t.is(error.code, 'BLOCK_NOT_AVAILABLE')
  }
})

test.skip('clear with diff option', async function (t) {
  t.comment('Hypercore Clear doesnt return correct value')
  t.plan(3)

  const blobs = await create(t)

  const buf = b4a.alloc(128)
  const id = await blobs.put(buf)
  const id2 = await blobs.put(buf)

  const cleared = await blobs.clear(id)
  t.is(cleared, null)

  const cleared2 = await blobs.clear(id2, { diff: true })
  t.ok(cleared2.blocks > 0)

  const cleared3 = await blobs.clear(id2, { diff: true })
  t.is(cleared3.blocks, 0)
})

test('upload/download can be monitored', async (t) => {
  t.plan(30)

  const [a, b] = await createPair(t)
  const blobsA = new Hyperblobs(a)
  const blobsB = new Hyperblobs(b)

  const bytes = 1024 * 100 // big enough to trigger more than one update event
  const buf = Buffer.alloc(bytes, '0')
  const id = await blobsA.put(buf)

  // add another blob which should not be monitored
  const controlId = await blobsA.put(buf)

  {
    const expectedBlocks = [2, 1]
    const expectedBytes = [bytes, 65536]
    const expectedPercentage = [100, 50]

    // Start monitoring upload
    const monitor = blobsA.monitor(id)
    t.teardown(() => monitor.close())
    monitor.on('update', () => {
      t.is(monitor.uploadStats.blocks, expectedBlocks.pop())
      t.is(monitor.uploadStats.monitoringBytes, expectedBytes.pop())
      t.is(monitor.uploadStats.targetBlocks, 2)
      t.is(monitor.uploadStats.targetBytes, bytes)
      t.is(monitor.uploadSpeed(), monitor.uploadStats.speed)
      t.is(monitor.uploadStats.percentage, expectedPercentage.pop())
      t.absent(monitor.downloadStats.blocks)
    })
  }

  {
    // Start monitoring download
    const expectedBlocks = [2, 1]
    const expectedBytes = [bytes, 65536]
    const expectedPercentage = [100, 50]

    const monitor = blobsB.monitor(id)
    t.teardown(() => monitor.close())
    monitor.on('update', () => {
      t.is(monitor.downloadStats.blocks, expectedBlocks.pop())
      t.is(monitor.downloadStats.monitoringBytes, expectedBytes.pop())
      t.is(monitor.downloadStats.targetBlocks, 2)
      t.is(monitor.downloadStats.targetBytes, bytes)
      t.is(monitor.downloadSpeed(), monitor.downloadStats.speed)
      t.is(monitor.downloadStats.percentage, expectedPercentage.pop())
      t.absent(monitor.uploadStats.blocks)
    })
  }

  const res = await blobsB.get(id)
  t.alike(res, buf)

  // should not generate events
  const controRes = await blobsB.get(controlId)
  t.alike(controRes, buf)
})

test('monitor is removed from the Set on close', async (t) => {
  const blobs = await create(t)

  const bytes = 1024 * 100 // big enough to trigger more than one update event
  const buf = Buffer.alloc(bytes, '0')
  const id = await blobs.put(buf)
  const monitor = blobs.monitor(id)
  t.teardown(() => monitor.close())
  t.is(blobs._monitors.size, 1)
  monitor.close()
  t.is(blobs._monitors.size, 0)
})

test('basic batch', async (t) => {
  const blobs = await create(t)
  const batch = blobs.batch()

  {
    const id = await batch.put(Buffer.from('hello world'))
    const buf = await batch.get(id)
    t.alike(buf, Buffer.from('hello world'))
  }

  {
    const id = await batch.put(Buffer.from('hej verden'))
    const buf = await batch.get(id)
    t.alike(buf, Buffer.from('hej verden'))
  }

  await batch.flush()
})

async function createPair (t) {
  const a = new Hypercore(await t.tmp())
  await a.ready()
  t.teardown(() => a.close(), { order: 1 })

  const b = new Hypercore(await t.tmp(), a.key)
  await b.ready()
  t.teardown(() => b.close(), { order: 1 })

  replicate(a, b)

  return [a, b]
}

function replicate (a, b) {
  const s1 = a.replicate(true, { keepAlive: false })
  const s2 = b.replicate(false, { keepAlive: false })
  s1.pipe(s2).pipe(s1)
  return [s1, s2]
}
