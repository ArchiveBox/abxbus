import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { BaseEvent, EventBus } from '../src/index.js'

test('expired child events release parent references while the parent remains retained', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'abxbus-ttl-'))
  const bus = new EventBus('TTLReferenceRelease', { max_history_size: null, event_ttl: 0 })
  const references: WeakRef<BaseEvent>[] = []
  bus.on('SaveFile', async (event: BaseEvent) => {
    await writeFile(join(directory, String(references.length)), 'saved')
    references.push(new WeakRef(event._event_original ?? event))
  })
  bus.on('SaveFiles', async (event: BaseEvent) => {
    for (let index = 0; index < 10; index++) {
      await event.emit(new BaseEvent({ event_type: 'SaveFile' })).now()
    }
  })
  const parent = new BaseEvent({ event_type: 'SaveFiles', event_ttl: -1 })
  try {
    await bus.emit(parent).now()
    await bus.waitUntilIdle()
    await bus.emit(new BaseEvent({ event_type: 'Trim' })).now()
    await bus.waitUntilIdle()
    await new Promise((resolve) => setImmediate(resolve))
    assert.ok(global.gc)
    global.gc()
    assert.equal(bus.event_history.get(parent.event_id), parent)
    assert.equal(references.length, 10)
    assert.ok(references.every((reference) => reference.deref() === undefined))
    for (let index = 0; index < 10; index++) assert.equal(await readFile(join(directory, String(index)), 'utf8'), 'saved')
  } finally {
    await bus.destroy()
    await rm(directory, { recursive: true })
  }
})
