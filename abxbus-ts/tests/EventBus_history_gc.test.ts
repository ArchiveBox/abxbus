import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { BaseEvent, EventBus } from '../src/index.js'

for (const ttl of [true, false]) {
  test(`expired child events release parent references (${ttl ? 'TTL' : 'capacity'})`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'abxbus-ttl-'))
    const bus = new EventBus('TTLReferenceRelease', { max_history_size: ttl ? null : 3, event_ttl: ttl ? 0 : null, max_history_drop: true })
    const references: WeakRef<BaseEvent>[] = []
    const eventIds: string[] = []
    bus.on('SaveFile', async (event: BaseEvent) => {
      await writeFile(join(directory, String(references.length)), 'saved')
      references.push(new WeakRef(event._event_original ?? event))
      eventIds.push(event.event_id)
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
      const expired = references.filter((_reference, index) => !bus.event_history.has(eventIds[index]!))
      assert.ok(expired.length >= 8)
      if (ttl) assert.equal(expired.length, 10)
      assert.ok(expired.every((reference) => reference.deref() === undefined))
      for (let index = 0; index < 10; index++) assert.equal(await readFile(join(directory, String(index)), 'utf8'), 'saved')
    } finally {
      await bus.destroy()
      await rm(directory, { recursive: true })
    }
  })
}
