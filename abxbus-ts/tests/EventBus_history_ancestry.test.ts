import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { z } from 'zod'
import { BaseEvent, EventBus } from '../src/index.js'

test('history pressure preserves pending children and their parent', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'abxbus-ancestry-'))
  const WriteFiles = BaseEvent.extend('WriteFiles', {})
  const WriteFile = BaseEvent.extend('WriteFile', { index: z.number() })
  const bus = new EventBus('HistoryAncestry', { max_history_size: 3, max_history_drop: true })
  const parent = WriteFiles({})
  bus.on(WriteFile, async (event) => {
    assert.equal(bus.event_history.get(parent.event_id), parent)
    assert.equal(bus.event_history.get(event.event_id)?.event_id, event.event_id)
    writeFileSync(join(directory, String(event.index)), String(event.index))
  })
  bus.on(WriteFiles, async (event) => {
    const children = Array.from({ length: 12 }, (_, index) => event.emit(WriteFile({ index })))
    for (const child of children) {
      await child.now()
      await child.eventResult({ raise_if_any: true })
    }
  })
  try {
    await bus.emit(parent).now()
    await parent.eventResult({ raise_if_any: true })
    await bus.waitUntilIdle()
    for (let index = 0; index < 12; index++) assert.equal(readFileSync(join(directory, String(index)), 'utf8'), String(index))
    assert.ok(bus.event_history.size <= 3)
    for (const [, event] of bus.event_history) {
      if (event.event_parent_id) assert.ok(bus.event_history.has(event.event_parent_id))
    }
  } finally {
    await bus.destroy()
    rmSync(directory, { recursive: true })
  }
})

test('history pressure preserves completed parent of pending child', async () => {
  const bus = new EventBus('HistoryAncestry', { max_history_size: 2, max_history_drop: true })
  const Parent = BaseEvent.extend('Parent', {})
  const Child = BaseEvent.extend('HeldChild', {})
  const Extra = BaseEvent.extend('Extra', {})
  let release!: () => void
  const gate = new Promise<void>((resolve) => { release = resolve })
  bus.on(Child, async () => { await gate; return 'ok' })
  try {
    const parent = await bus.emit(Parent({})).now()
    await bus.emit(Extra({})).now()
    const child = bus.emit(Child({ event_parent_id: parent.event_id }))
    const extras = Array.from({ length: 8 }, () => bus.emit(Extra({})))
    assert.equal(bus.event_history.get(parent.event_id)?.event_id, parent.event_id)
    assert.equal(bus.event_history.get(child.event_id)?.event_id, child.event_id)
    release()
    await Promise.all([child.now(), ...extras.map((event) => event.now())])
    await bus.waitUntilIdle()
    assert.equal(await child.eventResult({ raise_if_any: true }), 'ok')
    assert.ok(bus.event_history.size <= 2)
  } finally {
    release()
    await bus.destroy()
  }
})
