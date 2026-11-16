import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

// JS workers own separate heaps/registries; within a heap, active awaiting must
// likewise leave unrelated queued events to their own bus runner.
test('now does not drain unrelated bus', { timeout: 2000 }, async () => {
  const Blocker = BaseEvent.extend('Blocker', {})
  const Probe = BaseEvent.extend('Probe', {})
  const Own = BaseEvent.extend('Own', {})
  const bus = new EventBus('AwaitingBus')
  const other = new EventBus('UnrelatedBus')
  let started!: () => void
  const ready = new Promise<void>((resolve) => {
    started = resolve
  })
  let release!: () => void
  const blocked = new Promise<void>((resolve) => {
    release = resolve
  })
  const seen: string[] = []
  const ownSeen: string[] = []
  bus.on(Own, () => {
    ownSeen.push('own')
  })
  other.on(Blocker, async () => {
    started()
    await blocked
  })
  other.on(Probe, () => {
    seen.push('probe')
  })
  try {
    other.emit(Blocker({}))
    const queued = other.emit(Probe({}))
    await ready
    await bus.emit(Own({})).now()
    assert.deepEqual(ownSeen, ['own'])
    assert.deepEqual(seen, [])
    release()
    await queued.wait()
    assert.deepEqual(seen, ['probe'])
  } finally {
    release()
    await bus.destroy()
    await other.destroy()
  }
})
