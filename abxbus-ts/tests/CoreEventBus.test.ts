import assert from 'node:assert/strict'
import { test } from 'node:test'

import { RustCoreEventBus } from '../src/CoreEventBus.js'

test('RustCoreEventBus runs TS handler via core snapshot', async () => {
  const bus = new RustCoreEventBus('TsCoreBus')
  try {
    await bus.start()
    await bus.on('BaseEvent', (event) => Number((event as { value?: unknown }).value) + 1)
    const completed = await bus.emit({ event_type: 'BaseEvent', value: 41 })

    assert.equal(completed.type, 'event_completed')
    assert.equal(typeof completed.event_id, 'string')
  } finally {
    bus.close()
  }
})

test('RustCoreEventBus commits TS handler error through core', async () => {
  const bus = new RustCoreEventBus('TsCoreErrorBus')
  try {
    await bus.start()
    await bus.on('BaseEventError', () => {
      throw new Error('ts boom')
    })
    const completed = await bus.emit({ event_type: 'BaseEventError', value: 41 })

    assert.equal(completed.type, 'event_completed')
  } finally {
    bus.close()
  }
})
