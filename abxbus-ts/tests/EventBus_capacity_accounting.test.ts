import assert from 'node:assert/strict'
import { test } from 'node:test'
import { BaseEvent, EventBus } from '../src/index.js'

for (const limit of [50, 100, 150]) {
  test(`capacity does not double count queued events (limit=${limit})`, async () => {
    const bus = new EventBus('CapacityAccounting', { max_history_size: limit })
    const CapacityEvent = BaseEvent.extend('CapacityEvent', {})
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    bus.on(CapacityEvent, async () => {
      await gate
    })
    try {
      const events = Array.from({ length: limit }, () => bus.emit(CapacityEvent({})))
      assert.equal(bus.event_history.size, limit)
      const rejected = CapacityEvent({})
      assert.throws(() => bus.emit(rejected), new RegExp(`history limit reached \\(${limit}/${limit}\\)`))
      assert.equal(bus.event_history.has(rejected.event_id), false)
      assert.equal(bus.event_history.size, limit)
      release()
      assert.equal(await bus.waitUntilIdle(5), true)
      assert.equal(
        events.every((event) => event.event_status === 'completed'),
        true
      )
    } finally {
      release()
      await bus.destroy()
    }
  })
}
