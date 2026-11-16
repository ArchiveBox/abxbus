import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

for (const clear of [false, true]) {
  test(`destroy(clear=${clear}) releases pending idle waits and stays terminal`, { timeout: 2000 }, async () => {
    const ShutdownEvent = BaseEvent.extend('ShutdownEvent', {})
    const bus = new EventBus()
    let started!: () => void
    const handlerStarted = new Promise<void>((resolve) => {
      started = resolve
    })
    let release!: () => void
    const handlerRelease = new Promise<void>((resolve) => {
      release = resolve
    })
    bus.on(ShutdownEvent, async () => {
      started()
      await handlerRelease
    })
    bus.emit(ShutdownEvent({}))
    bus.emit(ShutdownEvent({}))
    await handlerStarted
    const waiting = bus.waitUntilIdle()
    let deadline: ReturnType<typeof setTimeout> | undefined
    try {
      await bus.destroy({ clear })
      const timedOut = new Promise<never>((_, reject) => {
        deadline = setTimeout(() => reject(new Error('destroy stranded the idle waiter')), 500)
      })
      assert.equal(await Promise.race([waiting, timedOut]), true)
      assert.throws(() => bus.emit(ShutdownEvent({})), /destroyed/)
      await assert.rejects(() => bus.waitUntilIdle(), /destroyed/)
      assert.equal(bus.pending_event_queue.length, 0)
    } finally {
      clearTimeout(deadline)
      release()
      await bus.destroy()
    }
  })
}
