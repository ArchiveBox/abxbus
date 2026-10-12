import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})

const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', { target_id: z.string() })

const SyncEvent = BaseEvent.extend('SyncEvent', {})
const TARGET_ID_1 = '9b447756-908c-7b75-8a51-4a2c2b4d9b14'
const TARGET_ID_2 = '194870e1-fa02-70a4-8101-d10d57c3449c'

test('simple debounce with child_of reuses recent event', async () => {
  const bus = new EventBus('DebounceBus')

  let child_event: BaseEvent | undefined

  bus.on(ParentEvent, async (event) => {
    child_event = event.emit(ScreenshotEvent({ target_id: '0c1ccf21-65c0-7390-8b64-9182e985740e' }))
    await child_event.now()
    return 'parent_done'
  })
  bus.on(ScreenshotEvent, () => 'screenshot_done')

  const parent_event = bus.emit(ParentEvent({}))
  await parent_event.now()
  await bus.waitUntilIdle()

  const reused_event =
    (await bus.find(ScreenshotEvent, {
      past: 10,
      future: false,
      child_of: parent_event,
    })) ?? bus.emit(ScreenshotEvent({ target_id: 'd41abf01-392b-7f28-8992-3105a258867d' }))
  await reused_event.now()

  assert.ok(child_event)
  assert.equal(reused_event.event_id, child_event.event_id)
  assert.equal(reused_event.event_parent_id, parent_event.event_id)
})

test('returns existing fresh event', async () => {
  const bus = new EventBus('DebounceFreshBus')

  const original = await bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 })).now()

  const is_fresh = (event: typeof original): boolean => {
    const completed_at = event.event_completed_at ? Date.parse(event.event_completed_at) : 0
    return Date.now() - completed_at < 5000
  }

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1 && is_fresh(event), { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.now()

  assert.equal(result.event_id, original.event_id)
})

test('advanced debounce prefers history then waits future then dispatches', async () => {
  const bus = new EventBus('AdvancedDebounceBus')

  const pending_event = bus.find(SyncEvent, { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(SyncEvent({}))
  }, 50)

  const resolved_event = (await bus.find(SyncEvent, { past: true, future: false })) ?? (await pending_event) ?? bus.emit(SyncEvent({}))
  await resolved_event.now()

  assert.ok(resolved_event)
  assert.equal(resolved_event.event_type, 'SyncEvent')
})

test('dispatches new when no match', async () => {
  const bus = new EventBus('DebounceNoMatchBus')

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.now()

  assert.ok(result)
  assert.equal(result.target_id, TARGET_ID_1)
  assert.equal(result.event_status, 'completed')
})

test('dispatches new when stale', async () => {
  const bus = new EventBus('DebounceStaleBus')

  await bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 })).now()

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1 && false, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.now()

  assert.ok(result)
  const screenshots = Array.from(bus.event_history.values()).filter((event) => event.event_type === 'ScreenshotEvent')
  assert.equal(screenshots.length, 2)
})

test('find past only returns immediately without waiting', async () => {
  const bus = new EventBus('DebouncePastOnlyBus')

  const result = await bus.find(ParentEvent, { past: true, future: false })

  assert.equal(result, null)
  assert.equal(bus.find_waiters.size, 0)
})

test('find past float returns immediately without waiting', async () => {
  const bus = new EventBus('DebouncePastWindowBus')

  const result = await bus.find(ParentEvent, { past: 5, future: false })

  assert.equal(result, null)
  assert.equal(bus.find_waiters.size, 0)
})

test('or chain without waiting finds existing', async () => {
  const bus = new EventBus('DebounceOrChainExistingBus')

  const original = await bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 })).now()

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.now()

  assert.equal(result.event_id, original.event_id)
  assert.equal(bus.find_waiters.size, 0)
})

test('or chain without waiting dispatches when no match', async () => {
  const bus = new EventBus('DebounceOrChainNoMatchBus')

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.now()

  assert.ok(result)
  assert.equal(result.target_id, TARGET_ID_1)
  assert.equal(bus.find_waiters.size, 0)
})

test('or chain multiple sequential lookups', async () => {
  const bus = new EventBus('DebounceSequentialBus')

  const result1 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))

  const result2 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))

  const result3 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_2, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_2 }))
  await Promise.all([result1.now(), result2.now(), result3.now()])

  assert.equal(result1.event_id, result2.event_id)
  assert.notEqual(result1.event_id, result3.event_id)
  assert.equal(result3.target_id, TARGET_ID_2)
  assert.equal(bus.find_waiters.size, 0)
})
