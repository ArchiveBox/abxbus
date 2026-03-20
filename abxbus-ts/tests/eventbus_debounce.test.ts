import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})

const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', { target_id: z.string() })

const SyncEvent = BaseEvent.extend('SyncEvent', {})
const TARGET_ID_1 = '9b447756-908c-7b75-8a51-4a2c2b4d9b14'
const TARGET_ID_2 = '194870e1-fa02-70a4-8101-d10d57c3449c'

test('simple debounce uses recent history or dispatches new', async () => {
  const bus = new EventBus('DebounceBus')

  const parent_event = bus.emit(ParentEvent({}))
  await parent_event.done()

  const child_event = parent_event.bus?.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  assert.ok(child_event)
  await child_event.done()

  const reused_event =
    (await bus.find(ScreenshotEvent, {
      past: 10,
      future: false,
      child_of: parent_event,
    })) ?? bus.emit(ScreenshotEvent({ target_id: TARGET_ID_2 }))
  await reused_event.done()

  assert.equal(reused_event.event_id, child_event.event_id)
  assert.equal(reused_event.event_parent_id, parent_event.event_id)
})

test('advanced debounce prefers history, then waits for future, then dispatches', async () => {
  const bus = new EventBus('AdvancedDebounceBus')

  const pending_event = bus.find(SyncEvent, { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(SyncEvent({}))
  }, 50)

  const resolved_event = (await bus.find(SyncEvent, { past: true, future: false })) ?? (await pending_event) ?? bus.emit(SyncEvent({}))
  await resolved_event.done()

  assert.ok(resolved_event)
  assert.equal(resolved_event.event_type, 'SyncEvent')
})

test('debounce returns existing fresh event', async () => {
  const bus = new EventBus('DebounceFreshBus')

  const original = await bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 })).done()

  const is_fresh = (event: typeof original): boolean => {
    const completed_at = event.event_completed_at ? Date.parse(event.event_completed_at) : 0
    return Date.now() - completed_at < 5000
  }

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1 && is_fresh(event), { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.done()

  assert.equal(result.event_id, original.event_id)
})

test('debounce dispatches new when no match', async () => {
  const bus = new EventBus('DebounceNoMatchBus')

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.done()

  assert.ok(result)
  assert.equal(result.target_id, TARGET_ID_1)
  assert.equal(result.event_status, 'completed')
})

test('debounce dispatches new when existing is stale', async () => {
  const bus = new EventBus('DebounceStaleBus')

  await bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 })).done()

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === TARGET_ID_1 && false, { past: true, future: false })) ??
    bus.emit(ScreenshotEvent({ target_id: TARGET_ID_1 }))
  await result.done()

  assert.ok(result)
  const screenshots = Array.from(bus.event_history.values()).filter((event) => event.event_type === 'ScreenshotEvent')
  assert.equal(screenshots.length, 2)
})

test('debounce or-chain handles sequential lookups without blocking', async () => {
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
  await Promise.all([result1.done(), result2.done(), result3.done()])

  assert.equal(result1.event_id, result2.event_id)
  assert.notEqual(result1.event_id, result3.event_id)
  assert.equal(result3.target_id, TARGET_ID_2)
})

test('debounce past-only and past-window lookups return immediately when empty', async () => {
  const bus = new EventBus('DebounceImmediateLookupBus')

  const past_start = Date.now()
  const found_past = await bus.find(ParentEvent, { past: true, future: false })
  const past_elapsed_ms = Date.now() - past_start

  const window_start = Date.now()
  const found_window = await bus.find(ParentEvent, { past: 5, future: false })
  const window_elapsed_ms = Date.now() - window_start

  assert.equal(found_past, null)
  assert.equal(found_window, null)
  assert.ok(past_elapsed_ms < 100)
  assert.ok(window_elapsed_ms < 100)
})
