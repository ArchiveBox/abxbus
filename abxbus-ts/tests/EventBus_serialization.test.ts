import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('EventBus toJSON/fromJSON roundtrip uses id-keyed structures', async () => {
  const bus = new EventBus('SerializableBus', {
    id: '018f8e40-1234-7000-8000-000000001234',
    max_history_size: 500,
    max_history_drop: false,
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
    event_timeout: null,
    event_handler_slow_timeout: 12,
    event_slow_timeout: 34,
    event_handler_detect_file_paths: false,
  })
  const SerializableEvent = BaseEvent.extend('SerializableEvent', {})

  bus.on(SerializableEvent, async () => {
    await delay(20)
    return 'ok'
  })

  const release_pause = bus.locks._requestRunloopPause()
  const pending_event = bus.emit(SerializableEvent({ event_timeout: 11 }))
  await Promise.resolve()

  const json = bus.toJSON()
  assert.equal(json.id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(json.name, 'SerializableBus')
  assert.equal(Object.keys(json.handlers).length, 1)
  assert.equal(Object.keys(json.handlers_by_key).length, 1)
  assert.equal(Array.isArray(json.handlers_by_key.SerializableEvent), true)
  assert.equal(Object.keys(json.event_history).length, 1)
  assert.equal((json.event_history[pending_event.event_id] as Record<string, unknown>).event_id, pending_event.event_id)
  assert.equal(json.pending_event_queue.length, 1)
  assert.equal(json.pending_event_queue[0], pending_event.event_id)

  const restored = EventBus.fromJSON(json)
  assert.equal(restored.id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(restored.name, 'SerializableBus')
  assert.equal(restored.event_history.max_history_size, 500)
  assert.equal(restored.event_history.max_history_drop, false)
  assert.equal(restored.event_concurrency, 'parallel')
  assert.equal(restored.event_handler_concurrency, 'parallel')
  assert.equal(restored.event_handler_completion, 'first')
  assert.equal(restored.event_timeout, null)
  assert.equal(restored.event_handler_slow_timeout, 12)
  assert.equal(restored.event_slow_timeout, 34)
  assert.equal(restored.event_handler_detect_file_paths, false)
  assert.equal(restored.handlers.size, 1)
  assert.equal(restored.handlers_by_key.get('SerializableEvent')?.length, 1)
  assert.equal(restored.event_history.size, 1)
  assert.equal(restored.pending_event_queue.length, 1)
  assert.equal(restored.pending_event_queue[0].event_id, pending_event.event_id)
  assert.equal(restored.runloop_running, false)

  release_pause()
  await pending_event.done()
})

test('EventBus preserves handler registration order through JSON and restore', async () => {
  const HandlerOrderEvent = BaseEvent.extend('HandlerOrderEvent', {})
  const bus = new EventBus('HandlerOrderSourceBus', {
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
    event_handler_detect_file_paths: false,
  })
  const original_order: string[] = []

  const first = bus.on(HandlerOrderEvent, () => {
    original_order.push('first')
    return 'first'
  })
  const second = bus.on(HandlerOrderEvent, () => {
    original_order.push('second')
    return 'second'
  })
  const expected_ids = [first.id, second.id]

  const json = bus.toJSON()
  assert.deepEqual(Object.keys(json.handlers), expected_ids)
  assert.deepEqual(json.handlers_by_key.HandlerOrderEvent, expected_ids)

  await bus.emit(HandlerOrderEvent({})).done()
  assert.deepEqual(original_order, ['first', 'second'])

  const restored = EventBus.fromJSON(json)
  const restored_json = restored.toJSON()
  assert.deepEqual(Array.from(restored.handlers.keys()), expected_ids)
  assert.deepEqual(restored.handlers_by_key.get('HandlerOrderEvent'), expected_ids)
  assert.deepEqual(Object.keys(restored_json.handlers), expected_ids)
  assert.deepEqual(restored_json.handlers_by_key.HandlerOrderEvent, expected_ids)

  const restored_order: string[] = []
  restored.handlers.get(first.id)!.handler = () => {
    restored_order.push('first')
    return 'first'
  }
  restored.handlers.get(second.id)!.handler = () => {
    restored_order.push('second')
    return 'second'
  }

  await restored.emit(HandlerOrderEvent({})).done()
  assert.deepEqual(restored_order, ['first', 'second'])

  bus.destroy()
  restored.destroy()
})

test('EventBus.fromJSON recreates missing handler entries from event_result metadata', async () => {
  const bus = new EventBus('MissingHandlerHydrationBus', {
    event_handler_detect_file_paths: false,
  })
  const SerializableEvent = BaseEvent.extend('MissingHandlerHydrationEvent', {})

  bus.on(SerializableEvent, () => 'ok')
  const event = bus.emit(SerializableEvent({}))
  await event.done()

  const handler_id = Array.from(event.event_results.values())[0].handler_id
  const json = bus.toJSON()
  json.handlers = {}
  json.handlers_by_key = {}

  const restored = EventBus.fromJSON(json)
  const restored_event = restored.event_history.get(event.event_id)
  assert.ok(restored_event)
  assert.ok(restored.handlers.has(handler_id))
  const restored_result = restored_event!.event_results.get(handler_id)
  assert.ok(restored_result)
  assert.equal(restored_result!.handler, restored.handlers.get(handler_id))
  assert.equal(typeof restored_result!.handler.handler, 'function')
  assert.equal(await restored_result!.handler.handler(restored_event as BaseEvent), undefined)
})

test('EventBus toJSON promotes pending events into event_history snapshot', async () => {
  const bus = new EventBus('ModelDumpPendingBus')
  const PendingEvent = BaseEvent.extend('ModelDumpPendingEvent', {})

  bus.on(PendingEvent, async () => {
    await delay(10)
    return 'ok'
  })

  const release_pause = bus.locks._requestRunloopPause()
  const pending = bus.emit(PendingEvent({}))
  await Promise.resolve()

  const json = bus.toJSON()
  assert.equal(Boolean(json.event_history[pending.event_id]), true)
  assert.equal(json.pending_event_queue.includes(pending.event_id), true)

  release_pause()
  await pending.done()
})
