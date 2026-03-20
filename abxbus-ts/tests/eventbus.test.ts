import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'
import { GlobalEventBusRegistry } from '../src/event_bus.js'
import { AsyncLock } from '../src/lock_manager.js'
import { z } from 'zod'

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

// ─── Constructor defaults ────────────────────────────────────────────────────

test('EventBus initializes with correct defaults', async () => {
  const bus = new EventBus('DefaultsBus')

  assert.equal(bus.name, 'DefaultsBus')
  assert.equal(bus.event_history.max_history_size, 100)
  assert.equal(bus.event_history.max_history_drop, false)
  assert.equal(bus.event_concurrency, 'bus-serial')
  assert.equal(bus.event_handler_concurrency, 'serial')
  assert.equal(bus.event_handler_completion, 'all')
  assert.equal(bus.event_timeout, 60)
  assert.equal(bus.event_history.size, 0)
  assert.ok(EventBus.all_instances.has(bus))
  await bus.waitUntilIdle()
})

test('waitUntilIdle(timeout) returns after timeout when work is still in-flight', async () => {
  const WaitForIdleTimeoutEvent = BaseEvent.extend('WaitForIdleTimeoutEvent', {})
  const bus = new EventBus('WaitForIdleTimeoutBus')

  let release_handler!: () => void
  const handler_gate = new Promise<void>((resolve) => {
    release_handler = resolve
  })

  bus.on(WaitForIdleTimeoutEvent, async () => {
    await handler_gate
  })

  bus.emit(WaitForIdleTimeoutEvent({}))

  const start_ms = performance.now()
  const became_idle = await bus.waitUntilIdle(0.05)
  const elapsed_ms = performance.now() - start_ms

  try {
    assert.ok(elapsed_ms >= 30, `expected timeout wait to be >=30ms, got ${elapsed_ms}ms`)
    assert.ok(elapsed_ms < 1000, `expected timeout wait to be <1000ms, got ${elapsed_ms}ms`)
    assert.equal(became_idle, false)
    assert.equal(bus.isIdleAndQueueEmpty(), false)
  } finally {
    release_handler()
    assert.equal(await bus.waitUntilIdle(), true)
  }
})

test('EventBus applies custom options', () => {
  const bus = new EventBus('CustomBus', {
    max_history_size: 500,
    max_history_drop: false,
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
    event_handler_completion: 'first',
    event_timeout: 30,
  })

  assert.equal(bus.event_history.max_history_size, 500)
  assert.equal(bus.event_history.max_history_drop, false)
  assert.equal(bus.event_concurrency, 'parallel')
  assert.equal(bus.event_handler_concurrency, 'serial')
  assert.equal(bus.event_handler_completion, 'first')
  assert.equal(bus.event_timeout, 30)
})

test('EventBus with null max_history_size means unlimited', () => {
  const bus = new EventBus('UnlimitedBus', { max_history_size: null })
  assert.equal(bus.event_history.max_history_size, null)
})

test('EventBus with null event_timeout disables timeouts', () => {
  const bus = new EventBus('NoTimeoutBus', { event_timeout: null })
  assert.equal(bus.event_timeout, null)
})

test('EventBus auto-generates name when not provided', () => {
  const bus = new EventBus()
  assert.equal(bus.name, 'EventBus')
})

test('EventBus exposes locks API surface', () => {
  const bus = new EventBus('GateSurfaceBus')
  const locks = bus.locks as unknown as Record<string, unknown>

  assert.equal(typeof locks._requestRunloopPause, 'function')
  assert.equal(typeof locks._waitUntilRunloopResumed, 'function')
  assert.equal(typeof locks._isPaused, 'function')
  assert.equal(typeof locks.waitForIdle, 'function')
  assert.equal(typeof locks._notifyIdleListeners, 'function')
  assert.equal(typeof locks.getLockForEvent, 'function')
})

test('EventBus locks methods are callable and preserve lock resolution behavior', async () => {
  const bus = new EventBus('GateInvocationBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const GateEvent = BaseEvent.extend('GateInvocationEvent', {})

  const release_pause = bus.locks._requestRunloopPause()
  assert.equal(bus.locks._isPaused(), true)

  let resumed = false
  const resumed_promise = bus.locks._waitUntilRunloopResumed().then(() => {
    resumed = true
  })
  await Promise.resolve()
  assert.equal(resumed, false)

  release_pause()
  await resumed_promise
  assert.equal(bus.locks._isPaused(), false)

  const event_with_global = GateEvent({
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'serial',
  })
  assert.equal(bus.locks.getLockForEvent(event_with_global), bus._lock_for_event_global_serial)
  const handler_lock = event_with_global._getHandlerLock(bus.event_handler_concurrency)
  assert.ok(handler_lock)

  const event_with_parallel = GateEvent({
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  assert.equal(bus.locks.getLockForEvent(event_with_parallel), null)
  assert.equal(event_with_parallel._getHandlerLock(bus.event_handler_concurrency), null)

  const another_serial_event = GateEvent({ event_handler_concurrency: 'serial' })
  const another_lock = another_serial_event._getHandlerLock(bus.event_handler_concurrency)
  assert.notEqual(handler_lock, another_lock)

  bus.emit(GateEvent({}))
  bus.locks._notifyIdleListeners()
  await bus.locks.waitForIdle()
})

test('BaseEvent lifecycle methods are callable and preserve lifecycle behavior', async () => {
  const LifecycleEvent = BaseEvent.extend('LifecycleMethodInvocationEvent', {})

  const standalone = LifecycleEvent({})
  standalone._markStarted()
  assert.equal(standalone.event_status, 'started')
  standalone._markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  await standalone.eventCompleted()

  const bus = new EventBus('LifecycleMethodInvocationBus')
  const dispatched = bus.emit(LifecycleEvent({}))
  await dispatched.eventCompleted()
  assert.equal(dispatched.event_status, 'completed')
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('RuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('RuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.emit(RuntimeEvent({}))
  await event.done({ raise_if_any: false })

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_at, 'string')
  assert.equal(typeof json.event_started_at, 'string')
  assert.equal(typeof json.event_completed_at, 'string')
  assert.equal(json.event_pending_bus_count, 0)
  assert.ok(Array.isArray(json.event_results))
  const json_results = json.event_results as Array<Record<string, unknown>>
  assert.equal(json_results.length, 1)
  assert.equal(json_results[0].status, 'completed')
  assert.equal(json_results[0].result, 'ok')
  assert.equal(json_results[0].handler_id, Array.from(event.event_results.values())[0].handler_id)

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_at, event.event_created_at)
  assert.equal(restored.event_pending_bus_count, 0)
  assert.equal(restored.event_results.size, 1)
  const restored_result = Array.from(restored.event_results.values())[0]
  assert.equal(restored_result.status, 'completed')
  assert.equal(restored_result.result, 'ok')
})

test('event_version supports defaults, extend-time defaults, runtime override, and JSON roundtrip', () => {
  const DefaultEvent = BaseEvent.extend('DefaultVersionEvent', {})
  const ExtendVersionEvent = BaseEvent.extend('ExtendVersionEvent', { event_version: '1.2.3' })

  class StaticVersionEvent extends BaseEvent {
    static event_type = 'StaticVersionEvent'
    static event_version = '4.5.6'
  }

  const default_event = DefaultEvent({})
  assert.equal(default_event.event_version, '0.0.1')

  const extended_default = ExtendVersionEvent({})
  assert.equal(extended_default.event_version, '1.2.3')

  const static_default = new StaticVersionEvent({})
  assert.equal(static_default.event_version, '4.5.6')

  const runtime_override = ExtendVersionEvent({ event_version: '9.9.9' })
  assert.equal(runtime_override.event_version, '9.9.9')

  const restored = BaseEvent.fromJSON(runtime_override.toJSON())
  assert.equal(restored.event_version, '9.9.9')
})

test('fromJSON accepts event_parent_id: null and preserves it in toJSON output', () => {
  const missing_field_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001233',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'MissingParentIdEvent',
    event_timeout: null,
  })
  assert.equal(missing_field_event.event_parent_id, null)

  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001234',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullParentIdEvent',
    event_parent_id: null,
    event_timeout: null,
  })

  assert.equal(event.event_parent_id, null)
  assert.equal((event.toJSON() as Record<string, unknown>).event_parent_id, null)
})

test('event_emitted_by_handler_id defaults to null and accepts null in fromJSON', () => {
  const fresh_event = BaseEvent.extend('NullEmittedByDefaultEvent')({})
  assert.equal(fresh_event.event_emitted_by_handler_id, null)

  const missing_field_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001239',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'MissingEmittedByIdEvent',
    event_timeout: null,
  })
  assert.equal(missing_field_event.event_emitted_by_handler_id, null)

  const json_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullEmittedByIdEvent',
    event_emitted_by_handler_id: null,
    event_timeout: null,
  })

  assert.equal(json_event.event_emitted_by_handler_id, null)
  assert.equal((json_event.toJSON() as Record<string, unknown>).event_emitted_by_handler_id, null)
})

test('fromJSON deserializes event_result_type and toJSON reserializes schema', () => {
  const raw_schema = { type: 'integer' }
  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001235',
    event_created_at: new Date('2025-01-01T00:00:01.000Z').toISOString(),
    event_type: 'RawSchemaEvent',
    event_timeout: null,
    event_result_type: raw_schema,
  })
  const json = event.toJSON() as Record<string, unknown>
  assert.equal(typeof (event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof json.event_result_type, 'object')
  assert.ok(['integer', 'number'].includes(String((json.event_result_type as { type?: unknown }).type)))
})

test('fromJSON reconstructs integer and null schemas for runtime validation', async () => {
  const bus = new EventBus('SchemaPrimitiveRuntimeBus')

  const int_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001236',
    event_created_at: new Date('2025-01-01T00:00:02.000Z').toISOString(),
    event_type: 'RawIntegerEvent',
    event_timeout: null,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEvent', () => 123)
  await bus.emit(int_event).done()
  const int_result = Array.from(int_event.event_results.values())[0]
  assert.equal(int_result.status, 'completed')

  const int_bad_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001237',
    event_created_at: new Date('2025-01-01T00:00:03.000Z').toISOString(),
    event_type: 'RawIntegerEventBad',
    event_timeout: null,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEventBad', () => 1.5)
  await bus.emit(int_bad_event).done({ raise_if_any: false })
  const int_bad_result = Array.from(int_bad_event.event_results.values())[0]
  assert.equal(int_bad_result.status, 'error')

  const null_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001238',
    event_created_at: new Date('2025-01-01T00:00:04.000Z').toISOString(),
    event_type: 'RawNullEvent',
    event_timeout: null,
    event_result_type: { type: 'null' },
  })
  bus.on('RawNullEvent', () => null)
  await bus.emit(null_event).done()
  const null_result = Array.from(null_event.event_results.values())[0]
  assert.equal(null_result.status, 'completed')

  await bus.waitUntilIdle()
})

test('fromJSON reconstructs nested object/array result schemas', async () => {
  const bus = new EventBus('SchemaNestedRuntimeBus')
  const raw_nested_schema = {
    type: 'object',
    properties: {
      items: { type: 'array', items: { type: 'integer' } },
      meta: { type: 'object', additionalProperties: { type: 'boolean' } },
    },
    required: ['items', 'meta'],
  }

  const valid_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001239',
    event_created_at: new Date('2025-01-01T00:00:05.000Z').toISOString(),
    event_type: 'RawNestedSchemaEvent',
    event_timeout: null,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEvent', () => ({ items: [1, 2, 3], meta: { ok: true } }))
  await bus.emit(valid_event).done()
  const valid_result = Array.from(valid_event.event_results.values())[0]
  assert.equal(valid_result.status, 'completed')

  const invalid_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001240',
    event_created_at: new Date('2025-01-01T00:00:06.000Z').toISOString(),
    event_type: 'RawNestedSchemaEventBad',
    event_timeout: null,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEventBad', () => ({ items: ['bad'], meta: { ok: 'yes' } }))
  await bus.emit(invalid_event).done({ raise_if_any: false })
  const invalid_result = Array.from(invalid_event.event_results.values())[0]
  assert.equal(invalid_result.status, 'error')

  await bus.waitUntilIdle()
})

// ─── Event dispatch and status lifecycle ─────────────────────────────────────

test('dispatch returns pending event with correct initial state', async () => {
  const bus = new EventBus('LifecycleBus', { max_history_size: 100 })
  const TestEvent = BaseEvent.extend('TestEvent', { data: z.string() })

  const event = bus.emit(TestEvent({ data: 'hello' }))

  // Immediate state after dispatch (before any microtask runs)
  assert.equal(event.event_type, 'TestEvent')
  assert.ok(event.event_id)
  assert.ok(event.event_created_at)
  assert.equal((event as any).data, 'hello')

  // event_path should include the bus label
  const original = event._event_original ?? event
  assert.ok(original.event_path.includes(bus.label))

  await bus.waitUntilIdle()
})

test('event transitions through pending -> started -> completed', async () => {
  const bus = new EventBus('StatusBus', { max_history_size: 100 })
  const TestEvent = BaseEvent.extend('TestEvent', {})
  let status_during_handler: string | undefined

  bus.on(TestEvent, (event: BaseEvent) => {
    status_during_handler = event.event_status
    return 'done'
  })

  const event = bus.emit(TestEvent({}))
  const original = event._event_original ?? event

  await event.done({ raise_if_any: false })

  assert.equal(status_during_handler, 'started')
  assert.equal(original.event_status, 'completed')
  assert.ok(original.event_started_at, 'event_started_at should be set')
  assert.ok(original.event_completed_at, 'event_completed_at should be set')
})

test('event with no handlers completes immediately', async () => {
  const bus = new EventBus('NoHandlerBus', { max_history_size: 100 })
  const OrphanEvent = BaseEvent.extend('OrphanEvent', {})

  const event = bus.emit(OrphanEvent({}))
  await event.done({ raise_if_any: false })

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')
  assert.equal(original.event_results.size, 0)
})

// ─── Event history tracking ──────────────────────────────────────────────────

test('dispatched events appear in event_history', async () => {
  const bus = new EventBus('HistoryBus', { max_history_size: 100 })
  const EventA = BaseEvent.extend('EventA', {})
  const EventB = BaseEvent.extend('EventB', {})

  bus.emit(EventA({}))
  bus.emit(EventB({}))
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 2)
  const history = Array.from(bus.event_history.values())
  assert.equal(history[0].event_type, 'EventA')
  assert.equal(history[1].event_type, 'EventB')

  // All events are accessible by id
  for (const event of bus.event_history.values()) {
    assert.ok(bus.event_history.has(event.event_id))
  }
})

// ─── History trimming (max_history_size) ─────────────────────────────────────

test('history is trimmed to max_history_size, completed events removed first', async () => {
  const bus = new EventBus('TrimBus', { max_history_size: 5, max_history_drop: true })
  const TrimEvent = BaseEvent.extend('TrimEvent', { seq: z.number() })

  bus.on(TrimEvent, () => 'ok')

  // Dispatch 10 events; they'll process and complete in FIFO order
  for (let i = 0; i < 10; i++) {
    bus.emit(TrimEvent({ seq: i }))
  }
  await bus.waitUntilIdle()

  // History should be trimmed to at most max_history_size
  assert.ok(bus.event_history.size <= 5, `expected <= 5, got ${bus.event_history.size}`)

  // The remaining events should be the MOST RECENT ones (oldest completed removed first)
  const seqs = Array.from(bus.event_history.values()).map((e) => (e as any).seq as number)
  for (let i = 1; i < seqs.length; i++) {
    assert.ok(seqs[i] > seqs[i - 1], 'remaining history should be in order')
  }
})

test('unlimited history (max_history_size: null) keeps all events', async () => {
  const bus = new EventBus('UnlimitedHistBus', { max_history_size: null })
  const PingEvent = BaseEvent.extend('PingEvent', {})

  bus.on(PingEvent, () => 'pong')

  for (let i = 0; i < 150; i++) {
    bus.emit(PingEvent({}))
  }
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 150)

  // All completed
  for (const event of bus.event_history.values()) {
    assert.equal(event.event_status, 'completed')
  }
})

test('max_history_drop=false rejects new dispatch when history is full', async () => {
  const bus = new EventBus('NoDropHistBus', { max_history_size: 2, max_history_drop: false })
  const NoDropEvent = BaseEvent.extend('NoDropEvent', { seq: z.number() })

  bus.on(NoDropEvent, () => 'ok')

  await bus.emit(NoDropEvent({ seq: 1 })).done()
  await bus.emit(NoDropEvent({ seq: 2 })).done()

  assert.equal(bus.event_history.size, 2)
  assert.throws(() => bus.emit(NoDropEvent({ seq: 3 })), /history limit reached \(2\/2\); set event_history\.max_history_drop=true/)
  assert.equal(bus.event_history.size, 2)
  assert.equal(bus.pending_event_queue.length, 0)
})

test('max_history_size=0 with max_history_drop=false still allows unbounded queueing and drops completed events', async () => {
  const bus = new EventBus('ZeroHistNoDropBus', { max_history_size: 0, max_history_drop: false })
  const BurstEvent = BaseEvent.extend('BurstEvent', {})

  let release!: () => void
  const unblock = new Promise<void>((resolve) => {
    release = resolve
  })

  bus.on(BurstEvent, async () => {
    await unblock
  })

  const events: BaseEvent[] = []
  for (let i = 0; i < 25; i++) {
    events.push(bus.emit(BurstEvent({})))
  }

  await delay(10)
  assert.ok(bus.pending_event_queue.length > 1)
  assert.ok(bus.event_history.size >= 1)

  release()
  await Promise.all(events.map((event) => event.done()))
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 0)
  assert.equal(bus.pending_event_queue.length, 0)
})

test('max_history_size=0 keeps in-flight events and drops them on completion', async () => {
  const bus = new EventBus('ZeroHistBus', { max_history_size: 0 })
  const SlowEvent = BaseEvent.extend('SlowEvent', {})

  let release!: () => void
  const unblock = new Promise<void>((resolve) => {
    release = resolve
  })

  bus.on(SlowEvent, async () => {
    await unblock
  })

  const first = bus.emit(SlowEvent({}))
  const second = bus.emit(SlowEvent({}))

  await delay(10)
  assert.ok(bus.event_history.has(first.event_id))
  assert.ok(bus.event_history.has(second.event_id))

  release()
  await Promise.all([first.done(), second.done()])
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 0)
})

// ─── Event type derivation ───────────────────────────────────────────────────

test('event_type is derived from extend() name argument', () => {
  const MyCustomEvent = BaseEvent.extend('MyCustomEvent', { val: z.number() })
  const event = MyCustomEvent({ val: 42 })
  assert.equal(event.event_type, 'MyCustomEvent')
})

test('event_type can be overridden at instantiation', () => {
  const FlexEvent = BaseEvent.extend('FlexEvent', {})
  const event = FlexEvent({ event_type: 'OverriddenType' })
  assert.equal(event.event_type, 'OverriddenType')
})

test('handler registration by string matches extend() name', async () => {
  const bus = new EventBus('StringMatchBus', { max_history_size: 100 })
  const NamedEvent = BaseEvent.extend('NamedEvent', {})
  const received: string[] = []

  bus.on('NamedEvent', () => {
    received.push('string_handler')
  })

  bus.emit(NamedEvent({}))
  await bus.waitUntilIdle()

  assert.equal(received.length, 1)
  assert.equal(received[0], 'string_handler')
})

test('wildcard handler receives all events', async () => {
  const bus = new EventBus('WildcardBus', { max_history_size: 100 })
  const EventA = BaseEvent.extend('EventA', {})
  const EventB = BaseEvent.extend('EventB', {})
  const types: string[] = []

  bus.on('*', (event: BaseEvent) => {
    types.push(event.event_type)
  })

  bus.emit(EventA({}))
  bus.emit(EventB({}))
  await bus.waitUntilIdle()

  assert.deepEqual(types, ['EventA', 'EventB'])
})

// ─── Error handling and isolation ────────────────────────────────────────────

test('handler error is captured without crashing the bus', async () => {
  const bus = new EventBus('ErrorBus', { max_history_size: 100 })
  const ErrorEvent = BaseEvent.extend('ErrorEvent', {})

  bus.on(ErrorEvent, () => {
    throw new Error('handler blew up')
  })

  const event = bus.emit(ErrorEvent({}))
  await event.done({ raise_if_any: false })

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')
  assert.ok(original.event_errors.length > 0, 'event should record the error')

  // The handler result should have error status
  const results = Array.from(original.event_results.values())
  assert.equal(results.length, 1)
  assert.equal(results[0].status, 'error')
  assert.ok(results[0].error instanceof Error)
  assert.equal((results[0].error as Error).message, 'handler blew up')
})

test('one handler error does not prevent other handlers from running', async () => {
  const bus = new EventBus('IsolationBus', {
    max_history_size: 100,
    event_handler_concurrency: 'parallel',
  })
  const MultiEvent = BaseEvent.extend('MultiEvent', {})

  const results_seen: string[] = []

  bus.on(MultiEvent, () => {
    results_seen.push('handler_1_ok')
    return 'result_1'
  })
  bus.on(MultiEvent, () => {
    throw new Error('handler_2_fails')
  })
  bus.on(MultiEvent, () => {
    results_seen.push('handler_3_ok')
    return 'result_3'
  })

  const event = bus.emit(MultiEvent({}))
  await event.done({ raise_if_any: false })

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')

  // Both non-erroring handlers should have run
  assert.ok(results_seen.includes('handler_1_ok'))
  assert.ok(results_seen.includes('handler_3_ok'))

  // Check individual results
  const all_results = Array.from(original.event_results.values())
  const completed_results = all_results.filter((r) => r.status === 'completed')
  const error_results = all_results.filter((r) => r.status === 'error')
  assert.equal(completed_results.length, 2)
  assert.equal(error_results.length, 1)
})

test('eventResultsList returns filtered values by default and can return raw values with include', async () => {
  const bus = new EventBus('EventResultsListBus', { event_handler_concurrency: 'serial' })
  const ResultListEvent = BaseEvent.extend('ResultListEvent', {})

  bus.on(ResultListEvent, () => ({ one: 1 }))
  bus.on(ResultListEvent, () => ['two'])
  bus.on(ResultListEvent, () => undefined)

  const values = await bus.emit(ResultListEvent({})).eventResultsList()
  assert.deepEqual(values, [{ one: 1 }, ['two']])

  const raw_values = await bus.emit(ResultListEvent({})).eventResultsList(() => true, {
    raise_if_any: false,
    raise_if_none: false,
  })
  assert.deepEqual(raw_values, [{ one: 1 }, ['two'], undefined])
})

test('eventResultsList supports timeout/include/raise_if_any/raise_if_none arguments', async () => {
  const bus = new EventBus('EventResultsListArgsBus', { event_handler_concurrency: 'serial' })
  const ArgsEvent = BaseEvent.extend('ArgsEvent', {})
  const EmptyEvent = BaseEvent.extend('EmptyEvent', {})
  const IncludeEvent = BaseEvent.extend('IncludeEvent', {})
  const MixedEvent = BaseEvent.extend('MixedEvent', {})
  const TimeoutEvent = BaseEvent.extend('TimeoutEvent', {})

  bus.on(ArgsEvent, () => 'ok')
  bus.on(ArgsEvent, () => {
    throw new Error('boom')
  })
  await assert.rejects(async () => bus.emit(ArgsEvent({})).eventResultsList(), /boom/)

  const values_without_errors = await bus.emit(ArgsEvent({})).eventResultsList({ raise_if_any: false, raise_if_none: true })
  assert.deepEqual(values_without_errors, ['ok'])

  bus.on(EmptyEvent, () => undefined)
  await assert.rejects(async () => bus.emit(EmptyEvent({})).eventResultsList(), /Expected at least one handler/)
  const empty_values = await bus.emit(EmptyEvent({})).eventResultsList({ raise_if_any: false, raise_if_none: false })
  assert.deepEqual(empty_values, [])

  bus.on(MixedEvent, () => undefined)
  bus.on(MixedEvent, () => 'valid')
  const mixed_values = await bus.emit(MixedEvent({})).eventResultsList({ raise_if_any: false, raise_if_none: true })
  assert.deepEqual(mixed_values, ['valid'])

  bus.on(IncludeEvent, () => 'keep')
  bus.on(IncludeEvent, () => 'drop')
  const filtered_values = await bus
    .emit(IncludeEvent({}))
    .eventResultsList((result) => result === 'keep', { raise_if_any: false, raise_if_none: true })
  assert.deepEqual(filtered_values, ['keep'])

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    return 'late'
  })
  await assert.rejects(async () => bus.emit(TimeoutEvent({})).eventResultsList({ timeout: 0.01 }), /Timed out waiting/)
})

// ─── Concurrent dispatch ─────────────────────────────────────────────────────

test('many events dispatched concurrently all complete', async () => {
  const bus = new EventBus('ConcurrentBus', { max_history_size: null })
  const BatchEvent = BaseEvent.extend('BatchEvent', { idx: z.number() })
  let processed = 0

  bus.on(BatchEvent, () => {
    processed += 1
    return 'ok'
  })

  const events: BaseEvent[] = []
  for (let i = 0; i < 100; i++) {
    events.push(bus.emit(BatchEvent({ idx: i })))
  }

  // Wait for all to complete
  await Promise.all(events.map((e) => e.done()))
  await bus.waitUntilIdle()

  assert.equal(processed, 100)
  assert.equal(bus.event_history.size, 100)

  for (const event of bus.event_history.values()) {
    assert.equal(event.event_status, 'completed')
  }
})

// ─── event_timeout default application ───────────────────────────────────────

test('dispatch leaves event_timeout unset and processing uses bus timeout default', async () => {
  const bus = new EventBus('TimeoutDefaultBus', {
    max_history_size: 100,
    event_timeout: 0.01,
  })
  const TEvent = BaseEvent.extend('TEvent', {})
  bus.on(TEvent, async () => {
    await delay(30)
  })

  const event = bus.emit(TEvent({}))
  const original = event._event_original ?? event

  assert.equal(original.event_timeout, null)

  await event.done({ raise_if_any: false })
  assert.equal(original.event_errors.length, 1)
})

test('event with explicit timeout is not overridden by bus default', async () => {
  const bus = new EventBus('TimeoutOverrideBus', {
    max_history_size: 100,
    event_timeout: 42,
  })
  const TEvent = BaseEvent.extend('TEvent', {})

  const event = bus.emit(TEvent({ event_timeout: 10 }))
  const original = event._event_original ?? event

  assert.equal(original.event_timeout, 10)

  await bus.waitUntilIdle()
})

// ─── EventBus.all_instances tracking ─────────────────────────────────────────────

test('EventBus.all_instances tracks all created buses', () => {
  const initial_count = EventBus.all_instances.size
  const bus_a = new EventBus('TrackA')
  const bus_b = new EventBus('TrackB')

  assert.ok(EventBus.all_instances.has(bus_a))
  assert.ok(EventBus.all_instances.has(bus_b))
  assert.equal(EventBus.all_instances.size, initial_count + 2)
})

test('EventBus subclasses isolate registries and global-serial locks', () => {
  class IsolatedBusA extends EventBus {}
  class IsolatedBusB extends EventBus {}

  const bus_a1 = new IsolatedBusA('IsolatedBusA1', { event_concurrency: 'global-serial' })
  const bus_a2 = new IsolatedBusA('IsolatedBusA2', { event_concurrency: 'global-serial' })
  const bus_b1 = new IsolatedBusB('IsolatedBusB1', { event_concurrency: 'global-serial' })

  assert.equal(IsolatedBusA.all_instances.has(bus_a1), true)
  assert.equal(IsolatedBusA.all_instances.has(bus_a2), true)
  assert.equal(IsolatedBusA.all_instances.has(bus_b1), false)
  assert.equal(IsolatedBusB.all_instances.has(bus_b1), true)
  assert.equal(IsolatedBusB.all_instances.has(bus_a1), false)
  assert.equal(EventBus.all_instances.has(bus_a1), false)
  assert.equal(EventBus.all_instances.has(bus_b1), false)

  const lock_a1 = bus_a1.locks.getLockForEvent(new BaseEvent())
  const lock_a2 = bus_a2.locks.getLockForEvent(new BaseEvent())
  const lock_b1 = bus_b1.locks.getLockForEvent(new BaseEvent())
  assert.notEqual(lock_a1, null)
  assert.notEqual(lock_a2, null)
  assert.notEqual(lock_b1, null)
  assert.equal(lock_a1, lock_a2)
  assert.notEqual(lock_a1, lock_b1)

  bus_a1.destroy()
  bus_a2.destroy()
  bus_b1.destroy()
})

// ─── Circular forwarding prevention ──────────────────────────────────────────

test('circular forwarding does not cause infinite loop', async () => {
  const bus_a = new EventBus('CircA', { max_history_size: 100 })
  const bus_b = new EventBus('CircB', { max_history_size: 100 })
  const bus_c = new EventBus('CircC', { max_history_size: 100 })

  // A -> B -> C -> A (circular)
  bus_a.on('*', bus_b.emit)
  bus_b.on('*', bus_c.emit)
  bus_c.on('*', bus_a.emit)

  const CircEvent = BaseEvent.extend('CircEvent', {})
  const handler_calls: string[] = []

  // Register real handlers on each bus
  bus_a.on(CircEvent, () => {
    handler_calls.push('A')
    return 'a'
  })
  bus_b.on(CircEvent, () => {
    handler_calls.push('B')
    return 'b'
  })
  bus_c.on(CircEvent, () => {
    handler_calls.push('C')
    return 'c'
  })

  const event = bus_a.emit(CircEvent({}))
  await event.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  // Each bus should process the event exactly once (loop prevention via event_path)
  assert.equal(handler_calls.filter((h) => h === 'A').length, 1)
  assert.equal(handler_calls.filter((h) => h === 'B').length, 1)
  assert.equal(handler_calls.filter((h) => h === 'C').length, 1)

  // event_path should contain all three buses
  const original = event._event_original ?? event
  assert.ok(original.event_path.includes(bus_a.label))
  assert.ok(original.event_path.includes(bus_b.label))
  assert.ok(original.event_path.includes(bus_c.label))
})

// ─── EventBus GC / memory leak ───────────────────────────────────────────────

const flush_gc_cycles = async (gc: () => void, cycles: number): Promise<void> => {
  for (let i = 0; i < cycles; i += 1) {
    gc()
    await new Promise<void>((resolve) => setImmediate(resolve))
  }
}

test('unreferenced EventBus can be garbage collected (not retained by all_instances)', async () => {
  const gc = globalThis.gc
  if (typeof gc !== 'function') {
    assert.fail('GC tests require --expose-gc')
  }

  let weak_ref: WeakRef<EventBus> | null = null

  // Create a bus inside an IIFE so the only reference is the WeakRef
  ;(() => {
    const bus = new EventBus('GCTestBus')
    weak_ref = new WeakRef(bus)
  })()

  await flush_gc_cycles(gc, 20)

  // If EventBus.all_instances holds a strong reference (Set<EventBus>),
  // the bus will NOT be collected — proving the memory leak.
  // After the fix (WeakRef-based storage), the bus should be collected.
  assert.notEqual(weak_ref, null, 'WeakRef should be assigned by the test setup IIFE')
  assert.equal(
    weak_ref.deref(),
    undefined,
    'bus should be garbage collected when no external references remain — ' +
      'EventBus.all_instances is holding a strong reference (memory leak)'
  )
})

test('subclass registry and global lock are collectable when subclass goes out of scope', async () => {
  const gc = globalThis.gc
  if (typeof gc !== 'function') {
    assert.fail('GC tests require --expose-gc')
  }

  let subclass_ref!: WeakRef<typeof EventBus>
  let registry_ref!: WeakRef<GlobalEventBusRegistry>
  let lock_ref!: WeakRef<AsyncLock>
  let bus_ref!: WeakRef<EventBus>
  ;(() => {
    class ScopedSubclassBus extends EventBus {}
    const bus = new ScopedSubclassBus('ScopedSubclassBus', { event_concurrency: 'global-serial' })
    subclass_ref = new WeakRef(ScopedSubclassBus)
    registry_ref = new WeakRef(ScopedSubclassBus.all_instances)
    lock_ref = new WeakRef(bus._lock_for_event_global_serial)
    bus_ref = new WeakRef(bus)
  })()

  await flush_gc_cycles(gc, 300)

  assert.equal(bus_ref.deref(), undefined, 'subclass bus instance should be collectable')
  assert.equal(subclass_ref.deref(), undefined, 'subclass type should be collectable')
  assert.equal(registry_ref.deref(), undefined, 'subclass all_instances registry should be collectable')
  assert.equal(lock_ref.deref(), undefined, 'subclass global lock should be collectable')
})

test('unreferenced buses with event history are garbage collected without destroy()', async () => {
  const gc = globalThis.gc
  if (typeof gc !== 'function') {
    assert.fail('GC tests require --expose-gc')
  }

  class IsolatedRegistryBus extends EventBus {}

  const GcEvent = BaseEvent.extend('GcNoDestroyEvent', {})
  const weak_refs: Array<WeakRef<IsolatedRegistryBus>> = []
  const created_bus_ids: string[] = []

  await flush_gc_cycles(gc, 10)
  const heap_before = process.memoryUsage().heapUsed

  const create_and_run_bus = async (index: number): Promise<{ ref: WeakRef<IsolatedRegistryBus>; id: string }> => {
    const bus = new IsolatedRegistryBus(`GC-NoDestroy-${index}`, { max_history_size: 200 })
    bus.on(GcEvent, () => {})
    for (let i = 0; i < 200; i += 1) {
      const event = bus.emit(GcEvent({}))
      await event.done()
    }
    await bus.waitUntilIdle()
    return { ref: new WeakRef(bus), id: bus.id }
  }

  for (let i = 0; i < 120; i += 1) {
    const { ref, id } = await create_and_run_bus(i)
    weak_refs.push(ref)
    created_bus_ids.push(id)
  }

  await flush_gc_cycles(gc, 30)

  const alive_count = weak_refs.reduce((count, ref) => count + (ref.deref() ? 1 : 0), 0)
  const remaining_ids = new Set(Array.from(IsolatedRegistryBus.all_instances).map((bus) => bus.id))
  const heap_after = process.memoryUsage().heapUsed

  assert.equal(alive_count, 0, 'all unreferenced buses should be garbage collected without explicit destroy()')
  for (const id of created_bus_ids) {
    assert.equal(remaining_ids.has(id), false, `all_instances should not retain unreferenced bus ${id}`)
  }
  assert.ok(
    heap_after <= heap_before + 20 * 1024 * 1024,
    `heap should return near baseline after GC, before=${(heap_before / 1024 / 1024).toFixed(1)}MB after=${(heap_after / 1024 / 1024).toFixed(1)}MB`
  )
})

// Consolidated from tests/coverage_gaps.test.ts

test('reset creates a fresh pending event for cross-bus dispatch', async () => {
  const ResetEvent = BaseEvent.extend('ResetCoverageEvent', {
    label: z.string(),
  })

  const bus_a = new EventBus('ResetCoverageBusA')
  const bus_b = new EventBus('ResetCoverageBusB')

  bus_a.on(ResetEvent, (event) => `a:${event.label}`)
  bus_b.on(ResetEvent, (event) => `b:${event.label}`)

  const completed = await bus_a.emit(ResetEvent({ label: 'hello' })).done()
  const fresh = completed.eventReset()

  assert.notEqual(fresh.event_id, completed.event_id)
  assert.equal(fresh.event_status, 'pending')
  assert.equal(fresh.event_results.size, 0)
  assert.equal(fresh.event_started_at, null)
  assert.equal(fresh.event_completed_at, null)

  const forwarded = await bus_b.emit(fresh).done()
  assert.equal(forwarded.event_status, 'completed')
  assert.equal(
    Array.from(forwarded.event_results.values()).some((result) => result.result === 'b:hello'),
    true
  )
  assert.equal(
    forwarded.event_path.some((entry) => entry.startsWith('ResetCoverageBusA#')),
    true
  )
  assert.equal(
    forwarded.event_path.some((entry) => entry.startsWith('ResetCoverageBusB#')),
    true
  )

  bus_a.destroy()
  bus_b.destroy()
})

test('scoped handler event reports bus and _event_original via in-operator', async () => {
  const ProxyEvent = BaseEvent.extend('ProxyHasCoverageEvent', {})
  const bus = new EventBus('ProxyHasCoverageBus')
  let has_bus = false
  let has_original = false

  bus.on(ProxyEvent, (event) => {
    has_bus = 'bus' in event
    has_original = '_event_original' in event
  })

  await bus.emit(ProxyEvent({})).done()

  assert.equal(has_bus, true)
  assert.equal(has_original, true)
  bus.destroy()
})

test('on() rejects BaseEvent matcher without a concrete event type', () => {
  const bus = new EventBus('InvalidMatcherCoverageBus')
  assert.throws(() => bus.on(BaseEvent as unknown as any, () => undefined), /must be a string event type/)
  bus.destroy()
})

test('max_history_size=0 prunes previously completed events on later dispatch', async () => {
  const HistEvent = BaseEvent.extend('ZeroHistoryCoverageEvent', {
    label: z.string(),
  })
  const bus = new EventBus('ZeroHistoryCoverageBus', { max_history_size: 1 })
  bus.on(HistEvent, () => undefined)

  const first = await bus.emit(HistEvent({ label: 'first' })).done()
  assert.equal(bus.event_history.has(first.event_id), true)

  bus.event_history.max_history_size = 0
  const second = await bus.emit(HistEvent({ label: 'second' })).done()
  assert.equal(bus.event_history.has(first.event_id), false)
  assert.equal(bus.event_history.has(second.event_id), false)
  assert.equal(bus.event_history.size, 0)

  bus.destroy()
})
