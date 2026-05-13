import assert from 'node:assert/strict'
import http from 'node:http'
import { readdirSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'
import { GlobalEventBusRegistry } from '../src/EventBus.js'
import { AsyncLock } from '../src/LockManager.js'
import { z } from 'zod'
import {
  ROOT_CONTEXT,
  SpanKind,
  type Context,
  type Span,
  type SpanAttributes,
  type SpanAttributeValue,
  type SpanContext,
  type SpanOptions,
  type SpanStatus,
  type TimeInput,
  type Tracer,
} from '@opentelemetry/api'
import { BasicTracerProvider, SimpleSpanProcessor, type ReadableSpan, type SpanExporter } from '@opentelemetry/sdk-trace-base'

import type { EventBusMiddleware } from '../src/EventBusMiddleware.js'
import { OtelTracingMiddleware, type OtelTracingSpanFactoryInput } from '../src/OtelTracingMiddleware.js'
import {
  clearSemaphoreRegistry,
  EventHandlerAbortedError,
  EventHandlerCancelledError,
  EventHandlerResultSchemaError,
  EventHandlerTimeoutError,
  retry,
} from '../src/index.js'

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

test('EventBus with zero event_timeout disables timeouts', () => {
  const bus = new EventBus('NoTimeoutBus', { event_timeout: 0 })
  assert.equal(bus.event_timeout, 0)
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
  await standalone.wait()

  const bus = new EventBus('LifecycleMethodInvocationBus')
  const dispatched = bus.emit(LifecycleEvent({}))
  await dispatched.wait()
  assert.equal(dispatched.event_status, 'completed')
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('RuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('RuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.emit(RuntimeEvent({}))
  await event.now()

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_at, 'string')
  assert.equal(typeof json.event_started_at, 'string')
  assert.equal(typeof json.event_completed_at, 'string')
  assert.equal(json.event_pending_bus_count, 0)

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.deepEqual(JSON.parse(JSON.stringify(restored.toJSON())), JSON.parse(JSON.stringify(json)))
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_at, event.event_created_at)
  assert.equal(restored.event_pending_bus_count, 0)
  assert.equal(restored.event_results.size, 1)
  const restored_result = Array.from(restored.event_results.values())[0]
  assert.ok(restored_result)
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
    event_timeout: 0,
  })
  assert.equal(missing_field_event.event_parent_id, null)

  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001234',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullParentIdEvent',
    event_parent_id: null,
    event_timeout: 0,
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
    event_timeout: 0,
  })
  assert.equal(missing_field_event.event_emitted_by_handler_id, null)

  const json_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullEmittedByIdEvent',
    event_emitted_by_handler_id: null,
    event_timeout: 0,
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
    event_timeout: 0,
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
    event_timeout: 0,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEvent', () => 123)
  await bus.emit(int_event).now()
  const int_result = Array.from(int_event.event_results.values())[0]
  assert.equal(int_result.status, 'completed')

  const int_bad_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001237',
    event_created_at: new Date('2025-01-01T00:00:03.000Z').toISOString(),
    event_type: 'RawIntegerEventBad',
    event_timeout: 0,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEventBad', () => 1.5)
  await bus.emit(int_bad_event).now()
  const int_bad_result = Array.from(int_bad_event.event_results.values())[0]
  assert.equal(int_bad_result.status, 'error')

  const null_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001238',
    event_created_at: new Date('2025-01-01T00:00:04.000Z').toISOString(),
    event_type: 'RawNullEvent',
    event_timeout: 0,
    event_result_type: { type: 'null' },
  })
  bus.on('RawNullEvent', () => null)
  await bus.emit(null_event).now()
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
    event_timeout: 0,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEvent', () => ({ items: [1, 2, 3], meta: { ok: true } }))
  await bus.emit(valid_event).now()
  const valid_result = Array.from(valid_event.event_results.values())[0]
  assert.equal(valid_result.status, 'completed')

  const invalid_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001240',
    event_created_at: new Date('2025-01-01T00:00:06.000Z').toISOString(),
    event_type: 'RawNestedSchemaEventBad',
    event_timeout: 0,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEventBad', () => ({ items: ['bad'], meta: { ok: 'yes' } }))
  await bus.emit(invalid_event).now()
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

  await event.now()

  assert.equal(status_during_handler, 'started')
  assert.equal(original.event_status, 'completed')
  assert.ok(original.event_started_at, 'event_started_at should be set')
  assert.ok(original.event_completed_at, 'event_completed_at should be set')
})

test('event with no handlers completes immediately', async () => {
  const bus = new EventBus('NoHandlerBus', { max_history_size: 100 })
  const OrphanEvent = BaseEvent.extend('OrphanEvent', {})

  const event = bus.emit(OrphanEvent({}))
  await event.now()

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

  await bus.emit(NoDropEvent({ seq: 1 })).now()
  await bus.emit(NoDropEvent({ seq: 2 })).now()

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
  await Promise.all(events.map((event) => event.now()))
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
  await Promise.all([first.now(), second.now()])
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
  await event.now()

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
  await event.now()

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
  const bus = new EventBus('EventResultsBus', { event_handler_concurrency: 'serial' })
  const ResultListEvent = BaseEvent.extend('ResultListEvent', {})

  bus.on(ResultListEvent, () => ({ one: 1 }))
  bus.on(ResultListEvent, () => ['two'])
  bus.on(ResultListEvent, () => undefined)

  const values = await bus.emit(ResultListEvent({})).eventResultsList()
  assert.deepEqual(values, [{ one: 1 }, ['two']])

  const raw_values = await bus.emit(ResultListEvent({})).eventResultsList({
    include: (result) => result !== 'never',
    raise_if_any: false,
    raise_if_none: false,
  })
  assert.deepEqual(raw_values, [{ one: 1 }, ['two'], undefined])
})

test('eventResultsList returns results in handler registration order', async () => {
  const bus = new EventBus('EventResultsOrderBus', { event_handler_concurrency: 'parallel' })
  const ResultOrderEvent = BaseEvent.extend('ResultOrderEvent', {})
  const completed_order: string[] = []
  const registered_at = '2026-01-01T00:00:00.000Z'

  bus.on(
    ResultOrderEvent,
    async () => {
      await delay(30)
      completed_order.push('null')
      return undefined
    },
    { id: '00000000-0000-5000-8000-00000000000b', handler_registered_at: registered_at }
  )
  bus.on(
    ResultOrderEvent,
    async () => {
      await delay(20)
      completed_order.push('winner')
      return 'winner'
    },
    { id: '00000000-0000-5000-8000-00000000000c', handler_registered_at: registered_at }
  )
  bus.on(
    ResultOrderEvent,
    async () => {
      completed_order.push('late')
      return 'late'
    },
    { id: '00000000-0000-5000-8000-00000000000a', handler_registered_at: registered_at }
  )

  const event = bus.emit(ResultOrderEvent({}))
  const values = await event.eventResultsList({ raise_if_any: false, raise_if_none: true })
  assert.deepEqual(values, ['winner', 'late'])

  const raw_values = await event.eventResultsList({
    include: (result) => result !== 'never',
    raise_if_any: false,
    raise_if_none: false,
  })
  assert.deepEqual(raw_values, [undefined, 'winner', 'late'])
  assert.deepEqual(completed_order, ['late', 'winner', 'null'])
})

test('test_event_results_list_defaults_filter_empty_values_raise_errors_and_options_override', async () => {
  const bus = new EventBus('EventResultsArgsBus', { event_handler_concurrency: 'serial' })
  const DefaultsEvent = BaseEvent.extend('DefaultsEvent', {})
  const ArgsEvent = BaseEvent.extend('ArgsEvent', {})
  const EmptyEvent = BaseEvent.extend('EmptyEvent', {})
  const IncludeEvent = BaseEvent.extend('IncludeEvent', {})
  const MixedEvent = BaseEvent.extend('MixedEvent', {})

  bus.on(DefaultsEvent, () => 'ok')
  bus.on(DefaultsEvent, () => undefined)
  bus.on(DefaultsEvent, () => BaseEvent.extend('ForwardedResultEvent', {})({}))
  const default_event = bus.emit(DefaultsEvent({}))
  await default_event.now()
  assert.deepEqual(await default_event.eventResultsList(), ['ok'])

  bus.on(ArgsEvent, () => 'ok')
  bus.on(ArgsEvent, () => {
    throw new Error('boom')
  })
  const error_event = bus.emit(ArgsEvent({}))
  await error_event.now()
  await assert.rejects(
    async () => error_event.eventResultsList({ raise_if_any: true }),
    (error) => error instanceof Error && !(error instanceof AggregateError) && error.message === 'boom'
  )

  const values_without_errors = await error_event.eventResultsList({ raise_if_any: false, raise_if_none: true })
  assert.deepEqual(values_without_errors, ['ok'])

  bus.on(EmptyEvent, () => undefined)
  const empty_event = bus.emit(EmptyEvent({}))
  await empty_event.now()
  await assert.rejects(async () => empty_event.eventResultsList({ raise_if_none: true }), /Expected at least one handler/)
  const empty_values = await empty_event.eventResultsList({ raise_if_any: false, raise_if_none: false })
  assert.deepEqual(empty_values, [])

  bus.on(MixedEvent, () => undefined)
  bus.on(MixedEvent, () => 'valid')
  const mixed_event = bus.emit(MixedEvent({}))
  await mixed_event.now()
  const mixed_values = await mixed_event.eventResultsList({ raise_if_any: false, raise_if_none: true })
  assert.deepEqual(mixed_values, ['valid'])

  bus.on(IncludeEvent, () => 'keep')
  bus.on(IncludeEvent, () => 'drop')
  const seen_include_handlers: Array<string | null> = []
  const include_event = bus.emit(IncludeEvent({}))
  await include_event.now()
  const filtered_values = await include_event.eventResultsList({
    include: (result, event_result) => {
      assert.equal(result, event_result.result)
      seen_include_handlers.push(event_result.handler_name)
      return result === 'keep'
    },
    raise_if_any: false,
    raise_if_none: true,
  })
  assert.deepEqual(filtered_values, ['keep'])
  assert.equal(seen_include_handlers.length, 2)
})

test('destroy default clear is terminal and frees bus state', async () => {
  const DestroyEvent = BaseEvent.extend('DestroyDefaultClearEvent', {})
  const bus = new EventBus('DestroyDefaultClearBus')

  bus.on(DestroyEvent, () => 'done')
  const event = await bus.emit(DestroyEvent({})).now()
  assert.equal(await event.eventResult(), 'done')

  await bus.destroy()

  assert.equal(bus.runloop_running, false)
  assert.equal(bus.pending_event_queue.length, 0)
  assert.equal(bus.handlers.size, 0)
  assert.equal(bus.handlers_by_key.size, 0)
  assert.equal(bus.event_history.size, 0)
  assert.equal(bus.in_flight_event_ids.size, 0)
  assert.equal(bus.find_waiters.size, 0)
  assert.equal(bus.all_instances.has(bus), false)

  assert.throws(() => bus.on(DestroyEvent, () => 'again'), /destroyed/)
  assert.throws(() => bus.emit(DestroyEvent({})), /destroyed/)
  await assert.rejects(() => bus.find(DestroyEvent, { future: false }), /destroyed/)
})

test('destroy clear false preserves handlers and history, resolves waiters, and is terminal', async () => {
  const TerminalEvent = BaseEvent.extend('DestroyClearFalseTerminalEvent', {})
  const bus = new EventBus('DestroyClearFalseTerminalBus')
  let calls = 0

  bus.on(TerminalEvent, () => {
    calls += 1
    return `handled:${calls}`
  })

  const first = await bus.emit(TerminalEvent({})).now()
  assert.equal(await first.eventResult(), 'handled:1')

  const waiter = bus.find('NeverHappens', { past: false, future: true })
  await Promise.resolve()

  await bus.destroy({ clear: false })

  assert.equal(await Promise.race([waiter, delay(1000).then(() => 'timeout')]), null)
  assert.equal(bus.runloop_running, false)
  assert.equal(bus.pending_event_queue.length, 0)
  assert.equal(bus.handlers.size, 1)
  assert.equal(bus.event_history.size, 1)
  assert.equal(bus.all_instances.has(bus), false)

  assert.throws(() => bus.on(TerminalEvent, () => 'again'), /destroyed/)
  assert.throws(() => bus.emit(TerminalEvent({})), /destroyed/)
  await assert.rejects(() => bus.find(TerminalEvent, { future: false }), /destroyed/)
  await bus.destroy()
})

test('destroy is immediate and rejects late handler emits', async () => {
  const ImmediateDestroyEvent = BaseEvent.extend('ImmediateDestroyEvent', {})
  const bus = new EventBus('DestroyImmediateBus')
  let mark_started!: () => void
  const handler_started = new Promise<void>((resolve) => {
    mark_started = resolve
  })
  let release_handler!: () => void
  const handler_released = new Promise<void>((resolve) => {
    release_handler = resolve
  })
  let mark_late_emit_rejected!: (value: boolean) => void
  const late_emit_rejected = new Promise<boolean>((resolve) => {
    mark_late_emit_rejected = resolve
  })

  bus.on(ImmediateDestroyEvent, async () => {
    mark_started()
    await handler_released
    try {
      bus.emit(ImmediateDestroyEvent({}))
    } catch {
      mark_late_emit_rejected(true)
      return
    }
    mark_late_emit_rejected(false)
  })

  bus.emit(ImmediateDestroyEvent({}))
  await handler_started

  const start_ms = performance.now()
  await bus.destroy({ clear: false })
  const elapsed_ms = performance.now() - start_ms

  assert.ok(elapsed_ms < 50, `Destroy should be immediate, elapsed=${elapsed_ms}ms`)
  assert.equal(bus.runloop_running, false)
  assert.equal(bus.event_history.size, 1)
  assert.equal(bus.all_instances.has(bus), false)
  assert.throws(() => bus.emit(ImmediateDestroyEvent({})), /destroyed/)

  release_handler()
  assert.equal(await Promise.race([late_emit_rejected, delay(1000).then(() => false)]), true)
  await bus.destroy()
})

test('destroying one bus does not break shared handlers or forward targets', async () => {
  const SharedDestroyEvent = BaseEvent.extend('SharedDestroyEvent', {})
  const source = new EventBus('DestroySharedSourceBus')
  const target = new EventBus('DestroySharedTargetBus')
  let seen = 0
  const shared_handler = () => {
    seen += 1
    return 'shared'
  }

  source.on(SharedDestroyEvent, shared_handler)
  source.on('*', (event) => target.emit(event))
  target.on(SharedDestroyEvent, shared_handler)

  const forwarded = await source.emit(SharedDestroyEvent({})).now()
  assert.equal((await forwarded.eventResultsList({ raise_if_any: false })).filter((value) => value === 'shared').length, 2)

  await source.destroy()

  const direct = await target.emit(SharedDestroyEvent({})).now()
  assert.equal(await direct.eventResult(), 'shared')
  assert.equal(target.handlers.size, 1)
  assert.equal(seen, 3)

  await target.destroy()
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
  await Promise.all(events.map((e) => e.now()))
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

  assert.equal(original.event_timeout ?? null, null)

  await event.now()
  assert.equal(original.event_timeout ?? null, null)
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
  await event.now()
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
      await event.now()
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

  const completed = await bus_a.emit(ResetEvent({ label: 'hello' })).now()
  const fresh = completed.eventReset()

  assert.notEqual(fresh.event_id, completed.event_id)
  assert.equal(fresh.event_status, 'pending')
  assert.equal(fresh.event_results.size, 0)
  assert.equal(fresh.event_started_at, null)
  assert.equal(fresh.event_completed_at, null)

  const forwarded = await bus_b.emit(fresh).now()
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

test('scoped handler event reports event_bus and _event_original via in-operator', async () => {
  const ProxyEvent = BaseEvent.extend('ProxyHasCoverageEvent', {})
  const bus = new EventBus('ProxyHasCoverageBus')
  let has_event_bus = false
  let has_legacy_bus = true
  let has_original = false

  bus.on(ProxyEvent, (event) => {
    has_event_bus = 'event_bus' in event
    has_legacy_bus = 'bus' in event
    has_original = '_event_original' in event
  })

  await bus.emit(ProxyEvent({})).now()

  assert.equal(has_event_bus, true)
  assert.equal(has_legacy_bus, false)
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

  const first = await bus.emit(HistEvent({ label: 'first' })).now()
  assert.equal(bus.event_history.has(first.event_id), true)

  bus.event_history.max_history_size = 0
  const second = await bus.emit(HistEvent({ label: 'second' })).now()
  assert.equal(bus.event_history.has(first.event_id), false)
  assert.equal(bus.event_history.has(second.event_id), false)
  assert.equal(bus.event_history.size, 0)

  bus.destroy()
})

// Folded from EventBusMiddleware.test.ts to keep test layout class-based.
const flushHooks = async (ticks: number = 4): Promise<void> => {
  for (let i = 0; i < ticks; i += 1) {
    await Promise.resolve()
  }
}

type HookRecord = {
  middleware: string
  hook: 'event' | 'result' | 'handler'
  bus_id: string
  status?: 'pending' | 'started' | 'completed'
  handler_id?: string
  registered?: boolean
}

type HandlerChangeRecord = {
  handler_id: string
  event_pattern: string
  registered: boolean
  eventbus_id: string
}

class RecordingMiddleware implements EventBusMiddleware {
  name: string
  records: HookRecord[]
  sequence: string[] | null

  constructor(name: string, sequence: string[] | null = null) {
    this.name = name
    this.records = []
    this.sequence = sequence
  }

  async onEventChange(eventbus: EventBus, _event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
    this.records.push({ middleware: this.name, hook: 'event', status, bus_id: eventbus.id })
    this.sequence?.push(`${this.name}:event:${status}`)
  }

  async onEventResultChange(
    eventbus: EventBus,
    _event: BaseEvent,
    event_result: { handler_id: string },
    status: 'pending' | 'started' | 'completed'
  ): Promise<void> {
    this.records.push({ middleware: this.name, hook: 'result', status, handler_id: event_result.handler_id, bus_id: eventbus.id })
    this.sequence?.push(`${this.name}:result:${status}`)
  }

  async onBusHandlersChange(eventbus: EventBus, handler: { id: string }, registered: boolean): Promise<void> {
    this.records.push({ middleware: this.name, hook: 'handler', registered, handler_id: handler.id, bus_id: eventbus.id })
    this.sequence?.push(`${this.name}:handler:${registered ? 'registered' : 'unregistered'}`)
  }
}

test('middleware ctor+instance normalization and handler registration hooks', async () => {
  class CtorMiddleware extends RecordingMiddleware {
    static created = 0

    constructor() {
      super('ctor')
      CtorMiddleware.created += 1
    }
  }

  const instance = new RecordingMiddleware('instance')
  const bus = new EventBus('MiddlewareCtorBus', {
    middlewares: [CtorMiddleware, instance],
  })
  const Event = BaseEvent.extend('MiddlewareCtorEvent', {})
  const handler = bus.on(Event, () => 'ok')
  bus.off(Event, handler)

  await flushHooks()

  assert.equal(CtorMiddleware.created, 1)
  assert.equal(
    instance.records.some((record) => record.hook === 'handler' && record.registered === true),
    true
  )
  assert.equal(
    instance.records.some((record) => record.hook === 'handler' && record.registered === false),
    true
  )

  bus.destroy()
})

test('middleware hooks execute sequentially in registration order', async () => {
  const sequence: string[] = []
  class FirstMiddleware extends RecordingMiddleware {
    constructor() {
      super('first', sequence)
    }
  }
  class SecondMiddleware extends RecordingMiddleware {
    constructor() {
      super('second', sequence)
    }
  }

  const bus = new EventBus('MiddlewareOrderBus', { middlewares: [FirstMiddleware, SecondMiddleware] })
  const Event = BaseEvent.extend('MiddlewareOrderEvent', {})

  bus.on(Event, () => 'ok')
  await bus.emit(Event({ event_timeout: 0.2 })).now()
  await flushHooks()

  const pairs: Array<[string, string]> = [
    ['first:event:pending', 'second:event:pending'],
    ['first:event:started', 'second:event:started'],
    ['first:result:pending', 'second:result:pending'],
    ['first:result:started', 'second:result:started'],
    ['first:result:completed', 'second:result:completed'],
    ['first:event:completed', 'second:event:completed'],
  ]
  for (const [first, second] of pairs) {
    assert.ok(sequence.indexOf(first) >= 0, `missing sequence marker: ${first}`)
    assert.ok(sequence.indexOf(second) >= 0, `missing sequence marker: ${second}`)
    assert.ok(sequence.indexOf(first) < sequence.indexOf(second), `expected ${first} before ${second}`)
  }

  bus.destroy()
})

test('middleware hooks are per-bus on forwarded events', async () => {
  const middleware_a = new RecordingMiddleware('a')
  const middleware_b = new RecordingMiddleware('b')
  const bus_a = new EventBus('MiddlewareForwardA', { middlewares: [middleware_a] })
  const bus_b = new EventBus('MiddlewareForwardB', { middlewares: [middleware_b] })
  const Event = BaseEvent.extend('MiddlewareForwardEvent', {})

  const handler_a = bus_a.on(Event, async (event) => {
    bus_b.emit(event)
  })
  const handler_b = bus_b.on(Event, async () => 'ok')

  await bus_a.emit(Event({ event_timeout: 0.2 })).now()
  await flushHooks()

  assert.equal(
    middleware_a.records.some((record) => record.hook === 'result' && record.handler_id === handler_a.id),
    true
  )
  assert.equal(
    middleware_a.records.some((record) => record.hook === 'result' && record.handler_id === handler_b.id),
    false
  )
  assert.equal(
    middleware_b.records.some((record) => record.hook === 'result' && record.handler_id === handler_b.id),
    true
  )

  bus_a.destroy()
  bus_b.destroy()
})

test('middleware emits event lifecycle hooks for no-handler events', async () => {
  const middleware = new RecordingMiddleware('single')
  const bus = new EventBus('MiddlewareNoHandlerBus', { middlewares: [middleware] })
  const Event = BaseEvent.extend('MiddlewareNoHandlerEvent', {})

  await bus.emit(Event({ event_timeout: 0.2 })).now()
  await flushHooks()

  const event_statuses = middleware.records
    .filter((record) => record.hook === 'event')
    .map((record) => record.status)
    .filter((status): status is 'pending' | 'started' | 'completed' => status !== undefined)
  assert.deepEqual(event_statuses, ['pending', 'started', 'completed'])
  assert.equal(
    middleware.records.some((record) => record.hook === 'result'),
    false
  )

  bus.destroy()
})

test('middleware event lifecycle ordering is deterministic per event', async () => {
  const event_statuses_by_id = new Map<string, Array<'pending' | 'started' | 'completed'>>()

  class LifecycleMiddleware extends RecordingMiddleware {
    constructor() {
      super('deterministic')
    }

    async onEventChange(eventbus: EventBus, event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      const statuses = event_statuses_by_id.get(event.event_id) ?? []
      statuses.push(status)
      event_statuses_by_id.set(event.event_id, statuses)
      await super.onEventChange(eventbus, event, status)
    }
  }

  const bus = new EventBus('MiddlewareDeterministicBus', { middlewares: [LifecycleMiddleware], max_history_size: null })
  const Event = BaseEvent.extend('MiddlewareDeterministicEvent', {})

  bus.on(Event, async () => {
    await delay(0)
    return 'ok'
  })

  const batch_count = 5
  const events_per_batch = 50
  for (let batch_index = 0; batch_index < batch_count; batch_index += 1) {
    const events = Array.from({ length: events_per_batch }, (_unused, _event_index) => bus.emit(Event({ event_timeout: 0.2 })))
    await Promise.all(events.map((event) => event.now()))
    await flushHooks()

    for (const event of events) {
      assert.deepEqual(event_statuses_by_id.get(event.event_id), ['pending', 'started', 'completed'])
    }
  }
  assert.equal(event_statuses_by_id.size, batch_count * events_per_batch)

  bus.destroy()
})

test('middleware result hooks never reverse from completed to started', async () => {
  const middleware = new RecordingMiddleware('ordering')
  const bus = new EventBus('MiddlewareOrderingBus', { middlewares: [middleware], event_handler_concurrency: 'parallel' })
  const Event = BaseEvent.extend('MiddlewareOrderingEvent', {})

  bus.on(Event, async () => {
    await delay(50)
    return 'slow'
  })
  bus.on(Event, async () => {
    await delay(50)
    return 'slow-2'
  })

  const timeout_event = bus.emit(Event({ event_timeout: 0.01 }))
  await timeout_event.wait()
  await flushHooks()

  const statuses_by_handler = new Map<string, string[]>()
  for (const record of middleware.records.filter((record) => record.hook === 'result' && record.handler_id && record.status)) {
    const statuses = statuses_by_handler.get(record.handler_id!) ?? []
    statuses.push(record.status!)
    statuses_by_handler.set(record.handler_id!, statuses)
  }
  for (const statuses of statuses_by_handler.values()) {
    const completed_index = statuses.indexOf('completed')
    if (completed_index >= 0) {
      assert.equal(statuses.slice(completed_index + 1).includes('started'), false)
    }
  }

  bus.destroy()
})

test('hard event timeout finalizes immediately without waiting for in-flight handlers', async () => {
  const bus = new EventBus('MiddlewareHardTimeoutBus', {
    event_handler_concurrency: 'parallel',
  })
  const Event = BaseEvent.extend('MiddlewareHardTimeoutEvent', {})

  bus.on(Event, async () => {
    await delay(200)
    return 'late-1'
  })
  bus.on(Event, async () => {
    await delay(200)
    return 'late-2'
  })

  const started_at = Date.now()
  const event = bus.emit(Event({ event_timeout: 0.01 }))
  await event.wait()
  const elapsed_ms = Date.now() - started_at

  const initial_snapshot = Array.from(event.event_results.values()).map((result) => ({
    id: result.id,
    status: result.status,
    error_name: (result.error as { constructor?: { name?: string } } | undefined)?.constructor?.name ?? null,
  }))

  assert.ok(elapsed_ms < 100, `event.now() took too long after timeout: ${elapsed_ms}ms`)
  assert.equal(
    initial_snapshot.every((result) => result.status === 'error'),
    true
  )

  await delay(250)
  const final_snapshot = Array.from(event.event_results.values()).map((result) => ({
    id: result.id,
    status: result.status,
    error_name: (result.error as { constructor?: { name?: string } } | undefined)?.constructor?.name ?? null,
  }))

  assert.deepEqual(final_snapshot, initial_snapshot)
  bus.destroy()
})

test('timeout/cancel/abort/result-schema taxonomy remains explicit', async () => {
  const SchemaEvent = BaseEvent.extend('MiddlewareSchemaEvent', {
    event_result_type: Number,
  })
  const serial_bus = new EventBus('MiddlewareTaxonomySerialBus', {
    event_handler_concurrency: 'serial',
  })
  const parallel_bus = new EventBus('MiddlewareTaxonomyParallelBus', {
    event_handler_concurrency: 'parallel',
  })

  serial_bus.on(SchemaEvent, async () => JSON.parse('"not-a-number"'))
  const schema_event = serial_bus.emit(SchemaEvent({ event_timeout: 0.2 }))
  await schema_event.wait()
  const schema_result = Array.from(schema_event.event_results.values())[0]
  assert.ok(schema_result.error instanceof EventHandlerResultSchemaError)

  const SerialTimeoutEvent = BaseEvent.extend('MiddlewareSerialTimeoutEvent', {})
  serial_bus.on(SerialTimeoutEvent, async () => {
    await delay(100)
    return 'slow'
  })
  serial_bus.on(SerialTimeoutEvent, async () => {
    await delay(100)
    return 'slow-2'
  })
  const serial_timeout_event = serial_bus.emit(SerialTimeoutEvent({ event_timeout: 0.01 }))
  await serial_timeout_event.wait()
  const serial_results = Array.from(serial_timeout_event.event_results.values())
  assert.equal(
    serial_results.some((result) => result.error instanceof EventHandlerCancelledError),
    true
  )
  assert.equal(
    serial_results.some((result) => result.error instanceof EventHandlerAbortedError || result.error instanceof EventHandlerTimeoutError),
    true
  )

  const ParallelTimeoutEvent = BaseEvent.extend('MiddlewareParallelTimeoutEvent', {})
  parallel_bus.on(ParallelTimeoutEvent, async () => {
    await delay(100)
    return 'slow'
  })
  parallel_bus.on(ParallelTimeoutEvent, async () => {
    await delay(100)
    return 'slow-2'
  })
  const parallel_timeout_event = parallel_bus.emit(ParallelTimeoutEvent({ event_timeout: 0.01 }))
  await parallel_timeout_event.wait()
  const parallel_results = Array.from(parallel_timeout_event.event_results.values())
  assert.equal(
    parallel_results.some((result) => result.error instanceof EventHandlerAbortedError || result.error instanceof EventHandlerTimeoutError),
    true
  )
  assert.equal(
    parallel_results.some((result) => result.error instanceof EventHandlerCancelledError),
    false
  )

  serial_bus.destroy()
  parallel_bus.destroy()
})

test('middleware hooks cover class/string/wildcard handler patterns', async () => {
  const event_statuses_by_id = new Map<string, string[]>()
  const result_hook_statuses_by_handler = new Map<string, string[]>()
  const result_runtime_statuses_by_handler = new Map<string, string[]>()
  const handler_change_records: HandlerChangeRecord[] = []

  class PatternRecordingMiddleware implements EventBusMiddleware {
    async onEventChange(_eventbus: EventBus, event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      const statuses = event_statuses_by_id.get(event.event_id) ?? []
      statuses.push(status)
      event_statuses_by_id.set(event.event_id, statuses)
    }

    async onEventResultChange(
      _eventbus: EventBus,
      _event: BaseEvent,
      event_result: { handler_id: string; status: string },
      status: 'pending' | 'started' | 'completed'
    ): Promise<void> {
      const hook_statuses = result_hook_statuses_by_handler.get(event_result.handler_id) ?? []
      hook_statuses.push(status)
      result_hook_statuses_by_handler.set(event_result.handler_id, hook_statuses)

      const runtime_statuses = result_runtime_statuses_by_handler.get(event_result.handler_id) ?? []
      runtime_statuses.push(event_result.status)
      result_runtime_statuses_by_handler.set(event_result.handler_id, runtime_statuses)
    }

    async onBusHandlersChange(
      _eventbus: EventBus,
      handler: { id: string; event_pattern: string; eventbus_id: string },
      registered: boolean
    ): Promise<void> {
      handler_change_records.push({
        handler_id: handler.id,
        event_pattern: handler.event_pattern,
        registered,
        eventbus_id: handler.eventbus_id,
      })
    }
  }

  const bus = new EventBus('MiddlewareHookPatternParityBus', {
    middlewares: [new PatternRecordingMiddleware()],
  })
  const PatternEvent = BaseEvent.extend('MiddlewarePatternEvent', {})

  const class_entry = bus.on(PatternEvent, async () => 'class-result')
  const string_entry = bus.on('MiddlewarePatternEvent', async () => 'string-result')
  const wildcard_entry = bus.on('*', async (event) => `wildcard:${event.event_type}`)

  await flushHooks()

  const registered_records = handler_change_records.filter((record) => record.registered)
  assert.equal(registered_records.length, 3)

  const expected_patterns = new Map<string, string>([
    [class_entry.id, 'MiddlewarePatternEvent'],
    [string_entry.id, 'MiddlewarePatternEvent'],
    [wildcard_entry.id, '*'],
  ])

  assert.deepEqual(new Set(registered_records.map((record) => record.handler_id)), new Set(expected_patterns.keys()))
  for (const record of registered_records) {
    assert.equal(record.event_pattern, expected_patterns.get(record.handler_id))
    assert.equal(record.eventbus_id, bus.id)
  }

  const event = bus.emit(PatternEvent({ event_timeout: 0.2 }))
  await event.now()
  await bus.waitUntilIdle()
  await flushHooks()

  assert.equal(event.event_status, 'completed')
  assert.deepEqual(event_statuses_by_id.get(event.event_id), ['pending', 'started', 'completed'])
  assert.deepEqual(new Set(event.event_results.keys()), new Set(expected_patterns.keys()))

  for (const handler_id of expected_patterns.keys()) {
    assert.deepEqual(result_hook_statuses_by_handler.get(handler_id), ['pending', 'started', 'completed'])
    assert.deepEqual(result_runtime_statuses_by_handler.get(handler_id), ['pending', 'started', 'completed'])
  }

  assert.equal(event.event_results.get(class_entry.id)?.result, 'class-result')
  assert.equal(event.event_results.get(string_entry.id)?.result, 'string-result')
  assert.equal(event.event_results.get(wildcard_entry.id)?.result, 'wildcard:MiddlewarePatternEvent')

  bus.off(PatternEvent, class_entry)
  bus.off('MiddlewarePatternEvent', string_entry)
  bus.off('*', wildcard_entry)
  await flushHooks()

  const unregistered_records = handler_change_records.filter((record) => !record.registered)
  assert.equal(unregistered_records.length, 3)
  assert.deepEqual(new Set(unregistered_records.map((record) => record.handler_id)), new Set(expected_patterns.keys()))
  for (const record of unregistered_records) {
    assert.equal(record.event_pattern, expected_patterns.get(record.handler_id))
  }

  bus.destroy()
})

test('middleware hooks cover ad-hoc BaseEvent string + wildcard patterns', async () => {
  const event_statuses_by_id = new Map<string, string[]>()
  const result_hook_statuses_by_handler = new Map<string, string[]>()
  const result_runtime_statuses_by_handler = new Map<string, string[]>()
  const handler_change_records: HandlerChangeRecord[] = []

  class PatternRecordingMiddleware implements EventBusMiddleware {
    async onEventChange(_eventbus: EventBus, event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      const statuses = event_statuses_by_id.get(event.event_id) ?? []
      statuses.push(status)
      event_statuses_by_id.set(event.event_id, statuses)
    }

    async onEventResultChange(
      _eventbus: EventBus,
      _event: BaseEvent,
      event_result: { handler_id: string; status: string },
      status: 'pending' | 'started' | 'completed'
    ): Promise<void> {
      const hook_statuses = result_hook_statuses_by_handler.get(event_result.handler_id) ?? []
      hook_statuses.push(status)
      result_hook_statuses_by_handler.set(event_result.handler_id, hook_statuses)

      const runtime_statuses = result_runtime_statuses_by_handler.get(event_result.handler_id) ?? []
      runtime_statuses.push(event_result.status)
      result_runtime_statuses_by_handler.set(event_result.handler_id, runtime_statuses)
    }

    async onBusHandlersChange(
      _eventbus: EventBus,
      handler: { id: string; event_pattern: string; eventbus_id: string },
      registered: boolean
    ): Promise<void> {
      handler_change_records.push({
        handler_id: handler.id,
        event_pattern: handler.event_pattern,
        registered,
        eventbus_id: handler.eventbus_id,
      })
    }
  }

  const bus = new EventBus('MiddlewareHookStringPatternParityBus', {
    middlewares: [new PatternRecordingMiddleware()],
  })

  const ad_hoc_event_type = 'AdHocPatternEvent'
  const string_entry = bus.on(ad_hoc_event_type, async (event) => {
    assert.equal(event.event_type, ad_hoc_event_type)
    return `string:${event.event_type}`
  })
  const wildcard_entry = bus.on('*', async (event) => `wildcard:${event.event_type}`)

  await flushHooks()

  const registered_records = handler_change_records.filter((record) => record.registered)
  assert.equal(registered_records.length, 2)

  const expected_patterns = new Map<string, string>([
    [string_entry.id, ad_hoc_event_type],
    [wildcard_entry.id, '*'],
  ])

  assert.deepEqual(new Set(registered_records.map((record) => record.handler_id)), new Set(expected_patterns.keys()))
  for (const record of registered_records) {
    assert.equal(record.event_pattern, expected_patterns.get(record.handler_id))
    assert.equal(record.eventbus_id, bus.id)
  }

  const event = bus.emit(new BaseEvent({ event_type: ad_hoc_event_type, event_timeout: 0.2 }))
  await event.now()
  await bus.waitUntilIdle()
  await flushHooks()

  assert.equal(event.event_status, 'completed')
  assert.deepEqual(event_statuses_by_id.get(event.event_id), ['pending', 'started', 'completed'])
  assert.deepEqual(new Set(event.event_results.keys()), new Set(expected_patterns.keys()))

  for (const handler_id of expected_patterns.keys()) {
    assert.deepEqual(result_hook_statuses_by_handler.get(handler_id), ['pending', 'started', 'completed'])
    assert.deepEqual(result_runtime_statuses_by_handler.get(handler_id), ['pending', 'started', 'completed'])
  }

  assert.equal(event.event_results.get(string_entry.id)?.result, `string:${ad_hoc_event_type}`)
  assert.equal(event.event_results.get(wildcard_entry.id)?.result, `wildcard:${ad_hoc_event_type}`)

  bus.off(ad_hoc_event_type, string_entry)
  bus.off('*', wildcard_entry)
  await flushHooks()

  const unregistered_records = handler_change_records.filter((record) => !record.registered)
  assert.equal(unregistered_records.length, 2)
  assert.deepEqual(new Set(unregistered_records.map((record) => record.handler_id)), new Set(expected_patterns.keys()))
  for (const record of unregistered_records) {
    assert.equal(record.event_pattern, expected_patterns.get(record.handler_id))
  }

  bus.destroy()
})

// Folded from EventBus_retry_integration.test.ts to keep test layout class-based.
class NetworkError extends Error {
  constructor(message: string = 'network error') {
    super(message)
    this.name = 'NetworkError'
  }
}

class ValidationError extends Error {
  constructor(message: string = 'validation error') {
    super(message)
    this.name = 'ValidationError'
  }
}

// ─── Integration with EventBus ───────────────────────────────────────────────

test('retry: works as event bus handler wrapper (inline HOF)', async () => {
  const bus = new EventBus('RetryBus', { event_timeout: 0 })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3 })(async (_event) => {
      calls++
      if (calls < 3) throw new Error(`handler fail ${calls}`)
      return 'handler ok'
    })
  )

  const event = bus.emit(TestEvent({}))
  await event.now()

  assert.equal(calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'handler ok')
})

test('retry: bus handler with retry_on_errors only retries matching errors (inline HOF)', async () => {
  const bus = new EventBus('RetryFilterBus', { event_timeout: 0 })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async (_event) => {
      calls++
      throw new ValidationError()
    })
  )

  const event = bus.emit(TestEvent({}))
  await event.now()
  await assert.rejects(event.eventResult(), (error: unknown) => error instanceof ValidationError)

  assert.equal(calls, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof ValidationError)
})

test('retry: @retry() decorated method works with bus.on via bind', async () => {
  const bus = new EventBus('DecoratorBus', { event_timeout: 0 })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  class Handler {
    calls = 0

    @retry({ max_attempts: 3 })
    async onTest(_event: InstanceType<typeof TestEvent>): Promise<string> {
      this.calls++
      if (this.calls < 3) throw new Error('handler fail')
      return 'handler ok'
    }
  }

  const handler = new Handler()
  bus.on(TestEvent, handler.onTest.bind(handler))

  const event = bus.emit(TestEvent({}))
  await event.now()
  assert.equal(handler.calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.result, 'handler ok')
})

// ─── @retry() decorator + bus.on via .bind(this) — all three scopes ─────────

test('retry: @retry(scope=class) + bus.on via .bind — serializes across instances', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeClassBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeClassEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'class', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.now()
  assert.equal(max_active, 1, 'class scope should serialize across instances')
})

test('retry: @retry(scope=instance) + bus.on via .bind — isolates per instance', async () => {
  const bus = new EventBus('ScopeInstanceBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeInstanceEvent', {})

  let active = 0
  let max_active = 0
  let total_calls = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: 'on_SomeEvent_inst' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      total_calls++
      await delay(200)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.now()

  assert.equal(total_calls, 2, 'both handlers should have run')
  assert.equal(
    max_active,
    2,
    `instance scope should allow different instances to run in parallel (got max_active=${max_active}, total_calls=${total_calls})`
  )
})

test('retry: @retry(scope=global) + bus.on via .bind — all calls share one semaphore', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeGlobalBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeGlobalEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'global', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.now()
  assert.equal(max_active, 1, 'global scope should serialize all calls')
})

// ─── HOF pattern: retry({...})(fn).bind(instance) — alternative to decorator ─

test('retry: HOF retry()(fn).bind(instance) — instance scope works when bind is after wrap', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('HOFBindBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('HOFBindEvent', {})

  let active = 0
  let max_active = 0

  const some_instance_a = { name: 'a' }
  const some_instance_b = { name: 'b' }

  const handler = retry({
    max_attempts: 1,
    semaphore_scope: 'instance',
    semaphore_limit: 1,
    semaphore_name: 'handler',
  })(async function (this: any, _event: InstanceType<typeof SomeEvent>): Promise<string> {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  bus.on(SomeEvent, handler.bind(some_instance_a))
  bus.on(SomeEvent, handler.bind(some_instance_b))

  const event = bus.emit(SomeEvent({}))
  await event.now()
  assert.equal(max_active, 2, 'bind-after-wrap: different instances should run in parallel')
})

// ─── retry wrapping emit→done (TECHNICALLY SUPPORTED, NOT RECOMMENDED) ──────

test('retry: retry wrapping emit→done retries the full dispatch cycle (discouraged pattern)', async () => {
  const bus = new EventBus('RetryEmitBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })

  const TabsEvent = BaseEvent.extend('TabsEvent', {})
  const DOMEvent = BaseEvent.extend('DOMEvent', {})
  const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', {})

  let tabs_attempts = 0
  let dom_calls = 0
  let screenshot_calls = 0

  bus.on(TabsEvent, async (_event) => {
    tabs_attempts++
    if (tabs_attempts < 3) throw new Error(`tabs fail attempt ${tabs_attempts}`)
    return 'tabs ok'
  })

  bus.on(DOMEvent, async (_event) => {
    dom_calls++
    return 'dom ok'
  })

  bus.on(ScreenshotEvent, async (_event) => {
    screenshot_calls++
    return 'screenshot ok'
  })

  const [tabs_event, dom_event, screenshot_event] = await Promise.all([
    retry({ max_attempts: 4 })(async () => {
      const event = bus.emit(TabsEvent({}))
      await event.now()
      if (event.event_errors.length) throw event.event_errors[0]
      return event
    })(),
    bus.emit(DOMEvent({})).now(),
    bus.emit(ScreenshotEvent({})).now(),
  ])

  assert.equal(tabs_attempts, 3)
  assert.equal(tabs_event.event_status, 'completed')
  assert.equal(dom_calls, 1)
  assert.equal(screenshot_calls, 1)
  assert.equal(dom_event.event_status, 'completed')
  assert.equal(screenshot_event.event_status, 'completed')
})

// Folded from optional_dependencies.test.ts to keep test layout class-based.
const tests_dir = dirname(fileURLToPath(import.meta.url))
const ts_root = join(tests_dir, '..')
const package_json_path = join(ts_root, 'package.json')
const src_dir = join(ts_root, 'src')

type PackageJSON = {
  dependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
  devDependencies?: Record<string, string>
  peerDependencies?: Record<string, string>
  peerDependenciesMeta?: Record<string, { optional?: boolean }>
  exports?: Record<string, string | Record<string, string>>
}

const loadPackageJson = (): PackageJSON => JSON.parse(readFileSync(package_json_path, 'utf8')) as PackageJSON

test('bridge and optional middleware dependencies are optional peers in package.json', () => {
  const package_json = loadPackageJson()
  const dependencies = package_json.dependencies ?? {}
  const peer_dependencies = package_json.peerDependencies ?? {}
  const peer_dependencies_meta = package_json.peerDependenciesMeta ?? {}

  for (const package_name of [
    'ioredis',
    'nats',
    'pg',
    '@tachyon-ipc/core',
    '@opentelemetry/api',
    '@opentelemetry/exporter-trace-otlp-http',
    '@opentelemetry/resources',
    '@opentelemetry/sdk-trace-base',
  ]) {
    assert.equal(Object.hasOwn(dependencies, package_name), false)
    assert.equal(Object.hasOwn(peer_dependencies, package_name), true)
    assert.equal(peer_dependencies_meta[package_name]?.optional, true)
  }
})

test('package.json does not depend on third-party sqlite packages', () => {
  const package_json = loadPackageJson()
  const sqlite_package_names = ['sqlite3', 'better-sqlite3', '@sqlite.org/sqlite-wasm']
  const dependency_sections = [
    package_json.dependencies ?? {},
    package_json.optionalDependencies ?? {},
    package_json.devDependencies ?? {},
    package_json.peerDependencies ?? {},
  ]

  for (const section of dependency_sections) {
    for (const sqlite_package_name of sqlite_package_names) {
      assert.equal(Object.hasOwn(section, sqlite_package_name), false, `unexpected sqlite package: ${sqlite_package_name}`)
    }
  }
})

test('package.json exposes source subpaths for direct TypeScript source imports', () => {
  const package_json = loadPackageJson()
  const exports_map = package_json.exports ?? {}

  assert.deepEqual(exports_map['./source'], {
    source: './src/index.ts',
    types: './src/index.ts',
    import: './src/index.ts',
    default: './src/index.ts',
  })
  assert.deepEqual(exports_map['./source/*'], {
    source: './src/*.ts',
    types: './src/*.ts',
    import: './src/*.ts',
    default: './src/*.ts',
  })
})

test('bridge and middleware implementation filenames match exported class names', () => {
  const source_files = new Set(readdirSync(src_dir))

  for (const filename of [
    'EventBridge.ts',
    'HTTPEventBridge.ts',
    'SocketEventBridge.ts',
    'JSONLEventBridge.ts',
    'SQLiteEventBridge.ts',
    'RedisEventBridge.ts',
    'NATSEventBridge.ts',
    'PostgresEventBridge.ts',
    'TachyonEventBridge.ts',
    'EventBusMiddleware.ts',
    'OtelTracingMiddleware.ts',
  ]) {
    assert.equal(source_files.has(filename), true, `missing class-named implementation file: ${filename}`)
  }

  for (const filename of source_files) {
    assert.equal(/^bridge_|^middleware_/.test(filename), false, `implementation file should use class name: ${filename}`)
  }
})

test('bridge modules do not statically import optional bridge packages', () => {
  const bridge_modules = [
    {
      path: join(src_dir, 'RedisEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]ioredis['"]/, /import\s+['"]ioredis['"]/],
      required_pattern: /importOptionalDependency\('RedisEventBridge', 'ioredis'\)/,
    },
    {
      path: join(src_dir, 'NATSEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]nats['"]/, /import\s+['"]nats['"]/],
      required_pattern: /importOptionalDependency\('NATSEventBridge', 'nats'\)/,
    },
    {
      path: join(src_dir, 'PostgresEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]pg['"]/, /import\s+['"]pg['"]/],
      required_pattern: /importOptionalDependency\('PostgresEventBridge', 'pg'\)/,
    },
    {
      path: join(src_dir, 'TachyonEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]@tachyon-ipc\/core['"]/, /import\s+['"]@tachyon-ipc\/core['"]/],
      required_pattern: /assertOptionalDependencyAvailable\('TachyonEventBridge', '@tachyon-ipc\/core'\)/,
    },
  ]

  for (const bridge_module of bridge_modules) {
    const source = readFileSync(bridge_module.path, 'utf8')
    for (const forbidden_pattern of bridge_module.forbidden_patterns) {
      assert.equal(forbidden_pattern.test(source), false, `${bridge_module.path} has eager optional dependency import`)
    }
    assert.equal(bridge_module.required_pattern.test(source), true, `${bridge_module.path} must use lazy optional dependency import`)
  }
})

test('root import excludes peer-backed integrations while namespaced imports resolve them', async () => {
  const root_source = readFileSync(join(src_dir, 'index.ts'), 'utf8')
  for (const forbidden_import of [
    'PostgresEventBridge',
    'RedisEventBridge',
    'NATSEventBridge',
    'TachyonEventBridge',
    'OtelTracingMiddleware',
    '@opentelemetry',
    'ioredis',
    'nats',
    'pg',
    '@tachyon-ipc/core',
  ]) {
    assert.equal(root_source.includes(forbidden_import), false, `root index pulls in ${forbidden_import}`)
  }
  assert.equal(root_source.includes('EventBusMiddleware'), true, 'root index should type-export base middleware types')

  const root = await import('../src/index.js')
  for (const root_export of ['EventBus', 'EventBridge', 'HTTPEventBridge', 'JSONLEventBridge', 'SQLiteEventBridge']) {
    assert.equal(root_export in root, true, `missing root export ${root_export}`)
  }
  for (const peer_export of ['PostgresEventBridge', 'RedisEventBridge', 'NATSEventBridge', 'TachyonEventBridge', 'OtelTracingMiddleware']) {
    assert.equal(peer_export in root, false, `peer-backed export leaked into root: ${peer_export}`)
  }

  const bridges = await import('../src/bridges.js')
  const otel = await import('../src/OtelTracingMiddleware.js')
  assert.equal(typeof bridges.PostgresEventBridge, 'function')
  assert.equal(typeof bridges.TachyonEventBridge, 'function')
  assert.equal(typeof otel.OtelTracingMiddleware, 'function')
})

// Folded from OtelTracingMiddleware.test.ts to keep test layout class-based.
function timeInputToNs(value: TimeInput | undefined): bigint | undefined {
  if (value === undefined) {
    return undefined
  }
  if (value instanceof Date) {
    return BigInt(value.getTime()) * 1_000_000n
  }
  if (Array.isArray(value)) {
    return BigInt(value[0]) * 1_000_000_000n + BigInt(value[1])
  }
  return BigInt(Math.trunc(value * 1_000_000))
}

type RecordingContext = {
  parent: Context
  span: RecordingSpan
}

class RecordingSpan implements Span {
  name: string
  options: SpanOptions | undefined
  parent_context: Context | undefined
  attributes: Record<string, SpanAttributeValue>
  status: SpanStatus | undefined
  exceptions: unknown[]
  ended: boolean
  end_time: TimeInput | undefined
  span_context: SpanContext
  parent_span_context: SpanContext | undefined

  constructor(
    name: string,
    options: SpanOptions | undefined,
    parent_context: Context | undefined,
    span_context: SpanContext = {
      traceId: '00000000000000000000000000000001',
      spanId: '0000000000000001',
      traceFlags: 1,
    },
    parent_span_context?: SpanContext
  ) {
    this.name = name
    this.options = options
    this.parent_context = parent_context
    this.span_context = span_context
    this.parent_span_context = parent_span_context
    this.attributes = {}
    for (const [key, value] of Object.entries(options?.attributes ?? {})) {
      if (value !== undefined) {
        this.attributes[key] = value
      }
    }
    this.exceptions = []
    this.ended = false
    this.end_time = undefined
  }

  spanContext(): SpanContext {
    return this.span_context
  }

  setAttribute(key: string, value: SpanAttributeValue): this {
    this.attributes[key] = value
    return this
  }

  setAttributes(attributes: SpanAttributes): this {
    for (const [key, value] of Object.entries(attributes)) {
      if (value !== undefined) {
        this.attributes[key] = value
      }
    }
    return this
  }

  addEvent(): this {
    return this
  }

  addLink(): this {
    return this
  }

  addLinks(): this {
    return this
  }

  setStatus(status: SpanStatus): this {
    this.status = status
    return this
  }

  updateName(name: string): this {
    this.name = name
    return this
  }

  end(end_time?: TimeInput): void {
    this.ended = true
    this.end_time = end_time
  }

  isRecording(): boolean {
    return true
  }

  recordException(exception: unknown): void {
    this.exceptions.push(exception)
  }
}

class RecordingTracer implements Tracer {
  spans: RecordingSpan[] = []

  startSpan(name: string, options?: SpanOptions, parent_context?: Context): Span {
    const span = new RecordingSpan(name, options, parent_context)
    this.spans.push(span)
    return span
  }

  startActiveSpan(): never {
    throw new Error('not implemented')
  }
}

test('OtelTracingMiddleware creates event and handler spans with child event parentage', async () => {
  const tracer = new RecordingTracer()
  const trace_api = {
    getTracer: () => tracer,
    setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
  }
  const bus = new EventBus('OtelTracingBus', {
    middlewares: [new OtelTracingMiddleware({ tracer, trace_api })],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).now()
    return 'parent'
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ event_timeout: 0.5 })).now()
  await flushHooks()

  const parent_event_span = tracer.spans.find((span) => span.name === 'OtelTracingBus.emit(OtelTracingParentEvent)')
  const parent_handler_span = tracer.spans.find((span) => span.name === 'anonymous(OtelTracingParentEvent)')
  const child_event_span = tracer.spans.find((span) => span.name === 'OtelTracingBus.emit(OtelTracingChildEvent)')
  const child_handler_span = tracer.spans.find((span) => span.name === 'anonymous(OtelTracingChildEvent)')

  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)
  assert.equal(parent_event_span.parent_context, ROOT_CONTEXT)
  assert.equal(parent_event_span.ended, true)
  assert.equal(parent_handler_span.ended, true)
  assert.equal(child_event_span.ended, true)
  assert.equal(child_handler_span.ended, true)
  assert.equal(recordingContext(parent_handler_span.parent_context).span, parent_event_span)
  assert.equal(recordingContext(child_event_span.parent_context).span, parent_handler_span)
  assert.equal(recordingContext(child_handler_span.parent_context).span, child_event_span)

  bus.destroy()
})

test('OtelTracingMiddleware names event and handler spans for display', async () => {
  const tracer = new RecordingTracer()
  const trace_api = {
    getTracer: () => tracer,
    setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
  }
  const bus = new EventBus('StagehandExtensionBackground', {
    middlewares: [new OtelTracingMiddleware({ tracer, trace_api })],
    max_history_size: null,
  })
  const CDPConnect = BaseEvent.extend('CDPConnect', {})

  bus.on(CDPConnect, () => 'connected', { handler_name: 'DebuggerClient.on_CDPConnect' })

  await bus.emit(CDPConnect({ event_timeout: 0.2 })).now()
  await flushHooks()

  assert.ok(tracer.spans.find((span) => span.name === 'StagehandExtensionBackground.emit(CDPConnect)'))
  assert.ok(tracer.spans.find((span) => span.name === 'DebuggerClient.on_CDPConnect(CDPConnect)'))

  bus.destroy()
})

test('OtelTracingMiddleware marks top-level event spans as roots with session attributes and non-zero duration', async () => {
  const tracer = new RecordingTracer()
  const trace_api = {
    getTracer: () => tracer,
    setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
  }
  const bus = new EventBus('OtelTracingSessionBus', {
    middlewares: [
      new OtelTracingMiddleware({
        tracer,
        trace_api,
        root_span_attributes: { 'stagehand.session_id': 'session-123' },
      }),
    ],
  })
  const InstantEvent = BaseEvent.extend('OtelTracingInstantEvent', {})

  await bus.emit(InstantEvent({ session_id: 'event-session-456', event_timeout: 0.2 } as any)).wait()
  await flushHooks()

  const event_span = tracer.spans.find((span) => span.name === 'OtelTracingSessionBus.emit(OtelTracingInstantEvent)')

  assert.ok(event_span)
  assert.equal(event_span.parent_context, ROOT_CONTEXT)
  assert.equal(event_span.attributes['stagehand.session_id'], 'session-123')
  assert.equal(event_span.attributes['abxbus.session_id'], 'event-session-456')
  assert.equal(event_span.attributes['abxbus.trace.root'], true)
  const start_time = timeInputToNs(event_span.options?.startTime)
  const end_time = timeInputToNs(event_span.end_time)
  assert.ok(start_time !== undefined)
  assert.ok(end_time !== undefined)
  assert.ok(end_time > start_time)

  bus.destroy()
})

test('OtelTracingMiddleware span_factory mirrors abxbus ids into stable parent and child span contexts', async () => {
  const spans: RecordingSpan[] = []
  const span_factory = (input: OtelTracingSpanFactoryInput): Span => {
    const span = new RecordingSpan(
      input.name,
      {
        attributes: input.attributes,
        startTime: input.start_time,
      },
      undefined,
      input.span_context,
      input.parent_span_context
    )
    spans.push(span)
    return span
  }
  const bus = new EventBus('OtelTracingManualIdsBus', {
    middlewares: [
      new OtelTracingMiddleware({
        span_factory,
        root_span_attributes: { 'stagehand.session_id': 'session-abc' },
      }),
    ],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingManualParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingManualChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).now()
    return 'parent'
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ session_id: 'session-abc', event_timeout: 0.5 } as any)).now()
  await flushHooks()

  const parent_event_span = spans.find((span) => span.name === 'OtelTracingManualIdsBus.emit(OtelTracingManualParentEvent)')
  const parent_handler_span = spans.find((span) => span.name === 'anonymous(OtelTracingManualParentEvent)')
  const child_event_span = spans.find((span) => span.name === 'OtelTracingManualIdsBus.emit(OtelTracingManualChildEvent)')
  const child_handler_span = spans.find((span) => span.name === 'anonymous(OtelTracingManualChildEvent)')

  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)
  assert.deepEqual(
    spans.map((span) => span.name),
    [
      'OtelTracingManualIdsBus.emit(OtelTracingManualParentEvent)',
      'anonymous(OtelTracingManualParentEvent)',
      'OtelTracingManualIdsBus.emit(OtelTracingManualChildEvent)',
      'anonymous(OtelTracingManualChildEvent)',
    ]
  )

  const trace_id = parent_event_span.spanContext().traceId
  assert.match(trace_id, /^[0-9a-f]{32}$/)
  assert.equal(parent_handler_span.spanContext().traceId, trace_id)
  assert.equal(child_event_span.spanContext().traceId, trace_id)
  assert.equal(child_handler_span.spanContext().traceId, trace_id)

  assert.equal(parent_event_span.parent_span_context, undefined)
  assert.equal(parent_event_span.attributes['stagehand.session_id'], 'session-abc')
  assert.equal(parent_event_span.attributes['abxbus.trace.root'], true)
  assert.equal(parent_handler_span.parent_span_context?.spanId, parent_event_span.spanContext().spanId)
  assert.equal(child_event_span.parent_span_context?.spanId, parent_handler_span.spanContext().spanId)
  assert.equal(child_handler_span.parent_span_context?.spanId, child_event_span.spanContext().spanId)

  const span_ids = new Set(spans.map((span) => span.spanContext().spanId))
  assert.equal(span_ids.size, spans.length)
  assert.ok([...span_ids].every((span_id) => /^[0-9a-f]{16}$/.test(span_id) && span_id !== '0000000000000000'))
  assert.equal(parent_event_span.attributes['abxbus.event_parent_id'], undefined)
  assert.equal(child_event_span.attributes['abxbus.event_parent_id'], parent_event_span.attributes['abxbus.event_id'])
  assert.equal(child_event_span.attributes['abxbus.event_emitted_by_handler_id'], parent_handler_span.attributes['abxbus.handler_id'])

  bus.destroy()
})

test('OtelTracingMiddleware span_factory waits until top-level event completion before exporting a trace tree', async () => {
  const spans: RecordingSpan[] = []
  const span_factory = (input: OtelTracingSpanFactoryInput): Span => {
    const span = new RecordingSpan(
      input.name,
      {
        attributes: input.attributes,
        startTime: input.start_time,
      },
      undefined,
      input.span_context,
      input.parent_span_context
    )
    spans.push(span)
    return span
  }
  const bus = new EventBus('OtelTracingRootStartBus', {
    middlewares: [
      new OtelTracingMiddleware({
        span_factory,
      }),
    ],
    max_history_size: null,
  })
  const RootEvent = BaseEvent.extend('OtelTracingRootStartEvent', {})
  let release!: () => void
  const gate = new Promise<void>((resolve) => {
    release = resolve
  })

  bus.on(RootEvent, async () => {
    await gate
    return 'done'
  })

  const emission = bus.emit(RootEvent({ event_timeout: 0.5 }))
  const completion = emission.now()
  await flushHooks()

  assert.equal(spans.length, 0)

  release()
  await completion
  await flushHooks()

  const event_span = spans.find((span) => span.name === 'OtelTracingRootStartBus.emit(OtelTracingRootStartEvent)')
  const handler_span = spans.find((span) => span.name === 'anonymous(OtelTracingRootStartEvent)')
  assert.ok(event_span)
  assert.ok(handler_span)
  assert.deepEqual(
    spans.map((span) => span.name),
    ['OtelTracingRootStartBus.emit(OtelTracingRootStartEvent)', 'anonymous(OtelTracingRootStartEvent)']
  )
  assert.equal(event_span.parent_span_context, undefined)
  assert.equal(event_span.attributes['abxbus.trace.root'], true)
  assert.equal(handler_span.parent_span_context?.spanId, event_span.spanContext().spanId)

  bus.destroy()
})

test('OtelTracingMiddleware span_provider creates SDK spans with abxbus span contexts', async () => {
  const exported_spans: ReadableSpan[] = []
  const exporter: SpanExporter = {
    export(spans, resultCallback) {
      exported_spans.push(...spans)
      resultCallback({ code: 0 })
    },
    shutdown: async () => undefined,
  }
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  })
  const bus = new EventBus('OtelTracingProviderBus', {
    middlewares: [
      new OtelTracingMiddleware({
        span_provider: provider,
        instrumentation_name: 'abxbus-test',
      }),
    ],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingProviderParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingProviderChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).now()
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ event_timeout: 0.5 })).now()
  await provider.forceFlush()
  await flushHooks()

  const parent_event_span = exported_spans.find((span) => span.name === 'OtelTracingProviderBus.emit(OtelTracingProviderParentEvent)')
  const parent_handler_span = exported_spans.find((span) => span.name === 'anonymous(OtelTracingProviderParentEvent)')
  const child_event_span = exported_spans.find((span) => span.name === 'OtelTracingProviderBus.emit(OtelTracingProviderChildEvent)')
  const child_handler_span = exported_spans.find((span) => span.name === 'anonymous(OtelTracingProviderChildEvent)')

  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)
  assert.equal(parent_event_span.kind, SpanKind.INTERNAL)
  assert.equal(parent_event_span.parentSpanContext, undefined)
  assert.equal(parent_event_span.attributes['abxbus.trace.root'], true)
  assert.equal(parent_handler_span.parentSpanContext?.spanId, parent_event_span.spanContext().spanId)
  assert.equal(child_event_span.parentSpanContext?.spanId, parent_handler_span.spanContext().spanId)
  assert.equal(child_handler_span.parentSpanContext?.spanId, child_event_span.spanContext().spanId)
  assert.equal(child_event_span.attributes['abxbus.event_parent_id'], parent_event_span.attributes['abxbus.event_id'])
  assert.equal(child_event_span.attributes['abxbus.event_emitted_by_handler_id'], parent_handler_span.attributes['abxbus.handler_id'])
  for (const span of [parent_event_span, parent_handler_span, child_event_span, child_handler_span]) {
    const start_time = timeInputToNs(span.startTime)
    const end_time = timeInputToNs(span.endTime)
    assert.ok(start_time !== undefined)
    assert.ok(end_time !== undefined)
    assert.ok(end_time > start_time)
  }

  await provider.shutdown()
  bus.destroy()
})

test('OtelTracingMiddleware OTLP endpoint batches a completed trace tree into one export request', async (t) => {
  const payloads: Record<string, unknown>[] = []
  const server = http.createServer((request, response) => {
    const chunks: Buffer[] = []
    request.on('data', (chunk: Buffer) => chunks.push(chunk))
    request.on('end', () => {
      payloads.push(JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>)
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end('{}')
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => {
    server.close()
  })
  const address = server.address()
  assert.ok(address && typeof address === 'object')

  const bus = new EventBus('OtelTracingOtlpBatchBus', {
    middlewares: [
      new OtelTracingMiddleware({
        otlp_endpoint: `http://127.0.0.1:${address.port}/v1/traces`,
      }),
    ],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingOtlpBatchParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingOtlpBatchChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).now()
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ event_timeout: 0.5 })).now()
  for (let index = 0; index < 20 && payloads.length === 0; index += 1) {
    await new Promise((resolve) => setTimeout(resolve, 25))
  }

  assert.equal(payloads.length, 1)
  const resource_spans = payloads[0].resourceSpans as Array<Record<string, unknown>>
  const scope_spans = resource_spans.flatMap((resource_span) => resource_span.scopeSpans as Array<Record<string, unknown>>)
  const spans = scope_spans.flatMap((scope_span) => scope_span.spans as Array<Record<string, string | undefined>>)
  const parent_event_span = spans.find((span) => span.name === 'OtelTracingOtlpBatchBus.emit(OtelTracingOtlpBatchParentEvent)')
  const parent_handler_span = spans.find((span) => span.name === 'anonymous(OtelTracingOtlpBatchParentEvent)')
  const child_event_span = spans.find((span) => span.name === 'OtelTracingOtlpBatchBus.emit(OtelTracingOtlpBatchChildEvent)')
  const child_handler_span = spans.find((span) => span.name === 'anonymous(OtelTracingOtlpBatchChildEvent)')

  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)
  assert.equal(parent_event_span.parentSpanId, undefined)
  assert.equal(parent_handler_span.parentSpanId, parent_event_span.spanId)
  assert.equal(child_event_span.parentSpanId, parent_handler_span.spanId)
  assert.equal(child_handler_span.parentSpanId, child_event_span.spanId)
  assert.ok(spans.every((span) => span.traceId === parent_event_span.traceId))

  bus.destroy()
})

test('OtelTracingMiddleware accepts OTLP endpoint constructor options', () => {
  const middleware = new OtelTracingMiddleware({
    otlp_endpoint: 'http://localhost:4318',
    service_name: 'stagehand-driver',
    instrumentation_name: 'stagehand-driver.abxbus',
  })
  assert.ok(middleware)
})

function recordingContext(value: Context | undefined): RecordingContext {
  assert.ok(value)
  return value as unknown as RecordingContext
}

test('OtelTracingMiddleware records handler errors on handler and event spans', async () => {
  const tracer = new RecordingTracer()
  const bus = new EventBus('OtelTracingErrorBus', {
    middlewares: [
      new OtelTracingMiddleware({
        tracer,
        trace_api: {
          getTracer: () => tracer,
          setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
        },
      }),
    ],
  })
  const ErrorEvent = BaseEvent.extend('OtelTracingErrorEvent', {})
  const error = new Error('handler failed')

  bus.on(ErrorEvent, () => {
    throw error
  })

  await bus.emit(ErrorEvent({ event_timeout: 0.2 })).wait()
  await flushHooks()

  const event_span = tracer.spans.find((span) => span.name === 'OtelTracingErrorBus.emit(OtelTracingErrorEvent)')
  const handler_span = tracer.spans.find((span) => span.name === 'anonymous(OtelTracingErrorEvent)')

  assert.ok(event_span)
  assert.ok(handler_span)
  assert.equal(event_span.status?.code, 2)
  assert.equal(handler_span.status?.code, 2)
  assert.equal(event_span.exceptions[0], error)
  assert.equal(handler_span.exceptions[0], error)

  bus.destroy()
})
