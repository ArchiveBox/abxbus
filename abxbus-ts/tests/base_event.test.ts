import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, monotonicDatetime } from '../src/index.js'

test('BaseEvent lifecycle transitions are explicit and awaitable', async () => {
  const LifecycleEvent = BaseEvent.extend('BaseEventLifecycleTestEvent', {})

  const standalone = LifecycleEvent({})
  assert.equal(standalone.event_status, 'pending')
  assert.equal(standalone.event_started_at, null)
  assert.equal(standalone.event_completed_at, null)

  standalone._markStarted()
  assert.equal(standalone.event_status, 'started')
  assert.equal(typeof standalone.event_started_at, 'string')

  standalone._markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  assert.equal(typeof standalone.event_completed_at, 'string')
  await standalone.eventCompleted()
})

test('done() re-raises the first processing exception after completion', async () => {
  const ErrorEvent = BaseEvent.extend('BaseEventDoneRaisesFirstErrorEvent', {})
  const bus = new EventBus('BaseEventDoneRaisesFirstErrorBus', {
    event_handler_concurrency: 'parallel',
    event_timeout: null,
  })

  bus.on(ErrorEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 1))
    throw new Error('first failure')
  })

  bus.on(ErrorEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
    throw new Error('second failure')
  })

  const event = bus.emit(ErrorEvent({}))
  await assert.rejects(() => event.done(), /first failure/)

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_results.size, 2)
  assert.equal(
    Array.from(event.event_results.values()).every((result) => result.status === 'error'),
    true
  )
})

test('BaseEvent.eventResultUpdate creates and updates typed handler results', async () => {
  const TypedEvent = BaseEvent.extend('BaseEventEventResultUpdateEvent', { event_result_type: z.string() })
  const bus = new EventBus('BaseEventEventResultUpdateBus')
  const event = TypedEvent({})
  const handler_entry = bus.on(TypedEvent, async () => 'ok')

  const pending = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'pending' })
  assert.equal(event.event_results.get(handler_entry.id), pending)
  assert.equal(pending.status, 'pending')

  const completed = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'completed', result: 'seeded' })
  assert.equal(completed, pending)
  assert.equal(completed.status, 'completed')
  assert.equal(completed.result, 'seeded')

  bus.destroy()
})

test('BaseEvent.eventResultUpdate status-only update does not implicitly pass undefined result/error keys', () => {
  const TypedEvent = BaseEvent.extend('BaseEventEventResultUpdateStatusOnlyEvent', { event_result_type: z.string() })
  const bus = new EventBus('BaseEventEventResultUpdateStatusOnlyBus')
  const event = TypedEvent({})
  const handler_entry = bus.on(TypedEvent, async () => 'ok')

  const errored = event.eventResultUpdate(handler_entry, { eventbus: bus, error: new Error('seeded error') })
  assert.equal(errored.status, 'error')
  assert.ok(errored.error instanceof Error)

  const status_only = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'pending' })
  assert.equal(status_only.status, 'pending')
  assert.ok(status_only.error instanceof Error)
  assert.equal(status_only.result, undefined)

  bus.destroy()
})

test('await event.done() queue-jumps child processing inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventImmediateParentEvent', {})
  const ChildEvent = BaseEvent.extend('BaseEventImmediateChildEvent', {})
  const SiblingEvent = BaseEvent.extend('BaseEventImmediateSiblingEvent', {})

  const bus = new EventBus('BaseEventImmediateQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    event.bus?.emit(SiblingEvent({}))
    const child = event.bus?.emit(ChildEvent({}))
    assert.ok(child)
    await child.done()
    order.push('parent_end')
  })

  bus.on(ChildEvent, async () => {
    order.push('child')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling')
  })

  await bus.emit(ParentEvent({})).done()
  await bus.waitUntilIdle()

  assert.deepEqual(order, ['parent_start', 'child', 'parent_end', 'sibling'])
  bus.destroy()
})

test('await event.eventCompleted() preserves normal queue order inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventQueuedParentEvent', {})
  const ChildEvent = BaseEvent.extend('BaseEventQueuedChildEvent', {})
  const SiblingEvent = BaseEvent.extend('BaseEventQueuedSiblingEvent', {})

  const bus = new EventBus('BaseEventQueueOrderBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    event.bus?.emit(SiblingEvent({}))
    const child = event.bus?.emit(ChildEvent({}))
    assert.ok(child)
    await child.eventCompleted()
    order.push('parent_end')
  })

  bus.on(ChildEvent, async () => {
    order.push('child_start')
    await new Promise((resolve) => setTimeout(resolve, 1))
    order.push('child_end')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling_start')
    await new Promise((resolve) => setTimeout(resolve, 1))
    order.push('sibling_end')
  })

  await bus.emit(ParentEvent({})).done()
  await bus.waitUntilIdle()

  assert.ok(order.indexOf('sibling_start') < order.indexOf('child_start'))
  assert.ok(order.indexOf('child_end') < order.indexOf('parent_end'))
  bus.destroy()
})

test('monotonicDatetime emits parseable, monotonic ISO timestamps', () => {
  const first = monotonicDatetime()
  const second = monotonicDatetime()

  assert.equal(typeof first, 'string')
  assert.equal(typeof second, 'string')
  assert.equal(Number.isInteger(Date.parse(first)), true)
  assert.equal(Number.isInteger(Date.parse(second)), true)
  assert.ok(second > first)
})

test('BaseEvent rejects reserved runtime fields in payload and event shape', () => {
  const ReservedFieldEvent = BaseEvent.extend('BaseEventReservedFieldEvent', {})

  assert.throws(() => {
    void ReservedFieldEvent({ bus: 'payload_bus_field' } as unknown as never)
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFieldShapeEvent', { bus: z.string() })
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ first: 'payload_first_field' } as unknown as never)
  }, /field "first" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFirstShapeEvent', { first: z.string() })
  }, /field "first" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toString: 'payload_to_string_field' } as unknown as never)
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToStringShapeEvent', { toString: z.string() })
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toJSON: 'payload_to_json_field' } as unknown as never)
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToJSONShapeEvent', { toJSON: z.string() })
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ fromJSON: 'payload_from_json_field' } as unknown as never)
  }, /field "fromJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFromJSONShapeEvent', { fromJSON: z.string() })
  }, /field "fromJSON" is reserved/i)
})

test('BaseEvent rejects unknown event_* fields while allowing known event_* overrides', () => {
  const AllowedEvent = BaseEvent.extend('BaseEventAllowedEventConfigEvent', {
    event_timeout: 123,
    event_slow_timeout: 9,
    event_handler_timeout: 45,
    value: z.string(),
  })

  const event = AllowedEvent({ value: 'ok' })
  assert.equal(event.event_timeout, 123)
  assert.equal(event.event_slow_timeout, 9)
  assert.equal(event.event_handler_timeout, 45)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventUnknownEventShapeFieldEvent', { event_some_field_we_dont_recognize: 1 })
  }, /starts with "event_" but is not a recognized BaseEvent field/i)

  assert.throws(() => {
    void AllowedEvent({
      value: 'ok',
      event_some_field_we_dont_recognize: 1,
    } as unknown as never)
  }, /starts with "event_" but is not a recognized BaseEvent field/i)
})

test('BaseEvent rejects model_* fields in payload and event shape', () => {
  const ModelReservedEvent = BaseEvent.extend('BaseEventModelReservedEvent', {})

  assert.throws(() => {
    void BaseEvent.extend('BaseEventModelReservedShapeEvent', { model_something_random: 1 })
  }, /starts with "model_" and is reserved/i)

  assert.throws(() => {
    void ModelReservedEvent({ model_something_random: 1 } as unknown as never)
  }, /starts with "model_" and is reserved/i)
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('BaseEventRuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('BaseEventRuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.emit(RuntimeEvent({}))
  await event.done()

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_at, 'string')
  assert.equal(typeof json.event_started_at, 'string')
  assert.equal(typeof json.event_completed_at, 'string')
  assert.match(String(json.event_created_at), /Z$/)
  assert.match(String(json.event_started_at), /Z$/)
  assert.match(String(json.event_completed_at), /Z$/)
  assert.equal(json.event_pending_bus_count, 0)

  const json_results = json.event_results as Array<Record<string, unknown>>
  assert.equal(json_results.length, 1)
  assert.equal(json_results[0].status, 'completed')
  assert.equal(json_results[0].result, 'ok')
  assert.equal(typeof json_results[0].handler_registered_at, 'string')
  assert.match(String(json_results[0].handler_registered_at), /Z$/)

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_at, event.event_created_at)
  assert.equal(restored.event_results.size, 1)
  assert.equal(Array.from(restored.event_results.values())[0].result, 'ok')

  bus.destroy()
})

test('BaseEvent event_*_at fields are recognized and normalized', () => {
  const AtFieldEvent = BaseEvent.extend('BaseEventAtFieldRecognitionEvent', {})
  const event = AtFieldEvent({
    event_created_at: '2025-01-02T03:04:05.678901234Z',
    event_started_at: '2025-01-02T03:04:06.100000000Z',
    event_completed_at: '2025-01-02T03:04:07.200000000Z',
    event_slow_timeout: 1.5,
    event_emitted_by_handler_id: '018f8e40-1234-7000-8000-000000000301',
    event_pending_bus_count: 2,
  } as never)

  assert.equal(event.event_created_at, '2025-01-02T03:04:05.678901234Z')
  assert.equal(event.event_started_at, '2025-01-02T03:04:06.100000000Z')
  assert.equal(event.event_completed_at, '2025-01-02T03:04:07.200000000Z')
  assert.equal(event.event_slow_timeout, 1.5)
  assert.equal(event.event_emitted_by_handler_id, '018f8e40-1234-7000-8000-000000000301')
  assert.equal(event.event_pending_bus_count, 2)
})

test('BaseEvent reset returns a fresh pending event that can be redispatched', async () => {
  const ResetEvent = BaseEvent.extend('BaseEventResetEvent', {
    label: z.string(),
  })

  const bus_a = new EventBus('BaseEventResetBusA')
  const bus_b = new EventBus('BaseEventResetBusB')

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

  bus_a.destroy()
  bus_b.destroy()
})

test('BaseEvent fromJSON preserves nullable parent/emitted metadata', () => {
  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'BaseEventFromJsonNullFieldsEvent',
    event_parent_id: null,
    event_emitted_by_handler_id: null,
    event_timeout: null,
  })

  assert.equal(event.event_parent_id, null)
  assert.equal(event.event_emitted_by_handler_id, null)

  const roundtrip = event.toJSON() as Record<string, unknown>
  assert.equal(roundtrip.event_parent_id, null)
  assert.equal(roundtrip.event_emitted_by_handler_id, null)
})

test('BaseEvent status hooks capture bus reference before event gc', async () => {
  const HookEvent = BaseEvent.extend('BaseEventHookCaptureEvent', {})

  class HookCaptureBus extends EventBus {
    seen_statuses: string[] = []

    async onEventChange(_event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      this.seen_statuses.push(status)
    }
  }

  const bus = new HookCaptureBus('BaseEventHookCaptureBus')
  const event = HookEvent({})
  event.bus = bus

  event._markStarted()
  event._markCompleted()
  event._gc()

  assert.deepEqual(bus.seen_statuses, ['started', 'completed'])

  bus.destroy()
})
