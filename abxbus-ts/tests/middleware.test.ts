import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  BaseEvent,
  EventBus,
  EventHandlerAbortedError,
  EventHandlerCancelledError,
  EventHandlerResultSchemaError,
  EventHandlerTimeoutError,
  type EventBusMiddleware,
} from '../src/index.js'

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

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
  await bus.emit(Event({ event_timeout: 0.2 })).done()
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

  await bus_a.emit(Event({ event_timeout: 0.2 })).done()
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

  await bus.emit(Event({ event_timeout: 0.2 })).done()
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
    await Promise.all(events.map((event) => event.done()))
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
  await timeout_event.eventCompleted()
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
  await event.eventCompleted()
  const elapsed_ms = Date.now() - started_at

  const initial_snapshot = Array.from(event.event_results.values()).map((result) => ({
    id: result.id,
    status: result.status,
    error_name: (result.error as { constructor?: { name?: string } } | undefined)?.constructor?.name ?? null,
  }))

  assert.ok(elapsed_ms < 100, `event.done() took too long after timeout: ${elapsed_ms}ms`)
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
  await schema_event.eventCompleted()
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
  await serial_timeout_event.eventCompleted()
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
  await parallel_timeout_event.eventCompleted()
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
  await event.done()
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
  await event.done()
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
