import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, EventHandlerResultSchemaError } from '../src/index.js'
import { EventHandler } from '../src/EventHandler.js'
import { EventResult } from '../src/EventResult.js'
import { withResolvers } from '../src/LockManager.js'

const TypedStringResultEvent = BaseEvent.extend('TypedStringResultEvent', {
  event_result_type: z.string(),
})

const ObjectResultEvent = BaseEvent.extend('ObjectResultEvent', {
  event_result_type: z.object({ value: z.string(), count: z.number() }),
})

const NoResultSchemaEvent = BaseEvent.extend('NoResultSchemaEvent', {})

test('event results capture handler return values', async () => {
  const bus = new EventBus('ResultCaptureBus')

  bus.on(StringResultEvent, () => 'ok')

  const event = bus.emit(StringResultEvent({}))
  await event.now()

  assert.equal(event.event_results.size, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'ok')
})

test('typed result schema validates handler result', async () => {
  const bus = new EventBus('ResultSchemaBus')

  bus.on(ObjectResultEvent, () => ({ value: 'hello', count: 2 }))

  const event = bus.emit(ObjectResultEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 2 })
})

test('event_result_type allows undefined handler return values', async () => {
  const bus = new EventBus('ResultSchemaUndefinedBus')

  bus.on(ObjectResultEvent, () => {})

  const event = bus.emit(ObjectResultEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, undefined)
})

test('invalid handler result marks error when schema is defined', async () => {
  const bus = new EventBus('ResultSchemaErrorBus')

  bus.on(ObjectResultEvent, () => JSON.parse('{"value":"bad","count":"nope"}'))

  const event = bus.emit(ObjectResultEvent({}))
  await event.now()
  await assert.rejects(
    () => event.eventResult(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerResultSchemaError)
  assert.ok(event.event_errors.length > 0)
})

test('event result all error options contract', async () => {
  const bus = new EventBus('AllErrorResultOptionsBus', { event_handler_concurrency: 'parallel' })
  const AllErrorEvent = BaseEvent.extend('AllErrorResultOptionsEvent', {})

  bus.on(AllErrorEvent, () => {
    throw new Error('first failure')
  })
  bus.on(AllErrorEvent, () => {
    throw new Error('second failure')
  })

  const event = await bus.emit(AllErrorEvent({})).now()

  await assert.rejects(
    () => event.eventResult(),
    (error: unknown) =>
      error instanceof AggregateError &&
      error.errors.length === 2 &&
      error.errors
        .map((item) => String(item.message))
        .sort()
        .join('|') === 'first failure|second failure'
  )
  await assert.rejects(
    () => event.eventResultsList(),
    (error: unknown) =>
      error instanceof AggregateError &&
      error.errors.length === 2 &&
      error.errors
        .map((item) => String(item.message))
        .sort()
        .join('|') === 'first failure|second failure'
  )

  assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
  assert.deepEqual(await event.eventResultsList({ raise_if_any: false, raise_if_none: false }), [])

  await assert.rejects(() => event.eventResult({ raise_if_any: false, raise_if_none: true }), /Expected at least one handler/)
  await assert.rejects(() => event.eventResultsList({ raise_if_any: false, raise_if_none: true }), /Expected at least one handler/)

  await assert.rejects(() => event.eventResult({ raise_if_any: true, raise_if_none: false }), AggregateError)
  await assert.rejects(() => event.eventResultsList({ raise_if_any: true, raise_if_none: false }), AggregateError)

  await assert.rejects(() => event.eventResult({ raise_if_any: true, raise_if_none: true }), AggregateError)
  await assert.rejects(() => event.eventResultsList({ raise_if_any: true, raise_if_none: true }), AggregateError)

  await bus.destroy()
})

test('event result default options contract', async () => {
  const error_bus = new EventBus('EventResultDefaultErrorOptionsBus')
  const DefaultErrorEvent = BaseEvent.extend('DefaultErrorOptionsEvent', {
    event_result_type: z.string(),
  })

  error_bus.on(DefaultErrorEvent, () => {
    throw new Error('default failure')
  })

  const error_event = await error_bus.emit(DefaultErrorEvent({})).now()

  await assert.rejects(() => error_event.eventResult(), /default failure/)
  await assert.rejects(() => error_event.eventResultsList(), /default failure/)

  assert.equal(await error_event.eventResult({ raise_if_any: false }), undefined)
  assert.deepEqual(await error_event.eventResultsList({ raise_if_any: false }), [])

  await error_bus.destroy()

  const empty_bus = new EventBus('EventResultDefaultNoneOptionsBus')
  const DefaultNoneEvent = BaseEvent.extend('DefaultNoneOptionsEvent', {
    event_result_type: z.string(),
  })

  const empty_event = await empty_bus.emit(DefaultNoneEvent({})).now()
  assert.equal(await empty_event.eventResult(), undefined)
  assert.deepEqual(await empty_event.eventResultsList(), [])

  await assert.rejects(() => empty_event.eventResult({ raise_if_none: true }), /Expected at least one handler/)
  await assert.rejects(() => empty_event.eventResultsList({ raise_if_none: true }), /Expected at least one handler/)

  await empty_bus.destroy()
})

test('event result error shapes use single exception or group', async () => {
  const bus = new EventBus('ErrorShapeContractBus', { event_handler_concurrency: 'parallel' })
  const SingleErrorEvent = BaseEvent.extend('SingleErrorShapeEvent', {})
  const MultiErrorEvent = BaseEvent.extend('MultiErrorShapeEvent', {})

  bus.on(SingleErrorEvent, () => {
    throw new Error('single shape failure')
  })
  bus.on(MultiErrorEvent, () => {
    throw new Error('first shape failure')
  })
  bus.on(MultiErrorEvent, () => {
    throw new TypeError('second shape failure')
  })

  const single_event = await bus.emit(SingleErrorEvent({})).now()
  await assert.rejects(
    () => single_event.eventResult(),
    (error) => error instanceof Error && !(error instanceof AggregateError) && /single shape failure/.test(error.message)
  )

  const multi_event = await bus.emit(MultiErrorEvent({})).now()
  await assert.rejects(
    () => multi_event.eventResult(),
    (error) => error instanceof AggregateError && error.errors.length === 2 && /had 2 handler error/.test(error.message)
  )

  await bus.destroy()
})

test('no schema leaves raw handler result untouched', async () => {
  const bus = new EventBus('NoSchemaBus')

  bus.on(NoResultSchemaEvent, () => ({ raw: true }))

  const event = bus.emit(NoResultSchemaEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { raw: true })
})

test('event result and results list use registration order for current result subset', async () => {
  const bus = new EventBus('EventResultRegistrationOrderBus', { event_handler_concurrency: 'parallel' })
  const AccessorEvent = BaseEvent.extend('EventResultRegistrationOrderEvent', {
    event_result_type: z.string(),
  })
  const completed_order: string[] = []

  const handlers = [
    bus.on(AccessorEvent, async function null_handler() {
      await new Promise((resolve) => setTimeout(resolve, 30))
      completed_order.push('null')
      return undefined
    }),
    bus.on(AccessorEvent, async function winner_handler() {
      await new Promise((resolve) => setTimeout(resolve, 20))
      completed_order.push('winner')
      return 'winner'
    }),
    bus.on(AccessorEvent, function late_handler() {
      completed_order.push('late')
      return 'late'
    }),
  ]
  handlers[0].handler_registered_at = '2026-01-01T00:00:00.001Z'
  handlers[1].handler_registered_at = '2026-01-01T00:00:00.002Z'
  handlers[2].handler_registered_at = '2026-01-01T00:00:00.003Z'

  const event = await bus.emit(AccessorEvent({})).now()

  assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: true }), 'winner')
  assert.deepEqual(await event.eventResultsList({ raise_if_any: false, raise_if_none: true }), ['winner', 'late'])
  assert.deepEqual(
    await event.eventResultsList({
      include: (_result, event_result) => event_result.status === 'completed' && event_result.error === undefined,
      raise_if_any: false,
      raise_if_none: false,
    }),
    [undefined, 'winner', 'late']
  )
  assert.deepEqual(completed_order, ['late', 'winner', 'null'])

  await bus.destroy()
})

test('event result JSON omits result_type and derives from parent event', async () => {
  const bus = new EventBus('ResultTypeDeriveBus')

  bus.on(StringResultEvent, () => 'ok')

  const event = bus.emit(StringResultEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  const json = result.toJSON() as Record<string, unknown>

  assert.equal('result_type' in json, false)
  assert.equal('handler' in json, false)
  assert.equal(typeof json.handler_id, 'string')
  assert.equal(typeof json.handler_name, 'string')
  assert.equal(typeof json.handler_event_pattern, 'string')
  assert.equal(typeof json.eventbus_name, 'string')
  assert.equal(typeof json.eventbus_id, 'string')
  assert.equal(typeof json.handler_registered_at, 'string')
  assert.equal(result.result_type, event.event_result_type)
})

test('EventHandler JSON roundtrips handler metadata', () => {
  const handler = (event: BaseEvent): string => event.event_type
  const entry = new EventHandler({
    handler,
    handler_name: 'pkg.module.handler',
    handler_file_path: '~/project/app.ts:123',
    handler_registered_at: '2025-01-02T03:04:05.678Z',
    event_pattern: 'StandaloneEvent',
    eventbus_name: 'StandaloneBus',
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
  })

  const dumped = entry.toJSON()
  const loaded = EventHandler.fromJSON(dumped)

  assert.equal(loaded.id, entry.id)
  assert.equal(loaded.event_pattern, 'StandaloneEvent')
  assert.equal(loaded.eventbus_name, 'StandaloneBus')
  assert.equal(loaded.eventbus_id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(loaded.handler_name, 'pkg.module.handler')
  assert.equal(loaded.handler_file_path, '~/project/app.ts:123')
})

test('EventHandler.computeHandlerId matches uuidv5 seed algorithm', () => {
  const expected_seed =
    '018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|2025-01-02T03:04:05.678901000Z|StandaloneEvent'
  const expected_id = '19ea9fe8-cfbe-541e-8a35-2579e4e9efff'

  const params = {
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
    handler_name: 'pkg.module.handler',
    handler_file_path: '~/project/app.py:123',
    handler_registered_at: '2025-01-02T03:04:05.678901000Z',
    event_pattern: 'StandaloneEvent',
  } as const
  const computed_id = EventHandler.computeHandlerId(params)

  const actual_seed = `${params.eventbus_id}|${params.handler_name}|${params.handler_file_path}|${params.handler_registered_at}|${params.event_pattern}`
  assert.equal(actual_seed, expected_seed)
  assert.equal(computed_id, expected_id)
})

test('EventHandler.fromCallable supports id override and detect_handler_file_path toggle', () => {
  const handler = (_event: BaseEvent): string => 'ok'
  const explicit_id = '018f8e40-1234-7000-8000-000000009999'

  const explicit = EventHandler.fromCallable({
    handler,
    id: explicit_id,
    event_pattern: 'StandaloneEvent',
    eventbus_name: 'StandaloneBus',
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
    detect_handler_file_path: false,
  })
  assert.equal(explicit.id, explicit_id)

  const no_detect = EventHandler.fromCallable({
    handler,
    event_pattern: 'StandaloneEvent',
    eventbus_name: 'StandaloneBus',
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
    detect_handler_file_path: false,
  })
  assert.equal(no_detect.handler_file_path, null)
})

test('EventResult.update keeps consistent ordering semantics for status/result/error', () => {
  const bus = new EventBus('EventResultUpdateOrderingBus')
  const handler = bus.on(StringResultEvent, () => 'ok')
  const event = StringResultEvent({})
  event.event_bus = bus
  const result = new EventResult({ event, handler })

  const existing_error = new Error('existing')
  result.error = existing_error
  result.update({ status: 'completed' })
  assert.equal(result.status, 'completed')
  assert.equal(result.error, existing_error)

  result.update({ status: 'error', result: 'seeded' })
  assert.equal(result.result, 'seeded')
  assert.equal(result.status, 'error')

  bus.destroy()
})

test('runHandler is a no-op for already-settled results', async () => {
  const SettledEvent = BaseEvent.extend('RunHandlerSettledEvent', {})
  const bus = new EventBus('RunHandlerSettledBus')
  let handler_calls = 0
  const handler = bus.on(SettledEvent, () => {
    handler_calls += 1
    return 'ok'
  })

  const event = SettledEvent({})
  event.event_bus = bus

  const result = new EventResult({ event, handler })
  result.status = 'completed'

  await result.runHandler(null)

  assert.equal(handler_calls, 0)
  assert.equal(result.status, 'completed')
  bus.destroy()
})

test('handler result stays pending while waiting for handler lock entry', async () => {
  const LockWaitEvent = BaseEvent.extend('RunHandlerLockWaitEvent', {})
  const bus = new EventBus('RunHandlerLockWaitBus', { event_handler_concurrency: 'serial' })
  const first_handler_started = withResolvers<void>()

  bus.on(LockWaitEvent, async function first_handler() {
    await first_handler_started.promise
    return 'first'
  })
  bus.on(LockWaitEvent, async function second_handler() {
    await new Promise((resolve) => setTimeout(resolve, 1))
    return 'second'
  })

  const event = bus.emit(LockWaitEvent({}))
  const start = Date.now()
  while (event.event_results.size < 2) {
    if (Date.now() - start > 1_000) {
      throw new Error('Timed out waiting for pending handler result')
    }
    await new Promise((resolve) => setTimeout(resolve, 0))
  }

  const second_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'second_handler')
  assert.ok(second_result)
  assert.equal(second_result.status, 'pending')

  await new Promise((resolve) => setTimeout(resolve, 5))
  assert.equal(second_result.status, 'pending')
  first_handler_started.resolve()
  await event.now()
  assert.equal(second_result.status, 'completed')
  bus.destroy()
})

test('slow handler warning is based on handler runtime after lock wait', async () => {
  const SlowAfterLockWaitEvent = BaseEvent.extend('RunHandlerSlowAfterLockWaitEvent', {})
  const bus = new EventBus('RunHandlerSlowAfterLockWaitBus', {
    event_handler_concurrency: 'serial',
    event_handler_slow_timeout: 0.01,
  })
  const warnings: string[] = []
  const original_warn = console.warn
  console.warn = (message?: unknown, ...args: unknown[]) => {
    warnings.push(String(message))
    if (args.length > 0) {
      warnings.push(args.map(String).join(' '))
    }
  }
  try {
    bus.on(SlowAfterLockWaitEvent, async function first_handler() {
      await new Promise((resolve) => setTimeout(resolve, 40))
      return 'first'
    })
    bus.on(SlowAfterLockWaitEvent, async function second_handler() {
      await new Promise((resolve) => setTimeout(resolve, 30))
      return 'second'
    })

    const event = bus.emit(SlowAfterLockWaitEvent({}))
    const start = Date.now()
    while (event.event_results.size < 2) {
      if (Date.now() - start > 1_000) {
        throw new Error('Timed out waiting for pending handler result')
      }
      await new Promise((resolve) => setTimeout(resolve, 0))
    }

    const second_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'second_handler')
    assert.ok(second_result)
    assert.equal(second_result.status, 'pending')
    await new Promise((resolve) => setTimeout(resolve, 20))
    assert.equal(second_result.status, 'pending')
    await event.now()

    assert.equal(
      warnings.some((message) => message.toLowerCase().includes('slow event handler')),
      true
    )
    assert.equal(
      warnings.some((message) => message.includes('first_handler')),
      true
    )
    assert.equal(
      warnings.some((message) => message.includes('second_handler')),
      true
    )
  } finally {
    console.warn = original_warn
    bus.destroy()
  }
})

const typed_result_type = z.object({
  value: z.string(),
  count: z.number(),
})

const TypedResultEvent = BaseEvent.extend('TypedResultEvent', {
  event_result_type: typed_result_type,
})

const StringResultEvent = BaseEvent.extend('StringResultEvent', {
  event_result_type: z.string(),
})

const NumberResultEvent = BaseEvent.extend('NumberResultEvent', {
  event_result_type: z.number(),
})

const ConstructorStringResultEvent = BaseEvent.extend('ConstructorStringResultEvent', {
  event_result_type: String,
})

const ConstructorNumberResultEvent = BaseEvent.extend('ConstructorNumberResultEvent', {
  event_result_type: Number,
})

const ConstructorBooleanResultEvent = BaseEvent.extend('ConstructorBooleanResultEvent', {
  event_result_type: Boolean,
})

const ConstructorArrayResultEvent = BaseEvent.extend('ConstructorArrayResultEvent', {
  event_result_type: Array,
})

const ConstructorObjectResultEvent = BaseEvent.extend('ConstructorObjectResultEvent', {
  event_result_type: Object,
})

const ComplexResultEvent = BaseEvent.extend('ComplexResultEvent', {
  event_result_type: z.object({
    items: z.array(z.string()),
    metadata: z.record(z.string(), z.number()),
  }),
})

const NoSchemaEvent = BaseEvent.extend('NoSchemaEvent', {})

test('typed result schema validates and parses handler result', async () => {
  const bus = new EventBus('TypedResultBus')

  bus.on(TypedResultEvent, () => ({ value: 'hello', count: 42 }))

  const event = bus.emit(TypedResultEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 42 })
})

test('built-in result schemas validate handler results', async () => {
  const bus = new EventBus('BuiltinResultBus')

  bus.on(TypedStringResultEvent, () => '42')
  bus.on(NumberResultEvent, () => 123)

  const string_event = bus.emit(TypedStringResultEvent({}))
  const number_event = bus.emit(NumberResultEvent({}))
  await string_event.now()
  await number_event.now()

  const string_result = Array.from(string_event.event_results.values())[0]
  const number_result = Array.from(number_event.event_results.values())[0]

  assert.equal(string_result.status, 'completed')
  assert.equal(string_result.result, '42')
  assert.equal(number_result.status, 'completed')
  assert.equal(number_result.result, 123)
})

test('event_result_type supports constructor shorthands and enforces them', async () => {
  const bus = new EventBus('ConstructorResultTypeBus')

  bus.on(ConstructorStringResultEvent, () => 'ok')
  bus.on(ConstructorNumberResultEvent, () => 123)
  bus.on(ConstructorBooleanResultEvent, () => true)
  bus.on(ConstructorArrayResultEvent, () => [1, 'two', false])
  bus.on(ConstructorObjectResultEvent, () => ({ id: 1, ok: true }))

  const string_event = bus.emit(ConstructorStringResultEvent({}))
  const number_event = bus.emit(ConstructorNumberResultEvent({}))
  const boolean_event = bus.emit(ConstructorBooleanResultEvent({}))
  const array_event = bus.emit(ConstructorArrayResultEvent({}))
  const object_event = bus.emit(ConstructorObjectResultEvent({}))

  await Promise.all([string_event.now(), number_event.now(), boolean_event.now(), array_event.now(), object_event.now()])

  assert.equal(typeof (string_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof (number_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof (boolean_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof (array_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof (object_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')

  assert.equal(Array.from(string_event.event_results.values())[0]?.status, 'completed')
  assert.equal(Array.from(number_event.event_results.values())[0]?.status, 'completed')
  assert.equal(Array.from(boolean_event.event_results.values())[0]?.status, 'completed')
  assert.equal(Array.from(array_event.event_results.values())[0]?.status, 'completed')
  assert.equal(Array.from(object_event.event_results.values())[0]?.status, 'completed')

  const invalid_number_event = BaseEvent.extend('ConstructorNumberResultEventInvalid', {
    event_result_type: Number,
  })
  bus.on(invalid_number_event, () => JSON.parse('"not-a-number"'))
  const invalid = bus.emit(invalid_number_event({}))
  await invalid.now()
  await assert.rejects(
    () => invalid.eventResult(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )
  const invalid_result = Array.from(invalid.event_results.values())[0]
  assert.equal(invalid_result?.status, 'error')
  assert.ok(invalid_result?.error instanceof EventHandlerResultSchemaError)
  assert.equal(invalid.event_errors.length, 1)
})

test('number result schema rejects invalid handler result', async () => {
  const bus = new EventBus('ResultValidationErrorBus')

  bus.on(NumberResultEvent, () => JSON.parse('"not-a-number"'))

  const event = bus.emit(NumberResultEvent({}))
  await event.now()
  await assert.rejects(
    () => event.eventResult(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerResultSchemaError)
  assert.ok(event.event_errors.length > 0)
})

test('separate no-schema event stores raw handler result', async () => {
  const bus = new EventBus('NoSchemaResultBus')

  bus.on(NoSchemaEvent, () => ({ raw: true }))

  const event = bus.emit(NoSchemaEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { raw: true })
})

test('complex result schema validates nested data', async () => {
  const bus = new EventBus('ComplexResultBus')

  bus.on(ComplexResultEvent, () => ({
    items: ['a', 'b'],
    metadata: { a: 1, b: 2 },
  }))

  const event = bus.emit(ComplexResultEvent({}))
  await event.now()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { items: ['a', 'b'], metadata: { a: 1, b: 2 } })
})

test('fromJSON converts event_result_type into zod schema', async () => {
  const bus = new EventBus('FromJsonResultBus')

  const original = TypedResultEvent({
    event_result_type: typed_result_type,
  })
  const json = original.toJSON()

  const restored = TypedResultEvent.fromJSON?.(json) ?? TypedResultEvent(json as never)

  assert.ok(restored.event_result_type)
  assert.equal(typeof (restored.event_result_type as { safeParse?: unknown }).safeParse, 'function')

  bus.on(TypedResultEvent, () => ({ value: 'from-json', count: 7 }))

  const dispatched = bus.emit(restored)
  await dispatched.now()

  const result = Array.from(dispatched.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'from-json', count: 7 })
})

test('fromJSON reconstructs primitive JSON schema', async () => {
  const bus = new EventBus('PrimitiveFromJsonBus')

  const source = new BaseEvent({
    event_type: 'PrimitiveResultEvent',
    event_result_type: z.boolean(),
  }).toJSON() as Record<string, unknown>

  const restored = BaseEvent.fromJSON(source)

  assert.ok(restored.event_result_type)
  assert.equal(typeof (restored.event_result_type as { safeParse?: unknown }).safeParse, 'function')

  bus.on('PrimitiveResultEvent', () => true)
  const dispatched = bus.emit(restored)
  await dispatched.now()

  const result = Array.from(dispatched.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, true)
})

test('roundtrip preserves complex result schema types', async () => {
  const bus = new EventBus('RoundtripSchemaBus')

  const complex_schema = z.object({
    title: z.string(),
    count: z.number(),
    flags: z.array(z.boolean()),
    active: z.boolean(),
    meta: z.object({
      tags: z.array(z.string()),
      rating: z.number(),
    }),
  })

  const ComplexRoundtripEvent = BaseEvent.extend('ComplexRoundtripEvent', {
    event_result_type: complex_schema,
  })

  const original = ComplexRoundtripEvent({
    event_result_type: complex_schema,
  })

  const roundtripped = ComplexRoundtripEvent.fromJSON?.(original.toJSON()) ?? ComplexRoundtripEvent(original.toJSON() as never)

  const zod_any = z as unknown as {
    toJSONSchema?: (schema: unknown) => unknown
  }
  if (typeof zod_any.toJSONSchema === 'function') {
    const original_schema_json = zod_any.toJSONSchema(complex_schema)
    const roundtrip_schema_json = zod_any.toJSONSchema(roundtripped.event_result_type)
    assert.deepEqual(roundtrip_schema_json, original_schema_json)
  }

  bus.on(ComplexRoundtripEvent, () => ({
    title: 'ok',
    count: 3,
    flags: [true, false, true],
    active: false,
    meta: { tags: ['a', 'b'], rating: 4 },
  }))

  const dispatched = bus.emit(roundtripped)
  await dispatched.now()

  const result = Array.from(dispatched.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, {
    title: 'ok',
    count: 3,
    flags: [true, false, true],
    active: false,
    meta: { tags: ['a', 'b'], rating: 4 },
  })
})
