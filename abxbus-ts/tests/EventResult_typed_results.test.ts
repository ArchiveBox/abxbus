import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, EventHandlerResultSchemaError } from '../src/index.js'

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
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 42 })
})

test('built-in result schemas validate handler results', async () => {
  const bus = new EventBus('BuiltinResultBus')

  bus.on(StringResultEvent, () => '42')
  bus.on(NumberResultEvent, () => 123)

  const string_event = bus.emit(StringResultEvent({}))
  const number_event = bus.emit(NumberResultEvent({}))
  await string_event.done()
  await number_event.done()

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

  await Promise.all([string_event.done(), number_event.done(), boolean_event.done(), array_event.done(), object_event.done()])

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
  await assert.rejects(
    () => invalid.done(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )
  const invalid_result = Array.from(invalid.event_results.values())[0]
  assert.equal(invalid_result?.status, 'error')
  assert.ok(invalid_result?.error instanceof EventHandlerResultSchemaError)
  assert.equal(invalid.event_errors.length, 1)
})

test('invalid handler result marks error when schema is defined', async () => {
  const bus = new EventBus('ResultValidationErrorBus')

  bus.on(NumberResultEvent, () => JSON.parse('"not-a-number"'))

  const event = bus.emit(NumberResultEvent({}))
  await assert.rejects(
    () => event.done(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerResultSchemaError)
  assert.ok(event.event_errors.length > 0)
})

test('no schema leaves raw handler result untouched', async () => {
  const bus = new EventBus('NoSchemaResultBus')

  bus.on(NoSchemaEvent, () => ({ raw: true }))

  const event = bus.emit(NoSchemaEvent({}))
  await event.done()

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
  await event.done()

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
  await dispatched.done()

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
  await dispatched.done()

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
  await dispatched.done()

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
