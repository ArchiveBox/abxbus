import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, EventHandlerResultSchemaError } from '../src/index.js'
import { EventHandler } from '../src/event_handler.js'
import { EventResult } from '../src/event_result.js'

const StringResultEvent = BaseEvent.extend('StringResultEvent', {
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
  await event.done()

  assert.equal(event.event_results.size, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'ok')
})

test('event_result_type validates handler results', async () => {
  const bus = new EventBus('ResultSchemaBus')

  bus.on(ObjectResultEvent, () => ({ value: 'hello', count: 2 }))

  const event = bus.emit(ObjectResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 2 })
})

test('event_result_type allows undefined handler return values', async () => {
  const bus = new EventBus('ResultSchemaUndefinedBus')

  bus.on(ObjectResultEvent, () => {})

  const event = bus.emit(ObjectResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, undefined)
})

test('invalid result marks handler error', async () => {
  const bus = new EventBus('ResultSchemaErrorBus')

  bus.on(ObjectResultEvent, () => JSON.parse('{"value":"bad","count":"nope"}'))

  const event = bus.emit(ObjectResultEvent({}))
  await assert.rejects(
    () => event.done(),
    (error: unknown) => error instanceof EventHandlerResultSchemaError
  )

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerResultSchemaError)
  assert.ok(event.event_errors.length > 0)
})

test('event with no result schema stores raw values', async () => {
  const bus = new EventBus('NoSchemaBus')

  bus.on(NoResultSchemaEvent, () => ({ raw: true }))

  const event = bus.emit(NoResultSchemaEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { raw: true })
})

test('event result JSON omits result_type and derives from parent event', async () => {
  const bus = new EventBus('ResultTypeDeriveBus')

  bus.on(StringResultEvent, () => 'ok')

  const event = bus.emit(StringResultEvent({}))
  await event.done()

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
  const expected_id = '0acdaf2c-a5b1-5785-8499-7c48b3c2c5d8'

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
  event.bus = bus
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
  event.bus = bus

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

  bus.on(LockWaitEvent, async function first_handler() {
    await new Promise((resolve) => setTimeout(resolve, 40))
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

  await new Promise((resolve) => setTimeout(resolve, 20))
  assert.equal(second_result.status, 'pending')
  await event.done()
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
    await event.done()

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
