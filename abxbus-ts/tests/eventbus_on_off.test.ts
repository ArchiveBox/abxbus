import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

test('on stores EventHandler entry and indexes it by event key', async () => {
  const bus = new EventBus('RegistryBus')
  const RegistryEvent = BaseEvent.extend('RegistryEvent', {})
  const handler = (event: BaseEvent): string => event.event_type

  const entry = bus.on('RegistryEvent', handler)

  assert.ok(entry.id)
  assert.equal(bus.handlers.has(entry.id), true)
  assert.equal(bus.handlers.get(entry.id), entry)
  assert.equal((bus.handlers_by_key.get('RegistryEvent') ?? []).includes(entry.id), true)

  const dispatched = bus.emit(RegistryEvent({}))
  await dispatched.done()

  const result = dispatched.event_results.get(entry.id)
  assert.ok(result)
  assert.equal(result!.handler.id, entry.id)
})

const RegistryTypingEvent = BaseEvent.extend('RegistryTypingEvent', {
  required_token: z.string(),
  event_result_type: z.string(),
})
type RegistryTypingEventInstance = InstanceType<typeof RegistryTypingEvent>

const typed_sync_handler = (event: RegistryTypingEventInstance): string => event.required_token
const typed_async_handler = async (event: RegistryTypingEventInstance): Promise<string> => event.required_token

test('off removes handlers by callable, handler id, entry object, or all', async () => {
  const bus = new EventBus('RegistryOffBus')
  const RegistryEvent = BaseEvent.extend('RegistryOffEvent', {})

  const handler_a = (_event: BaseEvent): void => {}
  const handler_b = (_event: BaseEvent): void => {}
  const handler_c = (_event: BaseEvent): void => {}

  const entry_a = bus.on('RegistryOffEvent', handler_a)
  const entry_b = bus.on('RegistryOffEvent', handler_b)
  const entry_c = bus.on('RegistryOffEvent', handler_c)

  bus.off('RegistryOffEvent', handler_a)
  assert.equal(bus.handlers.has(entry_a.id), false)
  assert.equal((bus.handlers_by_key.get('RegistryOffEvent') ?? []).includes(entry_a.id), false)
  assert.equal(bus.handlers.has(entry_b.id), true)

  bus.off('RegistryOffEvent', entry_b.id)
  assert.equal(bus.handlers.has(entry_b.id), false)
  assert.equal((bus.handlers_by_key.get('RegistryOffEvent') ?? []).includes(entry_b.id), false)
  assert.equal(bus.handlers.has(entry_c.id), true)

  bus.off('RegistryOffEvent', entry_c)
  assert.equal(bus.handlers.has(entry_c.id), false)
  assert.equal(bus.handlers_by_key.has('RegistryOffEvent'), false)

  bus.on('RegistryOffEvent', handler_a)
  bus.on('RegistryOffEvent', handler_b)
  bus.off('RegistryOffEvent')
  assert.equal(bus.handlers_by_key.has('RegistryOffEvent'), false)
  assert.equal(
    Array.from(bus.handlers.values()).every((entry) => entry.event_pattern !== 'RegistryOffEvent'),
    true
  )

  const dispatched = bus.emit(RegistryEvent({}))
  await dispatched.done()
  assert.equal(dispatched.event_results.size, 0)
})

test('on accepts sync handlers and dispatch captures their return values', async () => {
  const bus = new EventBus('RegistryNormalizeBus')
  const NormalizeEvent = BaseEvent.extend('RegistryNormalizeEvent', {})
  const calls: string[] = []

  const sync_handler = (event: BaseEvent): string => {
    calls.push(event.event_id)
    return 'normalized'
  }

  const entry = bus.on(NormalizeEvent, sync_handler)
  assert.equal(entry.handler, sync_handler)
  const normalized_result = await entry._handler_async(new NormalizeEvent({}))
  assert.equal(normalized_result, 'normalized')

  const dispatched = bus.emit(NormalizeEvent({}))
  await dispatched.done()
  const result = dispatched.event_results.get(entry.id)

  assert.ok(result)
  assert.equal(result!.status, 'completed')
  assert.equal(result!.result, 'normalized')
  assert.equal(calls.length, 2)
})

test('on keeps async handlers normalized through _handler_async', async () => {
  const bus = new EventBus('RegistryAsyncNormalizeBus')
  const NormalizeEvent = BaseEvent.extend('RegistryAsyncNormalizeEvent', {})
  const calls: string[] = []

  const async_handler = async (event: BaseEvent): Promise<string> => {
    calls.push(event.event_id)
    return 'async_normalized'
  }
  const entry = bus.on(NormalizeEvent, async_handler)
  assert.equal(entry.handler, async_handler)
  assert.equal(entry._handler_async, async_handler)

  const normalized_result = await entry._handler_async(new NormalizeEvent({}))
  assert.equal(normalized_result, 'async_normalized')

  const dispatched = bus.emit(NormalizeEvent({}))
  await dispatched.done()
  const result = dispatched.event_results.get(entry.id)

  assert.ok(result)
  assert.equal(result!.status, 'completed')
  assert.equal(result!.result, 'async_normalized')
  assert.equal(calls.length, 2)
})

test('_handler_async preserves typed arg/return contracts for sync handlers', async () => {
  const bus = new EventBus('RegistryTypingSyncBus')
  const entry = bus.on(RegistryTypingEvent, typed_sync_handler)
  const result = await entry._handler_async(RegistryTypingEvent({ required_token: 'sync' }))
  assert.equal(result, 'sync')
})

test('_handler_async preserves typed arg/return contracts for async handlers', async () => {
  const bus = new EventBus('RegistryTypingAsyncBus')
  const entry = bus.on(RegistryTypingEvent, typed_async_handler)
  const result = await entry._handler_async(RegistryTypingEvent({ required_token: 'async' }))
  assert.equal(result, 'async')
})
