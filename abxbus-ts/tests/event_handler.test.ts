import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const UserActionEvent = BaseEvent.extend('UserActionEvent', {
  action: z.string(),
  user_id: z.string(),
})

const SystemEventModel = BaseEvent.extend('SystemEventModel', {
  name: z.string(),
})

test('handler registration via string, class, and wildcard', async () => {
  const bus = new EventBus('HandlerRegistrationBus')
  const results: Record<string, string[]> = {
    specific: [],
    model: [],
    universal: [],
  }

  const user_handler = async (event: InstanceType<typeof UserActionEvent>): Promise<string> => {
    results.specific.push(event.action)
    return 'user_handled'
  }

  const system_handler = async (event: InstanceType<typeof SystemEventModel>): Promise<string> => {
    results.model.push(event.name)
    return 'system_handled'
  }

  const universal_handler = async (event: BaseEvent): Promise<string> => {
    results.universal.push(event.event_type)
    return 'universal'
  }

  bus.on('UserActionEvent', user_handler)
  bus.on(SystemEventModel, system_handler)
  bus.on('*', universal_handler)

  bus.emit(UserActionEvent({ action: 'login', user_id: 'e692b6cb-ae63-773b-8557-3218f7ce5ced' }))
  bus.emit(SystemEventModel({ name: 'startup' }))
  await bus.waitUntilIdle()

  assert.deepEqual(results.specific, ['login'])
  assert.deepEqual(results.model, ['startup'])
  assert.deepEqual(new Set(results.universal), new Set(['UserActionEvent', 'SystemEventModel']))
})

test('handlers can be sync or async', async () => {
  const bus = new EventBus('SyncAsyncHandlersBus')

  const sync_handler = (_event: BaseEvent): string => 'sync'
  const async_handler = async (_event: BaseEvent): Promise<string> => 'async'

  bus.on('TestEvent', sync_handler)
  bus.on('TestEvent', async_handler)

  const handler_count = Array.from(bus.handlers.values()).filter((entry) => entry.event_pattern === 'TestEvent').length
  assert.equal(handler_count, 2)

  const event = bus.emit(BaseEvent.extend('TestEvent', {})({}))
  await event.done()

  const results = Array.from(event.event_results.values()).map((result) => result.result)
  assert.ok(results.includes('sync'))
  assert.ok(results.includes('async'))
})

test('class matcher falls back to class name and matches generic BaseEvent event_type', async () => {
  const bus = new EventBus('ClassNameFallbackBus')

  class DifferentNameFromClass extends BaseEvent {}

  const seen: string[] = []
  bus.on(DifferentNameFromClass, (event: BaseEvent) => {
    seen.push(`class:${event.event_type}`)
  })
  bus.on('DifferentNameFromClass', (event: BaseEvent) => {
    seen.push(`string:${event.event_type}`)
  })
  bus.on('*', (event: BaseEvent) => {
    seen.push(`wildcard:${event.event_type}`)
  })

  await bus.emit(new BaseEvent({ event_type: 'DifferentNameFromClass' })).done()

  assert.deepEqual(seen, ['class:DifferentNameFromClass', 'string:DifferentNameFromClass', 'wildcard:DifferentNameFromClass'])
  assert.equal(bus.handlers_by_key.get('DifferentNameFromClass')?.length, 2)
})

test('instance, class, and static method handlers', async () => {
  const bus = new EventBus('MethodHandlersBus')
  const results: string[] = []

  class EventProcessor {
    name: string
    value: number

    constructor(name: string, value: number) {
      this.name = name
      this.value = value
    }

    sync_method_handler = (event: InstanceType<typeof UserActionEvent>): Record<string, unknown> => {
      results.push(`${this.name}_sync`)
      return { processor: this.name, value: this.value, action: event.action }
    }

    async async_method_handler(event: InstanceType<typeof UserActionEvent>): Promise<Record<string, unknown>> {
      await new Promise((resolve) => setTimeout(resolve, 10))
      results.push(`${this.name}_async`)
      return { processor: this.name, value: this.value * 2, action: event.action }
    }

    static class_method_handler(event: InstanceType<typeof UserActionEvent>): string {
      results.push('classmethod')
      return `Handled by ${event.event_type}`
    }

    static static_method_handler(_event: InstanceType<typeof UserActionEvent>): string {
      results.push('staticmethod')
      return 'Handled by static method'
    }
  }

  const processor1 = new EventProcessor('Processor1', 10)
  const processor2 = new EventProcessor('Processor2', 20)

  bus.on(UserActionEvent, processor1.sync_method_handler)
  bus.on(UserActionEvent, processor1.async_method_handler.bind(processor1))
  bus.on(UserActionEvent, processor2.sync_method_handler)
  bus.on('UserActionEvent', EventProcessor.class_method_handler)
  bus.on('UserActionEvent', EventProcessor.static_method_handler)

  const event = UserActionEvent({ action: 'test_methods', user_id: 'dab45f48-9e3a-7042-80f8-ac8f07b6cfe3' })
  const completed_event = bus.emit(event)
  await completed_event.done()

  assert.equal(results.length, 5)
  assert.ok(results.includes('Processor1_sync'))
  assert.ok(results.includes('Processor1_async'))
  assert.ok(results.includes('Processor2_sync'))
  assert.ok(results.includes('classmethod'))
  assert.ok(results.includes('staticmethod'))

  const result_values = Array.from(completed_event.event_results.values()).map((result) => result.result)

  const p1_sync = result_values.find(
    (result) =>
      typeof result === 'object' &&
      result !== null &&
      (result as { processor?: string; value?: number }).processor === 'Processor1' &&
      (result as { value?: number }).value === 10
  ) as { action?: string } | undefined

  const p1_async = result_values.find(
    (result) =>
      typeof result === 'object' &&
      result !== null &&
      (result as { processor?: string; value?: number }).processor === 'Processor1' &&
      (result as { value?: number }).value === 20
  ) as { action?: string } | undefined

  assert.equal(p1_sync?.action, 'test_methods')
  assert.equal(p1_async?.action, 'test_methods')
})
