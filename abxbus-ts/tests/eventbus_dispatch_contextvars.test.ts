import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'
import { async_local_storage, hasAsyncLocalStorage } from '../src/async_context.js'

type ContextStore = {
  request_id?: string
  user_id?: string
  trace_id?: string
}

const SimpleEvent = BaseEvent.extend('SimpleEvent', {})
const ChildEvent = BaseEvent.extend('ChildEvent', {})

const require_async_local_storage = () => {
  assert.ok(async_local_storage, 'AsyncLocalStorage not available')
  return async_local_storage
}

assert.ok(hasAsyncLocalStorage(), 'AsyncLocalStorage must be available for context propagation tests')

const get_store = (store: ContextStore | undefined | null): ContextStore => store ?? {}

test('context propagates to handler', async () => {
  const bus = new EventBus('ContextTestBus')
  const captured_values: ContextStore = {}
  const storage = require_async_local_storage()
  const request_id = 'a9e03792-9be2-700b-82c9-b46f260cb0cd'
  const user_id = '930eaeb0-5ebe-7f3b-82b2-a824c3a57bae'

  bus.on(SimpleEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_values.request_id = store?.request_id
    captured_values.user_id = store?.user_id
  })

  await storage.run({ request_id, user_id }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.equal(captured_values.request_id, request_id)
  assert.equal(captured_values.user_id, user_id)
})

test('context propagates through nested handlers', async () => {
  const bus = new EventBus('NestedContextBus')
  const captured_parent: ContextStore = {}
  const captured_child: ContextStore = {}
  const storage = require_async_local_storage()
  const request_id = '623d15fd-1410-7da3-8533-ae995961aa0a'
  const trace_id = '4c000901-1ec0-780f-8b2e-fa740047ac91'

  bus.on(SimpleEvent, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    captured_parent.request_id = store?.request_id
    captured_parent.trace_id = store?.trace_id

    const child = event.bus?.emit(ChildEvent({}))
    if (child) {
      await child.done()
    }
  })

  bus.on(ChildEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_child.request_id = store?.request_id
    captured_child.trace_id = store?.trace_id
  })

  await storage.run({ request_id, trace_id }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.equal(captured_parent.request_id, request_id)
  assert.equal(captured_parent.trace_id, trace_id)
  assert.equal(captured_child.request_id, request_id)
  assert.equal(captured_child.trace_id, trace_id)
})

test('context isolation between dispatches', async () => {
  const bus = new EventBus('IsolationTestBus')
  const captured_values: string[] = []
  const storage = require_async_local_storage()
  const request_id_a = '27c90da2-7243-7272-8a0c-4e0b838da162'
  const request_id_b = '0b30a5e6-2d24-7026-82b1-32eb3bee012a'

  bus.on(SimpleEvent, async () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_values.push(store?.request_id ?? '<unset>')
  })

  const event_a = storage.run({ request_id: request_id_a }, () => bus.emit(SimpleEvent({})))
  const event_b = storage.run({ request_id: request_id_b }, () => bus.emit(SimpleEvent({})))

  await event_a.done()
  await event_b.done()

  assert.ok(captured_values.includes(request_id_a))
  assert.ok(captured_values.includes(request_id_b))
})

test('context propagates to multiple handlers', async () => {
  const bus = new EventBus('ParallelContextBus')
  const captured_values: string[] = []
  const storage = require_async_local_storage()
  const request_id = '6e2d89e5-3274-7c5d-8d33-3969cb2cb90a'

  bus.on(SimpleEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_values.push(`h1:${store?.request_id ?? '<unset>'}`)
  })

  bus.on(SimpleEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_values.push(`h2:${store?.request_id ?? '<unset>'}`)
  })

  await storage.run({ request_id }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.ok(captured_values.includes(`h1:${request_id}`))
  assert.ok(captured_values.includes(`h2:${request_id}`))
})

test('context propagates through event forwarding', async () => {
  const bus_a = new EventBus('BusA')
  const bus_b = new EventBus('BusB')
  const captured_bus_a: ContextStore = {}
  const captured_bus_b: ContextStore = {}
  const storage = require_async_local_storage()
  const request_id = '0709aae5-f00f-72e5-8cbe-c08092ed3bb3'

  bus_a.on(SimpleEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_bus_a.request_id = store?.request_id
  })

  bus_b.on(SimpleEvent, () => {
    const store = storage.getStore() as ContextStore | undefined
    captured_bus_b.request_id = store?.request_id
  })

  bus_a.on('*', bus_b.emit)

  await storage.run({ request_id }, async () => {
    const event = bus_a.emit(SimpleEvent({}))
    await event.done()
    await bus_b.waitUntilIdle()
  })

  assert.equal(captured_bus_a.request_id, request_id)
  assert.equal(captured_bus_b.request_id, request_id)
})

test('handler can modify context without affecting parent', async () => {
  const bus = new EventBus('ModifyContextBus')
  const storage = require_async_local_storage()
  let parent_value_after_child = ''
  const parent_request_id = 'bd1374df-0716-77a5-8846-8564a9e75abc'
  const child_request_id = '51969726-2c10-7abc-875f-2607ff63f6e7'

  bus.on(SimpleEvent, async (event) => {
    if (!storage.enterWith) {
      throw new Error('AsyncLocalStorage.enterWith is required for this test')
    }
    storage.enterWith({ request_id: parent_request_id })
    const child = event.bus?.emit(ChildEvent({}))
    if (child) {
      await child.done()
    }
    const store = get_store(storage.getStore() as ContextStore | undefined)
    parent_value_after_child = store.request_id ?? '<unset>'
  })

  bus.on(ChildEvent, () => {
    if (!storage.enterWith) {
      throw new Error('AsyncLocalStorage.enterWith is required for this test')
    }
    storage.enterWith({ request_id: child_request_id })
  })

  await storage.run({}, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.equal(parent_value_after_child, parent_request_id)
})

test('event parent_id tracking still works with context propagation', async () => {
  const bus = new EventBus('ParentIdTrackingBus')
  const storage = require_async_local_storage()
  let parent_event_id: string | undefined
  let child_event_parent_id: string | null | undefined

  bus.on(SimpleEvent, async (event) => {
    parent_event_id = event.event_id
    const child = event.bus?.emit(ChildEvent({}))
    if (child) {
      await child.done()
    }
  })

  bus.on(ChildEvent, (event) => {
    child_event_parent_id = event.event_parent_id
  })

  await storage.run({ request_id: '36a584b5-40c5-7c8b-8627-f9a2e9ce6f82' }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.ok(parent_event_id)
  assert.ok(child_event_parent_id)
  assert.equal(child_event_parent_id, parent_event_id)
})

test('dispatch context and parent_id both work together', async () => {
  const bus = new EventBus('CombinedContextBus')
  const storage = require_async_local_storage()
  const results: Record<string, string | null | undefined> = {}
  const request_id = '3259fa1a-254b-7368-8f50-1ef37ce95f86'

  bus.on(SimpleEvent, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    results.parent_request_id = store?.request_id
    results.parent_event_id = event.event_id
    const child = event.bus?.emit(ChildEvent({}))
    if (child) {
      await child.done()
    }
  })

  bus.on(ChildEvent, (event) => {
    const store = storage.getStore() as ContextStore | undefined
    results.child_request_id = store?.request_id
    results.child_event_parent_id = event.event_parent_id
  })

  await storage.run({ request_id }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.equal(results.parent_request_id, request_id)
  assert.equal(results.child_request_id, request_id)
  assert.equal(results.child_event_parent_id, results.parent_event_id)
})

test('deeply nested context and parent tracking', async () => {
  const bus = new EventBus('DeepNestingBus')
  const storage = require_async_local_storage()
  const request_id = 'fbe90c6c-8193-79de-81d8-e732135fb217'
  const results: Array<{
    level: number
    request_id?: string
    event_id: string
    parent_id?: string | null
  }> = []

  const Level2Event = BaseEvent.extend('Level2Event', {})
  const Level3Event = BaseEvent.extend('Level3Event', {})

  bus.on(SimpleEvent, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    results.push({
      level: 1,
      request_id: store?.request_id,
      event_id: event.event_id,
      parent_id: event.event_parent_id,
    })
    const child = event.bus?.emit(Level2Event({}))
    if (child) {
      await child.done()
    }
  })

  bus.on(Level2Event, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    results.push({
      level: 2,
      request_id: store?.request_id,
      event_id: event.event_id,
      parent_id: event.event_parent_id,
    })
    const child = event.bus?.emit(Level3Event({}))
    if (child) {
      await child.done()
    }
  })

  bus.on(Level3Event, (event) => {
    const store = storage.getStore() as ContextStore | undefined
    results.push({
      level: 3,
      request_id: store?.request_id,
      event_id: event.event_id,
      parent_id: event.event_parent_id,
    })
  })

  await storage.run({ request_id }, async () => {
    const event = bus.emit(SimpleEvent({}))
    await event.done()
  })

  assert.equal(results.length, 3)
  for (const result of results) {
    assert.equal(result.request_id, request_id)
  }
  assert.equal(results[0].parent_id, null)
  assert.equal(results[1].parent_id, results[0].event_id)
  assert.equal(results[2].parent_id, results[1].event_id)
})
