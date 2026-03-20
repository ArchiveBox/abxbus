import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../../src/index.js'
import { async_local_storage, hasAsyncLocalStorage } from '../../src/async_context.js'

type ContextStore = {
  request_id?: string
}

const QueueJumpRootEvent = BaseEvent.extend('QueueJumpRootEvent', {})
const QueueJumpChildEvent = BaseEvent.extend('QueueJumpChildEvent', {})
const QueueJumpSiblingEvent = BaseEvent.extend('QueueJumpSiblingEvent', {})

const ConcurrencyIntersectionEvent = BaseEvent.extend('ConcurrencyIntersectionEvent', {})

const TimeoutEnforcementEvent = BaseEvent.extend('TimeoutEnforcementEvent', {
  event_timeout: 0.02,
})
const TimeoutFollowupEvent = BaseEvent.extend('TimeoutFollowupEvent', {})

const ZeroHistoryEvent = BaseEvent.extend('ZeroHistoryEvent', {})

const ContextParentEvent = BaseEvent.extend('ContextParentEvent', {})
const ContextChildEvent = BaseEvent.extend('ContextChildEvent', {})
const PendingVisibilityEvent = BaseEvent.extend('PendingVisibilityEvent', {})
const BackpressureEvent = BaseEvent.extend('BackpressureEvent', {})

test('queue-jump preserves parent/child lineage and find visibility', async () => {
  const bus = new EventBus('ParityQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const execution_order: string[] = []
  let child_event_id: string | null = null

  bus.on(QueueJumpRootEvent, async (event) => {
    execution_order.push('root:start')
    const child = event.bus!.emit(QueueJumpChildEvent({}))
    await child.done()
    execution_order.push('root:end')
    return 'root-ok'
  })

  bus.on(QueueJumpChildEvent, async (event) => {
    child_event_id = event.event_id
    execution_order.push('child')
    await new Promise((resolve) => setTimeout(resolve, 5))
    return 'child-ok'
  })

  bus.on(QueueJumpSiblingEvent, async () => {
    execution_order.push('sibling')
    return 'sibling-ok'
  })

  const root = bus.emit(QueueJumpRootEvent({}))
  const sibling = bus.emit(QueueJumpSiblingEvent({}))
  await root.done()
  await sibling.done()
  await bus.waitUntilIdle()

  assert.deepEqual(execution_order, ['root:start', 'child', 'root:end', 'sibling'])

  const found_child = await bus.find(QueueJumpChildEvent, {
    child_of: root,
    past: true,
    future: false,
  })
  assert.ok(found_child)
  assert.ok(child_event_id)
  assert.equal(found_child!.event_id, child_event_id)
  assert.equal(found_child!.event_parent_id, root.event_id)
  assert.equal(
    root.event_children.some((child) => child.event_id === found_child!.event_id),
    true
  )

  bus.destroy()
})

test('concurrency intersection: parallel events with serial handlers stays serial per-event', async () => {
  const bus = new EventBus('ParityConcurrencyIntersectionBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
    max_history_size: null,
  })

  const current_by_event = new Map<string, number>()
  const max_by_event = new Map<string, number>()
  let global_current = 0
  let global_max = 0

  const tracked_handler = async (event: BaseEvent): Promise<string> => {
    const current = (current_by_event.get(event.event_id) ?? 0) + 1
    current_by_event.set(event.event_id, current)
    max_by_event.set(event.event_id, Math.max(max_by_event.get(event.event_id) ?? 0, current))

    global_current += 1
    global_max = Math.max(global_max, global_current)

    await new Promise((resolve) => setTimeout(resolve, 10))

    current_by_event.set(event.event_id, Math.max(0, (current_by_event.get(event.event_id) ?? 1) - 1))
    global_current -= 1

    return 'ok'
  }

  bus.on(ConcurrencyIntersectionEvent, tracked_handler)
  bus.on(ConcurrencyIntersectionEvent, tracked_handler)

  const events = Array.from({ length: 8 }, () => bus.emit(ConcurrencyIntersectionEvent({})))
  await Promise.all(events.map((event) => event.done()))
  await bus.waitUntilIdle()

  for (const event of events) {
    assert.equal(max_by_event.get(event.event_id), 1)
    assert.equal(
      Array.from(event.event_results.values()).every((result) => result.status === 'completed'),
      true
    )
  }

  assert.ok(global_max >= 2)
  bus.destroy()
})

test('timeout enforcement preserves follow-up processing and queue state', async () => {
  const bus = new EventBus('ParityTimeoutEnforcementBus', {
    event_handler_concurrency: 'parallel',
  })

  bus.on(TimeoutEnforcementEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 200))
    return 'slow-a'
  })

  bus.on(TimeoutEnforcementEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 200))
    return 'slow-b'
  })

  bus.on(TimeoutFollowupEvent, async () => 'followup-ok')

  const timed_out = await bus.emit(TimeoutEnforcementEvent({})).done({ raise_if_any: false })
  assert.equal(timed_out.event_status, 'completed')
  assert.equal(
    Array.from(timed_out.event_results.values()).every((result) => result.status === 'error'),
    true
  )

  const followup = await bus.emit(TimeoutFollowupEvent({})).done()
  assert.equal(
    Array.from(followup.event_results.values()).every((result) => result.status === 'completed'),
    true
  )
  assert.equal(Array.from(followup.event_results.values())[0]?.result, 'followup-ok')

  await bus.waitUntilIdle()
  assert.equal(bus.pending_event_queue.length, 0)
  assert.equal(bus.in_flight_event_ids.size, 0)
  bus.destroy()
})

test('zero-history backpressure with find future still resolves new events', async () => {
  const bus = new EventBus('ParityZeroHistoryBus', {
    max_history_size: 0,
    max_history_drop: false,
  })

  bus.on(ZeroHistoryEvent, async (event) => `ok:${(event as BaseEvent & { value?: string }).value ?? '<missing>'}`)

  const first = await bus.emit(ZeroHistoryEvent({ value: 'first' } as Record<string, unknown>)).done()
  assert.equal(bus.event_history.has(first.event_id), false)

  const past = await bus.find(ZeroHistoryEvent, { past: true, future: false })
  assert.equal(past, null)

  let captured_future_id: string | null = null
  const later = (async () => {
    await new Promise((resolve) => setTimeout(resolve, 20))
    const future_event = bus.emit(ZeroHistoryEvent({ value: 'future' } as Record<string, unknown>))
    captured_future_id = future_event.event_id
  })()

  const future_match = await bus.find(ZeroHistoryEvent, (event) => (event as BaseEvent & { value?: string }).value === 'future', {
    past: false,
    future: 1,
  })
  await later

  assert.ok(future_match)
  assert.equal((future_match as BaseEvent & { value?: string }).value, 'future')
  assert.ok(captured_future_id)
  assert.equal(future_match!.event_id, captured_future_id)

  await bus.waitUntilIdle()
  assert.equal(bus.event_history.size, 0)
  bus.destroy()
})

test('context propagates through forwarding and child dispatch with lineage intact', async () => {
  assert.ok(hasAsyncLocalStorage(), 'AsyncLocalStorage must be available')
  assert.ok(async_local_storage)

  const storage = async_local_storage!
  const bus_a = new EventBus('ParityContextForwardA')
  const bus_b = new EventBus('ParityContextForwardB')

  let captured_parent_request_id: string | null = null
  let captured_child_request_id: string | null = null
  let parent_event_id: string | null = null
  let child_parent_id: string | null = null

  bus_a.on('*', bus_b.emit)

  bus_b.on(ContextParentEvent, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    captured_parent_request_id = store?.request_id ?? null
    parent_event_id = event.event_id

    const child = event.bus!.emit(ContextChildEvent({}))
    await child.done()
    return 'parent-ok'
  })

  bus_b.on(ContextChildEvent, async (event) => {
    const store = storage.getStore() as ContextStore | undefined
    captured_child_request_id = store?.request_id ?? null
    child_parent_id = event.event_parent_id
    return 'child-ok'
  })

  const request_id = 'fc81f432-98cd-7a06-824c-dafed74761bb'
  const parent = await storage.run({ request_id }, async () => bus_a.emit(ContextParentEvent({})).done())
  await bus_b.waitUntilIdle()

  assert.equal(captured_parent_request_id, request_id)
  assert.equal(captured_child_request_id, request_id)
  assert.ok(parent_event_id)
  assert.equal(child_parent_id, parent_event_id)
  assert.equal(parent.event_path[0]?.startsWith('ParityContextForwardA#'), true)
  assert.equal(
    parent.event_path.some((path) => path.startsWith('ParityContextForwardB#')),
    true
  )

  const found_child = await bus_b.find(ContextChildEvent, {
    child_of: parent,
    past: true,
    future: false,
  })
  assert.ok(found_child)
  assert.equal(found_child!.event_parent_id, parent.event_id)

  bus_a.destroy()
  bus_b.destroy()
})

test('pending queue find visibility transitions to completed after release', async () => {
  const bus = new EventBus('ParityPendingFindBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
    max_history_size: null,
  })

  const blocking_control: { release?: () => void } = {}
  const blocking_handler_started = new Promise<void>((resolve) => {
    bus.on(PendingVisibilityEvent, async (event) => {
      const tag = (event as BaseEvent & { tag?: string }).tag
      if (tag === 'blocking') {
        resolve()
        await new Promise<void>((inner_resolve) => {
          blocking_control.release = inner_resolve
        })
      }
      return `ok:${tag ?? '<missing>'}`
    })
  })

  const blocking = bus.emit(PendingVisibilityEvent({ tag: 'blocking' } as Record<string, unknown>))
  await blocking_handler_started

  const queued = bus.emit(PendingVisibilityEvent({ tag: 'queued' } as Record<string, unknown>))
  await new Promise((resolve) => setTimeout(resolve, 10))

  const pending = await bus.find(PendingVisibilityEvent, (event) => (event as BaseEvent & { tag?: string }).tag === 'queued', {
    past: true,
    future: false,
    event_status: 'pending',
  })
  assert.ok(pending)
  assert.equal(pending!.event_id, queued.event_id)

  const release = blocking_control.release
  assert.ok(release)
  release()
  await blocking.done()
  await queued.done()
  await bus.waitUntilIdle()

  const completed = await bus.find(PendingVisibilityEvent, (event) => (event as BaseEvent & { tag?: string }).tag === 'queued', {
    past: true,
    future: false,
    event_status: 'completed',
  })
  assert.ok(completed)
  assert.equal(completed!.event_id, queued.event_id)
  assert.equal(bus.pending_event_queue.length, 0)
  assert.equal(bus.in_flight_event_ids.size, 0)

  bus.destroy()
})

test('history backpressure rejects overflow and preserves findable history', async () => {
  const bus = new EventBus('ParityBackpressureBus', {
    max_history_size: 1,
    max_history_drop: false,
  })

  bus.on(BackpressureEvent, async (event) => `ok:${(event as BaseEvent & { value?: string }).value ?? '<missing>'}`)

  const first = await bus.emit(BackpressureEvent({ value: 'first' } as Record<string, unknown>)).done()
  assert.equal(bus.event_history.size, 1)
  assert.equal(bus.event_history.has(first.event_id), true)

  const found_first = await bus.find(BackpressureEvent, (event) => (event as BaseEvent & { value?: string }).value === 'first', {
    past: true,
    future: false,
  })
  assert.ok(found_first)
  assert.equal(found_first!.event_id, first.event_id)

  await assert.rejects(
    async () => {
      await bus.emit(BackpressureEvent({ value: 'second' } as Record<string, unknown>)).done()
    },
    (error: unknown) => error instanceof Error && error.message.includes('history limit reached')
  )

  assert.equal(bus.event_history.size, 1)
  assert.equal(bus.event_history.has(first.event_id), true)
  assert.equal(bus.pending_event_queue.length, 0)
  assert.equal(bus.in_flight_event_ids.size, 0)

  bus.destroy()
})
