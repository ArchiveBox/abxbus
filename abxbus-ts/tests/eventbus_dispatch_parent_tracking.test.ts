import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {
  message: z.string().optional(),
})
const ChildEvent = BaseEvent.extend('ChildEvent', {
  data: z.string().optional(),
})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {
  value: z.number().optional(),
})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('basic parent tracking: child events get event_parent_id', async () => {
  const bus = new EventBus('TestBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    const child = event.bus?.emit(ChildEvent({ data: `child_of_${event.message ?? 'root'}` }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'parent_handled'
  })

  const parent = bus.emit(ParentEvent({ message: 'test_parent' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 1)
  assert.equal(child_events[0].event_parent_id, parent.event_id)
})

test('multi-level parent tracking preserves lineage', async () => {
  const bus = new EventBus('LineageBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'child_data' }))
    return 'parent'
  })

  bus.on(ChildEvent, (event) => {
    event.bus?.emit(GrandchildEvent({ value: 42 }))
    return 'child'
  })

  bus.on(GrandchildEvent, () => 'grandchild')

  const parent = bus.emit(ParentEvent({ message: 'root' }))
  await bus.waitUntilIdle()
  await parent.done()

  const seen_parent = Array.from(bus.event_history.values()).find((event) => event.event_id === parent.event_id)
  const seen_child = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  const seen_grandchild = Array.from(bus.event_history.values()).find((event) => event.event_type === 'GrandchildEvent')
  assert.ok(seen_parent)
  assert.ok(seen_child)
  assert.ok(seen_grandchild)
  assert.equal(seen_parent.event_parent_id, null)
  assert.equal(seen_child.event_parent_id, parent.event_id)
  assert.equal(seen_grandchild.event_parent_id, seen_child.event_id)
})

test('multiple children from same parent keep same event_parent_id', async () => {
  const bus = new EventBus('MultiChildBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    for (let i = 0; i < 3; i += 1) {
      const child = event.bus?.emit(ChildEvent({ data: `child_${i}` }))
      if (!child) {
        throw new Error('expected scoped bus on parent handler event')
      }
      child_events.push(child)
    }
    return 'spawned_children'
  })

  const parent = bus.emit(ParentEvent({ message: 'multi_child_parent' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 3)
  for (const child of child_events) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
})

test('bus.emit inside handler auto-links parent when not using event.bus', async () => {
  const bus = new EventBus('ImplicitParentLinkBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    const child = ChildEvent({ data: `implicit_for_${event.event_id.slice(-4)}` })
    child_events.push(bus.emit(child))
    return 'parent_done'
  })
  bus.on(ChildEvent, () => 'child_done')

  const parent = bus.emit(ParentEvent({ message: 'implicit_parent' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 1)
  const child = child_events[0]
  assert.equal(child.event_parent_id, parent.event_id)
  assert.ok(child.event_emitted_by_handler_id)
  assert.ok(parent.event_results.has(child.event_emitted_by_handler_id!))
})

test('cross-bus bus.emit inside handler auto-links parent when exactly one handler is active', async () => {
  const bus1 = new EventBus('ImplicitParentLinkBus1')
  const bus2 = new EventBus('ImplicitParentLinkBus2')
  const emitted_children: BaseEvent[] = []

  bus1.on(ParentEvent, (event) => {
    const child = ChildEvent({ data: `cross_implicit_for_${event.event_id.slice(-4)}` })
    emitted_children.push(bus2.emit(child))
    return 'parent_done'
  })
  bus2.on(ChildEvent, () => 'child_done')

  const parent = bus1.emit(ParentEvent({ message: 'cross_implicit_parent' }))
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle()])
  await parent.done()

  assert.equal(emitted_children.length, 1)
  const child = emitted_children[0]
  assert.equal(child.event_parent_id, parent.event_id)
  assert.ok(child.event_emitted_by_handler_id)
  assert.ok(parent.event_results.has(child.event_emitted_by_handler_id!))
})

test('bus.emit outside handler does not guess a parent when multiple handlers are active', async () => {
  const bus1 = new EventBus('ImplicitParentAmbiguousBus1')
  const bus2 = new EventBus('ImplicitParentAmbiguousBus2')
  const bus3 = new EventBus('ImplicitParentAmbiguousBus3')

  let release_a!: () => void
  let release_b!: () => void
  let mark_started_a!: () => void
  let mark_started_b!: () => void
  const hold_a = new Promise<void>((resolve) => {
    release_a = resolve
  })
  const hold_b = new Promise<void>((resolve) => {
    release_b = resolve
  })
  const started_a = new Promise<void>((resolve) => {
    mark_started_a = resolve
  })
  const started_b = new Promise<void>((resolve) => {
    mark_started_b = resolve
  })

  bus1.on(ParentEvent, async () => {
    mark_started_a()
    await hold_a
    return 'a_done'
  })
  bus2.on(ParentEvent, async () => {
    mark_started_b()
    await hold_b
    return 'b_done'
  })
  bus3.on(ChildEvent, () => 'child_done')

  const parent_a = bus1.emit(ParentEvent({ message: 'a' }))
  const parent_b = bus2.emit(ParentEvent({ message: 'b' }))
  await Promise.all([started_a, started_b])

  const unrelated_child = bus3.emit(ChildEvent({ data: 'outside_handler_emit' }))

  release_a()
  release_b()
  await Promise.all([parent_a.done(), parent_b.done(), unrelated_child.done()])
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle(), bus3.waitUntilIdle()])

  assert.equal(unrelated_child.event_parent_id, null)
  assert.equal(unrelated_child.event_emitted_by_handler_id, null)
})

test('parallel parent handlers preserve parent tracking', async () => {
  const bus = new EventBus('ParallelParentTrackingBus', { event_handler_concurrency: 'parallel' })
  const child_events_h1: BaseEvent[] = []
  const child_events_h2: BaseEvent[] = []

  bus.on(ParentEvent, async (event) => {
    await delay(10)
    const child = event.bus?.emit(ChildEvent({ data: 'from_h1' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events_h1.push(child)
    return 'h1'
  })

  bus.on(ParentEvent, async (event) => {
    await delay(20)
    const child = event.bus?.emit(ChildEvent({ data: 'from_h2' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events_h2.push(child)
    return 'h2'
  })

  const parent = bus.emit(ParentEvent({ message: 'parallel_test' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events_h1.length, 1)
  assert.equal(child_events_h2.length, 1)
  assert.equal(child_events_h1[0].event_parent_id, parent.event_id)
  assert.equal(child_events_h2[0].event_parent_id, parent.event_id)
})

test('explicit event_parent_id is not overridden', async () => {
  const bus = new EventBus('ExplicitParentBus')
  const explicit_parent_id = '018f8e40-1234-7000-8000-000000001234'

  bus.on(ParentEvent, () => {
    const child = ChildEvent({ data: 'explicit', event_parent_id: explicit_parent_id })
    bus.emit(child)
    return 'dispatched'
  })

  const parent = bus.emit(ParentEvent({ message: 'test' }))
  await bus.waitUntilIdle()
  await parent.done()

  const captured_child = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(captured_child)
  assert.equal(captured_child.event_parent_id, explicit_parent_id)
  assert.notEqual(captured_child.event_parent_id, parent.event_id)
})

test('cross-eventbus dispatch preserves parent tracking', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  bus1.on(ParentEvent, (event) => {
    const child = ChildEvent({ data: 'cross_bus_child' })
    event.bus?.emit(child)
    bus2.emit(child)
    return 'bus1_handled'
  })

  bus2.on(ChildEvent, () => 'bus2_handled')

  const parent = bus1.emit(ParentEvent({ message: 'cross_bus_test' }))
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle()])
  await parent.done()

  const received_child = Array.from(bus2.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(received_child)
  assert.equal(received_child.event_parent_id, parent.event_id)
})

test('parent tracking works with sync handlers and handler errors', async () => {
  const bus = new EventBus('SyncAndErrorParentTrackingBus')
  const child_events: BaseEvent[] = []

  const sync_handler = (event: BaseEvent): string => {
    const child = event.bus?.emit(ChildEvent({ data: 'from_sync' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'sync_handled'
  }

  const failing_handler = (event: BaseEvent): never => {
    const child = event.bus?.emit(ChildEvent({ data: 'before_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    throw new Error('expected parent-tracking error path')
  }

  const success_handler = (event: BaseEvent): string => {
    const child = event.bus?.emit(ChildEvent({ data: 'after_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'success'
  }

  bus.on(ParentEvent, sync_handler)
  bus.on(ParentEvent, failing_handler)
  bus.on(ParentEvent, success_handler)
  bus.on(ChildEvent, () => 'child_handled')

  const parent = bus.emit(ParentEvent({ message: 'mixed_test' }))
  await bus.waitUntilIdle()
  await parent.done({ raise_if_any: false })

  assert.equal(child_events.length, 3)
  for (const child of child_events) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
  assert.equal(parent.event_errors.length, 1)
  assert.equal((parent.event_errors[0] as Error).message, 'expected parent-tracking error path')
})

test('erroring parent handlers still preserve child event_parent_id', async () => {
  const bus = new EventBus('ErrorOnlyParentTrackingBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    const child = event.bus?.emit(ChildEvent({ data: 'before_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    throw new Error('expected parent handler failure')
  })
  bus.on(ParentEvent, (event) => {
    const child = event.bus?.emit(ChildEvent({ data: 'after_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'recovered'
  })
  bus.on(ChildEvent, () => 'child_handled')

  const parent = bus.emit(ParentEvent({ message: 'error_only' }))
  await bus.waitUntilIdle()
  await parent.done({ raise_if_any: false })

  assert.equal(child_events.length, 2)
  for (const child of child_events) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
  assert.equal(parent.event_errors.length, 1)
  assert.equal((parent.event_errors[0] as Error).message, 'expected parent handler failure')
})

test('event_children tracks direct and nested descendants', async () => {
  const bus = new EventBus('ChildrenTrackingBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'level1' }))
    return 'parent'
  })

  bus.on(ChildEvent, (event) => {
    event.bus?.emit(GrandchildEvent({ value: 42 }))
    return 'child'
  })

  bus.on(GrandchildEvent, () => 'grandchild')

  const parent = bus.emit(ParentEvent({ message: 'nested_test' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(parent.event_children.length, 1)
  const child = parent.event_children[0]
  assert.equal(child.event_type, 'ChildEvent')
  assert.equal(child.event_children.length, 1)
  const grandchild = child.event_children[0]
  assert.equal(grandchild.event_type, 'GrandchildEvent')
})

test('event_children tracks multiple children from a single handler', async () => {
  const bus = new EventBus('EventChildrenSingleHandlerBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'child_0' }))
    event.bus?.emit(ChildEvent({ data: 'child_1' }))
    event.bus?.emit(ChildEvent({ data: 'child_2' }))
    return 'parent_done'
  })
  bus.on(ChildEvent, (event) => `handled_${event.data ?? 'unknown'}`)

  const parent = bus.emit(ParentEvent({ message: 'children_tracking' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(parent.event_children.length, 3)
  const child_data = parent.event_children.map((child) => Reflect.get(child, 'data') as string | undefined).sort()
  assert.deepEqual(child_data, ['child_0', 'child_1', 'child_2'])
  for (const child of parent.event_children) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
})

test('multiple parent handlers contribute to one event_children list', async () => {
  const bus = new EventBus('EventChildrenMultiHandlerBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'from_handler_1' }))
    return 'h1'
  })
  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'from_handler_2a' }))
    event.bus?.emit(ChildEvent({ data: 'from_handler_2b' }))
    return 'h2'
  })
  bus.on(ChildEvent, (event) => `handled_${event.data ?? 'unknown'}`)

  const parent = bus.emit(ParentEvent({ message: 'multi_handler_children' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(parent.event_children.length, 3)
  const child_data = parent.event_children.map((child) => Reflect.get(child, 'data') as string | undefined).sort()
  assert.deepEqual(child_data, ['from_handler_1', 'from_handler_2a', 'from_handler_2b'])
})

test('event_children is empty when handlers do not emit children', async () => {
  const bus = new EventBus('EventChildrenEmptyBus')

  bus.on(ParentEvent, () => 'no_children')

  const parent = bus.emit(ParentEvent({ message: 'no_children' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(parent.event_children.length, 0)
})

test('parent completion waits for all children', async () => {
  const bus = new EventBus('EventChildrenCompletionBus')
  const completion_order: string[] = []
  let child_started_resolve: (() => void) | null = null
  const child_started = new Promise<void>((resolve) => {
    child_started_resolve = resolve
  })
  let release_children!: () => void
  const children_released = new Promise<void>((resolve) => {
    release_children = () => {
      resolve()
    }
  })
  let child_started_signaled = false

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'child_a' }))
    event.bus?.emit(ChildEvent({ data: 'child_b' }))
    completion_order.push('parent_handler')
    return 'parent'
  })
  bus.on(ChildEvent, async (event) => {
    if (!child_started_signaled) {
      child_started_signaled = true
      child_started_resolve?.()
    }
    await children_released
    completion_order.push(`child_handler_${event.data ?? 'child'}`)
    return `done_${event.data ?? 'child'}`
  })

  const parent = bus.emit(ParentEvent({ message: 'completion' }))
  try {
    await child_started
    assert.ok(parent.event_children.length >= 1)
    assert.equal(parent.event_completed_at ?? null, null)
    assert.notEqual(parent.event_status, 'completed')

    release_children()
    await parent.done()
    await bus.waitUntilIdle()

    assert.equal(parent.event_children.length, 2)
    assert.equal(parent.event_status, 'completed')
    assert.ok(parent.event_completed_at)
    assert.ok(completion_order.includes('parent_handler'))
    for (const child of parent.event_children) {
      assert.equal(child.event_status, 'completed')
    }
  } finally {
    release_children()
  }
})

test('forwarded events are not counted as parent event_children', async () => {
  const bus1 = new EventBus('ForwardBus1')
  const bus2 = new EventBus('ForwardBus2')

  bus1.on('*', bus2.emit)

  const parent = bus1.emit(ParentEvent({ message: 'forward_test' }))
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle()])
  await parent.done()

  assert.equal(parent.event_children.length, 0)
  assert.equal(parent._areAllChildrenComplete(), true)
})
