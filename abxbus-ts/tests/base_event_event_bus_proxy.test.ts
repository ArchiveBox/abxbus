import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const MainEvent = BaseEvent.extend('MainEvent', {})
const ChildEvent = BaseEvent.extend('ChildEvent', {})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {})

test('event.bus inside handler returns the dispatching bus', async () => {
  const bus = new EventBus('TestBus')

  let handler_called = false
  let handler_bus_name: string | undefined
  let child_event: BaseEvent | undefined

  bus.on(MainEvent, (event) => {
    handler_called = true
    handler_bus_name = event.bus?.name

    // Should be able to dispatch child events using event.bus
    child_event = event.bus?.emit(ChildEvent({}))
  })

  bus.on(ChildEvent, () => {})

  bus.emit(MainEvent({}))
  await bus.waitUntilIdle()

  assert.equal(handler_called, true)
  assert.equal(handler_bus_name, 'TestBus')
  assert.ok(child_event, 'child event should have been dispatched via event.bus')
  assert.equal(child_event!.event_type, 'ChildEvent')
})

test('event.event_bus inside handler returns the dispatching bus', async () => {
  const bus = new EventBus('EventBusPropertyBus')
  let handler_bus_name: string | undefined

  bus.on(MainEvent, (event) => {
    handler_bus_name = event.event_bus.name
    assert.equal(event.event_bus, event.bus)
  })

  await bus.emit(MainEvent({})).done()
  assert.equal(handler_bus_name, 'EventBusPropertyBus')
})

test('event.event_bus aliases event.bus for child events emitted in handler', async () => {
  const bus = new EventBus('EventBusPropertyFallbackBus')
  let child_bus_name: string | undefined

  bus.on(MainEvent, (event) => {
    const child = event.event_bus.emit(ChildEvent({}))
    child_bus_name = child.event_bus.name
    assert.equal(child.event_bus, child.bus)
  })
  bus.on(ChildEvent, () => {})

  await bus.emit(MainEvent({})).done()
  assert.equal(child_bus_name, 'EventBusPropertyFallbackBus')
})

test('event.event_bus aliases event.bus for detached events', async () => {
  const bus = new EventBus('EventBusPropertyDetachedBus')
  bus.on(MainEvent, () => {})

  const original = bus.emit(MainEvent({}))
  await original.done()

  const detached = BaseEvent.fromJSON(original.toJSON())
  assert.equal(detached.bus, undefined)
  assert.deepEqual(detached.event_path, [bus.label])
  assert.equal(detached.event_bus, detached.bus)
})

test('event.event_bus aliases event.bus outside handler context', async () => {
  const bus = new EventBus('EventBusPropertyOutsideHandlerBus')
  const event = bus.emit(MainEvent({}))
  await event.done()

  assert.equal(event.event_bus, event.bus)
})

test('event.bus returns correct bus when multiple buses exist', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  let handler1_bus_name: string | undefined
  let handler2_bus_name: string | undefined

  bus1.on(MainEvent, (event) => {
    handler1_bus_name = event.bus?.name
  })

  bus2.on(MainEvent, (event) => {
    handler2_bus_name = event.bus?.name
  })

  bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()

  bus2.emit(MainEvent({}))
  await bus2.waitUntilIdle()

  assert.equal(handler1_bus_name, 'Bus1')
  assert.equal(handler2_bus_name, 'Bus2')
})

test('event.bus reflects the currently-processing bus when forwarded', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  // Forward all events from bus1 to bus2
  bus1.on('*', bus2.emit)

  let bus2_handler_bus_name: string | undefined

  bus2.on(MainEvent, (event) => {
    bus2_handler_bus_name = event.bus?.name
  })

  const event = bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()
  await bus2.waitUntilIdle()

  // The handler on bus2 should see bus2 as event.bus, not bus1
  assert.equal(bus2_handler_bus_name, 'Bus2')
  assert.deepEqual(event.event_path, [bus1.label, bus2.label])
})

test('event.bus in nested handlers sees the same bus', async () => {
  const bus = new EventBus('MainBus')

  let outer_bus_name: string | undefined
  let inner_bus_name: string | undefined

  bus.on(MainEvent, async (event) => {
    outer_bus_name = event.bus?.name

    // Dispatch child using event.bus
    const child = event.bus!.emit(ChildEvent({}))
    await child.done()
  })

  bus.on(ChildEvent, (event) => {
    inner_bus_name = event.bus?.name
  })

  const parent = bus.emit(MainEvent({}))
  await parent.done()

  assert.equal(outer_bus_name, 'MainBus')
  assert.equal(inner_bus_name, 'MainBus')
})

test('event.bus.emit sets parent-child relationships through 3 levels', async () => {
  const bus = new EventBus('MainBus')

  const execution_order: string[] = []
  let child_ref: BaseEvent | undefined
  let grandchild_ref: BaseEvent | undefined

  bus.on(MainEvent, async (event) => {
    execution_order.push('parent_start')
    assert.equal(event.bus?.name, 'MainBus')

    child_ref = event.bus!.emit(ChildEvent({}))
    await child_ref.done()

    execution_order.push('parent_end')
  })

  bus.on(ChildEvent, async (event) => {
    execution_order.push('child_start')
    assert.equal(event.bus?.name, 'MainBus')

    grandchild_ref = event.bus!.emit(GrandchildEvent({}))
    await grandchild_ref.done()

    execution_order.push('child_end')
  })

  bus.on(GrandchildEvent, (event) => {
    execution_order.push('grandchild_start')
    assert.equal(event.bus?.name, 'MainBus')
    execution_order.push('grandchild_end')
  })

  const parent_event = bus.emit(MainEvent({}))
  await parent_event.done()

  // Child events should queue-jump and complete before their parents return
  assert.deepEqual(execution_order, ['parent_start', 'child_start', 'grandchild_start', 'grandchild_end', 'child_end', 'parent_end'])

  // All events completed
  assert.equal(parent_event.event_status, 'completed')
  assert.ok(child_ref)
  assert.equal(child_ref!.event_status, 'completed')
  assert.ok(grandchild_ref)
  assert.equal(grandchild_ref!.event_status, 'completed')

  // Parent-child relationships are set correctly
  assert.equal(child_ref!.event_parent_id, parent_event.event_id)
  assert.equal(grandchild_ref!.event_parent_id, child_ref!.event_id)
  assert.equal(child_ref!.event_parent?.event_id, parent_event.event_id)
  assert.equal(grandchild_ref!.event_parent?.event_id, child_ref!.event_id)
})

test('event.bus with forwarding: child dispatched via event.bus goes to the correct bus', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  // Forward all events from bus1 to bus2
  bus1.on('*', bus2.emit)

  let child_handler_bus_name: string | undefined

  // Handlers only on bus2
  bus2.on(MainEvent, async (event) => {
    // Handler runs on bus2 (forwarded from bus1)
    assert.equal(event.bus?.name, 'Bus2')

    // Child dispatched via event.bus should go to bus2
    const child = event.bus!.emit(ChildEvent({}))
    await child.done()
  })

  bus2.on(ChildEvent, (event) => {
    child_handler_bus_name = event.bus?.name
  })

  bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()
  await bus2.waitUntilIdle()

  // Child handler should have seen bus2
  assert.equal(child_handler_bus_name, 'Bus2')
})

test('event.bus is set on the event after dispatch (outside handler)', async () => {
  const bus = new EventBus('TestBus')

  // Before dispatch, bus is not set
  const raw_event = MainEvent({})
  assert.equal(raw_event.bus, undefined)

  // After dispatch, bus is set on the original event
  const dispatched = bus.emit(raw_event)
  assert.ok(dispatched.bus, 'event.bus should be set after dispatch')

  await bus.waitUntilIdle()
})

test('event.bus.emit from handler correctly attributes event_emitted_by_handler_id', async () => {
  const bus = new EventBus('TestBus')

  bus.on(MainEvent, (event) => {
    event.bus?.emit(ChildEvent({}))
  })

  bus.on(ChildEvent, () => {})

  const parent = bus.emit(MainEvent({}))
  await bus.waitUntilIdle()

  // Find the child event in history
  const child = Array.from(bus.event_history.values()).find((e) => e.event_type === 'ChildEvent')
  assert.ok(child, 'child event should be in history')
  assert.equal(child!.event_parent_id, parent.event_id)
  assert.equal(child!.event_parent?.event_id, parent.event_id)

  // The child should have event_emitted_by_handler_id set to the handler that emitted it
  assert.ok(child!.event_emitted_by_handler_id, 'event_emitted_by_handler_id should be set on child events dispatched via event.bus')

  // The handler id should correspond to a handler result on the parent event
  const parent_from_history = Array.from(bus.event_history.values()).find((e) => e.event_type === 'MainEvent')
  assert.ok(parent_from_history)
  const handler_result = parent_from_history!.event_results.get(child!.event_emitted_by_handler_id!)
  assert.ok(handler_result, 'handler_id on child should match a handler result on the parent')
})

test('dispatch preserves explicit event_parent_id and does not override it', async () => {
  const bus = new EventBus('ExplicitParentBus')
  const explicit_parent_id = '018f8e40-1234-7000-8000-000000001234'

  bus.on(MainEvent, (event) => {
    const child = ChildEvent({
      event_parent_id: explicit_parent_id,
    })
    event.bus?.emit(child)
  })

  const parent = bus.emit(MainEvent({}))
  await bus.waitUntilIdle()

  const child = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(child, 'child event should be in history')
  assert.equal(child.event_parent_id, explicit_parent_id)
  assert.notEqual(child.event_parent_id, parent.event_id)
})

// Consolidated from tests/parent_child.test.ts

const LineageParentEvent = BaseEvent.extend('LineageParentEvent', {})
const LineageChildEvent = BaseEvent.extend('LineageChildEvent', {})
const LineageGrandchildEvent = BaseEvent.extend('LineageGrandchildEvent', {})
const LineageUnrelatedEvent = BaseEvent.extend('LineageUnrelatedEvent', {})

test('eventIsChildOf and eventIsParentOf work for direct children', async () => {
  const bus = new EventBus('ParentChildBus')

  bus.on(LineageParentEvent, (event) => {
    event.bus?.emit(LineageChildEvent({}))
  })

  const parent_event = bus.emit(LineageParentEvent({}))
  await bus.waitUntilIdle()

  const child_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'LineageChildEvent')
  assert.ok(child_event)

  assert.equal(child_event.event_parent_id, parent_event.event_id)
  assert.equal(child_event.event_parent?.event_id, parent_event.event_id)
  assert.equal(bus.eventIsChildOf(child_event, parent_event), true)
  assert.equal(bus.eventIsParentOf(parent_event, child_event), true)
})

test('eventIsChildOf works for grandchildren', async () => {
  const bus = new EventBus('GrandchildBus')

  bus.on(LineageParentEvent, (event) => {
    event.bus?.emit(LineageChildEvent({}))
  })

  bus.on(LineageChildEvent, (event) => {
    event.bus?.emit(LineageGrandchildEvent({}))
  })

  const parent_event = bus.emit(LineageParentEvent({}))
  await bus.waitUntilIdle()

  const child_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'LineageChildEvent')
  const grandchild_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'LineageGrandchildEvent')

  assert.ok(child_event)
  assert.ok(grandchild_event)

  assert.equal(bus.eventIsChildOf(child_event, parent_event), true)
  assert.equal(bus.eventIsChildOf(grandchild_event, parent_event), true)
  assert.equal(child_event.event_parent?.event_id, parent_event.event_id)
  assert.equal(grandchild_event.event_parent?.event_id, child_event.event_id)
  assert.equal(bus.eventIsParentOf(parent_event, grandchild_event), true)
})

test('eventIsChildOf returns false for unrelated events', async () => {
  const bus = new EventBus('UnrelatedBus')

  const parent_event = bus.emit(LineageParentEvent({}))
  const unrelated_event = bus.emit(LineageUnrelatedEvent({}))
  await parent_event.done()
  await unrelated_event.done()

  assert.equal(bus.eventIsChildOf(unrelated_event, parent_event), false)
  assert.equal(bus.eventIsParentOf(parent_event, unrelated_event), false)
})
