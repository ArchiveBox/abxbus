import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})
const ChildEvent = BaseEvent.extend('ChildEvent', {})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {})
const UnrelatedEvent = BaseEvent.extend('UnrelatedEvent', {})
const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', { target_id: z.string() })
const NavigateEvent = BaseEvent.extend('NavigateEvent', { url: z.string() })
const TabCreatedEvent = BaseEvent.extend('TabCreatedEvent', { tab_id: z.string() })
const SystemEvent = BaseEvent.extend('SystemEvent', {})
const UserActionEvent = BaseEvent.extend('UserActionEvent', {
  action: z.string(),
  user_id: z.string(),
})
const FIND_TARGET_A = '7d787f06-07fd-7406-8be7-0255fb41f459'
const FIND_TARGET_B = 'a2c7f40b-a8a7-78b2-84ef-9f8c60c40a24'
const FIND_USER_1 = 'b57fcb67-faeb-7a56-8907-116d8cbb1472'
const FIND_USER_2 = '28536f9b-4031-7f53-827f-98c24c1b3839'
const FIND_USER_3 = '50d357df-e68c-7111-8a6c-7018569514b0'
const FIND_USER_4 = 'eab58ec9-90ea-7758-893f-afed99518f43'
const FIND_TARGET_OLD = '9b447756-908c-7b75-8a51-4a2c2b4d9b14'
const FIND_TARGET_NEW = '194870e1-fa02-70a4-8101-d10d57c3449c'
const FIND_TARGET_CHILD = '12f38f3d-d8a7-7ae2-8778-bc27e285ea34'

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('find past returns most recent dispatched event', async () => {
  const bus = new EventBus('FindPastBus')

  const first_event = bus.emit(ParentEvent({}))
  await first_event.now()
  await delay(20)
  const second_event = bus.emit(ParentEvent({}))
  await second_event.now()

  const found_event = await bus.find(ParentEvent, { past: true, future: false })
  assert.ok(found_event)
  assert.equal(found_event.event_id, second_event.event_id)
})

test('find past returns null when no matching event exists', async () => {
  const bus = new EventBus('FindPastNoneBus')

  const start = Date.now()
  const found_event = await bus.find(ParentEvent, { past: true, future: false })
  const elapsed_ms = Date.now() - start

  assert.equal(found_event, null)
  assert.ok(elapsed_ms < 100)
})

test('find past history lookup is bus-scoped', async () => {
  const bus_a = new EventBus('FindScopeA')
  const bus_b = new EventBus('FindScopeB')

  bus_b.on(ParentEvent, () => 'done')
  const event_on_b = bus_b.emit(ParentEvent({}))
  await event_on_b.now()

  const found_on_a = await bus_a.find(ParentEvent, { past: true, future: false })
  const found_on_b = await bus_b.find(ParentEvent, { past: true, future: false })

  assert.equal(found_on_a, null)
  assert.ok(found_on_b)
  assert.equal(found_on_b!.event_id, event_on_b.event_id)
})

test('find past result retains origin bus label in event_path', async () => {
  const bus = new EventBus('FindOriginBus')

  const dispatched = bus.emit(ParentEvent({}))
  await dispatched.now()

  const found_event = await bus.find(ParentEvent, { past: true, future: false })
  assert.ok(found_event)
  assert.equal(found_event!.event_path[0], bus.label)
})

test('find past window filters by time', async () => {
  const bus = new EventBus('FindWindowBus')

  const old_event = bus.emit(ParentEvent({}))
  await old_event.now()
  await delay(120)
  const new_event = bus.emit(ParentEvent({}))
  await new_event.now()

  const found_event = await bus.find(ParentEvent, { past: 0.1, future: false })
  assert.ok(found_event)
  assert.equal(found_event.event_id, new_event.event_id)
})

test('find past returns null when all events are too old', async () => {
  const bus = new EventBus('FindTooOldBus')

  const old_event = bus.emit(ParentEvent({}))
  await old_event.now()
  await delay(120)

  const found_event = await bus.find(ParentEvent, { past: 0.05, future: false })
  assert.equal(found_event, null)
})

test('find future waits for event', async () => {
  const bus = new EventBus('FindFutureBus')

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 50)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.event_type, 'ParentEvent')
})

test('max_history_size=0 disables past history search but future find still resolves', async () => {
  const bus = new EventBus('FindZeroHistoryBus', { max_history_size: 0 })
  bus.on(ParentEvent, () => 'ok')

  const find_future = bus.find(ParentEvent, { past: false, future: 0.5 })
  const dispatched = bus.emit(ParentEvent({}))

  const found_future = await find_future
  assert.ok(found_future)
  assert.equal(found_future.event_id, dispatched.event_id)

  await dispatched.now()
  assert.equal(bus.event_history.has(dispatched.event_id), false)

  const found_past = await bus.find(ParentEvent, { past: true, future: false })
  assert.equal(found_past, null)
})

test('find future works with string event keys', async () => {
  const bus = new EventBus('FindFutureStringBus')

  const find_promise = bus.find('ParentEvent', { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 30)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.event_type, 'ParentEvent')
})

test('find class pattern matches generic BaseEvent event_type for future lookups', async () => {
  const bus = new EventBus('FindFutureClassPatternBus')

  class DifferentNameFromClass extends BaseEvent {}

  bus.on('DifferentNameFromClass', () => 'done')

  const find_promise = bus.find(DifferentNameFromClass, { past: false, future: 1 })

  setTimeout(() => {
    void bus.emit(new BaseEvent({ event_type: 'DifferentNameFromClass' }))
  }, 30)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event!.event_type, 'DifferentNameFromClass')
})

test('find future ignores past events', async () => {
  const bus = new EventBus('FindFutureIgnoresPastBus')

  const prior = bus.emit(ParentEvent({}))
  await prior.now()

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 })
  assert.equal(found_event, null)
})

test('find future ignores already-dispatched in-flight events when past=false', async () => {
  const bus = new EventBus('FindFutureIgnoresInflightBus')

  bus.on(ParentEvent, async () => {
    await delay(80)
  })

  const inflight = bus.emit(ParentEvent({}))
  await delay(5)

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 })
  assert.equal(found_event, null)

  await inflight.now()
})

test('find future times out when no event arrives', async () => {
  const bus = new EventBus('FindFutureTimeoutBus')

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 })
  assert.equal(found_event, null)
})

test('find past=false future=false returns null immediately', async () => {
  const bus = new EventBus('FindNeitherBus')

  const start = Date.now()
  const found_event = await bus.find(ParentEvent, { past: false, future: false })
  const elapsed_ms = Date.now() - start

  assert.equal(found_event, null)
  assert.ok(elapsed_ms < 100)
})

test('find defaults to past=true future=false when both are undefined', async () => {
  const bus = new EventBus('FindDefaultWindowBus')

  const start = Date.now()
  const missing = await bus.find(ParentEvent)
  const elapsed_ms = Date.now() - start
  assert.equal(missing, null)
  assert.ok(elapsed_ms < 100)

  const dispatched = bus.emit(ParentEvent({}))
  const found = await bus.find(ParentEvent)
  assert.ok(found)
  assert.equal(found.event_id, dispatched.event_id)
})

test('find past+future returns past event immediately', async () => {
  const bus = new EventBus('FindPastFutureBus')

  const dispatched = bus.emit(ParentEvent({}))
  await dispatched.now()

  const start = Date.now()
  const found_event = await bus.find(ParentEvent, { past: true, future: 0.5 })
  const elapsed_ms = Date.now() - start

  assert.ok(found_event)
  assert.equal(found_event.event_id, dispatched.event_id)
  assert.ok(elapsed_ms < 100)
})

test('find past+future waits for future when no past match', async () => {
  const bus = new EventBus('FindPastFutureWaitBus')

  const find_promise = bus.find(ChildEvent, { past: true, future: 0.3 })

  setTimeout(() => {
    bus.emit(ChildEvent({}))
  }, 50)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.event_type, 'ChildEvent')
})

test('find past/future windows are independent', async () => {
  const bus = new EventBus('FindWindowIndependentBus')

  const old_event = bus.emit(ParentEvent({}))
  await old_event.now()
  await delay(120)

  const start = Date.now()
  const found_event = await bus.find(ParentEvent, { past: 0.05, future: 0.05 })
  const elapsed_ms = Date.now() - start

  assert.equal(found_event, null)
  assert.ok(elapsed_ms > 30)
})

test('find past true future float returns old event immediately', async () => {
  const bus = new EventBus('FindPastTrueFutureFloatBus')

  const dispatched = bus.emit(ParentEvent({}))
  await dispatched.now()
  await delay(120)

  const found_event = await bus.find(ParentEvent, { past: true, future: 0.1 })
  assert.ok(found_event)
  assert.equal(found_event.event_id, dispatched.event_id)
})

test('find past float future waits for new event', async () => {
  const bus = new EventBus('FindPastFloatFutureWaitBus')

  const old_event = bus.emit(ParentEvent({}))
  await old_event.now()
  await delay(120)

  const find_promise = bus.find(ParentEvent, { past: 0.05, future: 0.2 })

  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 50)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.notEqual(found_event.event_id, old_event.event_id)
})

test('find past true future true returns past event immediately', async () => {
  const bus = new EventBus('FindPastTrueFutureTrueBus')

  const dispatched = bus.emit(ParentEvent({}))
  await dispatched.now()

  const start = Date.now()
  const found_event = await bus.find(ParentEvent, { past: true, future: true })
  const elapsed_ms = Date.now() - start

  assert.ok(found_event)
  assert.equal(found_event.event_id, dispatched.event_id)
  assert.ok(elapsed_ms < 100)
})

test('find respects where filter', async () => {
  const bus = new EventBus('FindWhereBus')

  const event_a = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_A }))
  const event_b = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_B }))
  await event_a.now()
  await event_b.now()

  const found_event = await bus.find(ScreenshotEvent, (event) => event.target_id === FIND_TARGET_B, { past: true, future: false })

  assert.ok(found_event)
  assert.equal(found_event.event_id, event_b.event_id)
})

test('find supports metadata filters like event_status', async () => {
  const bus = new EventBus('FindEventStatusFilterBus')
  const release_pause = bus.locks._requestRunloopPause()

  const pending_event = bus.emit(ParentEvent({}))

  const found_pending = await bus.find(ParentEvent, { past: true, future: false, event_status: 'pending' })
  assert.ok(found_pending)
  assert.equal(found_pending.event_id, pending_event.event_id)

  release_pause()
  await pending_event.now()

  const found_completed = await bus.find(ParentEvent, { past: true, future: false, event_status: 'completed' })
  assert.ok(found_completed)
  assert.equal(found_completed.event_id, pending_event.event_id)
})

test('find supports metadata equality filters like event_id and event_timeout', async () => {
  const bus = new EventBus('FindEventFieldFilterBus')

  const event_a = bus.emit(ParentEvent({ event_timeout: 11 }))
  const event_b = bus.emit(ParentEvent({ event_timeout: 22 }))
  await event_a.now()
  await event_b.now()

  const found_a = await bus.find(ParentEvent, {
    past: true,
    future: false,
    event_id: event_a.event_id,
    event_timeout: 11,
  })
  assert.ok(found_a)
  assert.equal(found_a.event_id, event_a.event_id)

  const mismatch = await bus.find(ParentEvent, {
    past: true,
    future: false,
    event_id: event_a.event_id,
    event_timeout: 22,
  })
  assert.equal(mismatch, null)
})

test('find supports non-event data field equality filters', async () => {
  const bus = new EventBus('FindDataFieldFilterBus')

  const event_a = bus.emit(UserActionEvent({ action: 'logout', user_id: FIND_USER_2 }))
  const event_b = bus.emit(UserActionEvent({ action: 'login', user_id: FIND_USER_1 }))
  await event_a.now()
  await event_b.now()

  const found = await bus.find(UserActionEvent, {
    past: true,
    future: false,
    action: 'login',
    user_id: FIND_USER_1,
  })
  assert.ok(found)
  assert.equal(found.event_id, event_b.event_id)

  const mismatch = await bus.find(UserActionEvent, {
    past: true,
    future: false,
    action: 'signup',
  })
  assert.equal(mismatch, null)
})

test('find where filter works with future waiting', async () => {
  const bus = new EventBus('FindWhereFutureBus')

  const find_promise = bus.find(UserActionEvent, (event) => event.user_id === FIND_USER_3, { past: false, future: 0.3 })

  setTimeout(() => {
    bus.emit(UserActionEvent({ action: 'logout', user_id: FIND_USER_4 }))
    bus.emit(UserActionEvent({ action: 'login', user_id: FIND_USER_3 }))
  }, 50)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.user_id, FIND_USER_3)
})

test('find wildcard "*" with where filter matches across event types in history', async () => {
  const bus = new EventBus('FindWildcardPastBus')

  const user_event = bus.emit(UserActionEvent({ action: 'login', user_id: FIND_USER_1 }))
  const system_event = bus.emit(SystemEvent({}))
  await user_event.now()
  await system_event.now()

  const found_event = await bus.find(
    '*',
    (event) => event.event_type === 'UserActionEvent' && (event as InstanceType<typeof UserActionEvent>).user_id === FIND_USER_1,
    { past: true, future: false }
  )

  assert.ok(found_event)
  assert.equal(found_event.event_id, user_event.event_id)
  assert.equal(found_event.event_type, 'UserActionEvent')
})

test('find wildcard "*" with where filter works for future waiting', async () => {
  const bus = new EventBus('FindWildcardFutureBus')

  const find_promise = bus.find(
    '*',
    (event) => event.event_type === 'UserActionEvent' && (event as InstanceType<typeof UserActionEvent>).action === 'special',
    { past: false, future: 0.3 }
  )

  setTimeout(() => {
    bus.emit(SystemEvent({}))
    bus.emit(UserActionEvent({ action: 'normal', user_id: '16ced2b3-de40-7d9b-85c8-c02241a00354' }))
    bus.emit(UserActionEvent({ action: 'special', user_id: '391ce6ed-aa72-73d6-87c4-5e20f3c6fc63' }))
  }, 40)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.event_type, 'UserActionEvent')
  assert.equal((found_event as InstanceType<typeof UserActionEvent>).action, 'special')
})

test('find with multiple concurrent waiters resolves correct events', async () => {
  const bus = new EventBus('FindConcurrentBus')

  const find_normal = bus.find(UserActionEvent, (event) => event.action === 'normal', { past: false, future: 0.5 })
  const find_special = bus.find(UserActionEvent, (event) => event.action === 'special', { past: false, future: 0.5 })
  const find_system = bus.find('SystemEvent', { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(UserActionEvent({ action: 'normal', user_id: 'e692b6cb-ae63-773b-8557-3218f7ce5ced' }))
    bus.emit(SystemEvent({}))
    bus.emit(UserActionEvent({ action: 'special', user_id: '2a312e4d-3035-7883-86b9-578ce47046b2' }))
  }, 50)

  const [normal, system, special] = await Promise.all([find_normal, find_system, find_special])

  assert.ok(normal)
  assert.equal(normal.action, 'normal')
  assert.ok(system)
  assert.equal(system.event_type, 'SystemEvent')
  assert.ok(special)
  assert.equal(special.action, 'special')
})

test('find child_of returns child event', async () => {
  const bus = new EventBus('FindChildBus')

  bus.on(ParentEvent, (event) => {
    event.emit(ChildEvent({}))
  })

  const parent_event = bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const child_event = await bus.find(ChildEvent, {
    past: true,
    future: false,
    child_of: parent_event,
  })

  assert.ok(child_event)
  assert.equal(child_event.event_parent_id, parent_event.event_id)
})

test('find child_of returns null for non-child', async () => {
  const bus = new EventBus('FindNonChildBus')

  const parent_event = bus.emit(ParentEvent({}))
  const unrelated_event = bus.emit(UnrelatedEvent({}))
  await parent_event.now()
  await unrelated_event.now()

  const found_event = await bus.find(UnrelatedEvent, {
    past: true,
    future: false,
    child_of: parent_event,
  })

  assert.equal(found_event, null)
})

test('find child_of returns grandchild event', async () => {
  const bus = new EventBus('FindGrandchildBus')

  let child_event_id: string | null = null
  bus.on(ParentEvent, async (event) => {
    const child = await event.emit(ChildEvent({})).now()
    child_event_id = child?.event_id ?? null
  })
  bus.on(ChildEvent, async (event) => {
    await event.emit(GrandchildEvent({})).now()
  })

  const parent_event = bus.emit(ParentEvent({}))
  await parent_event.now()
  await bus.waitUntilIdle()

  const grandchild_event = await bus.find(GrandchildEvent, {
    past: true,
    future: false,
    child_of: parent_event,
  })

  assert.ok(grandchild_event)
  assert.equal(grandchild_event.event_parent_id, child_event_id)
})

test('find child_of works across forwarded buses', async () => {
  const main_bus = new EventBus('MainBus')
  const auth_bus = new EventBus('AuthBus')

  let child_event_id: string | null = null

  main_bus.on(ParentEvent, auth_bus.emit)
  auth_bus.on(ParentEvent, async (event) => {
    const event_bus = event.event_bus
    assert.ok(event_bus)
    const child_event = event_bus.emit(ChildEvent({}))
    const child = await child_event.now()
    assert.ok(child)
    child_event_id = child.event_id
  })

  const parent_event = main_bus.emit(ParentEvent({}))
  await parent_event.now()
  await main_bus.waitUntilIdle()
  await auth_bus.waitUntilIdle()

  const found_child = await auth_bus.find(ChildEvent, {
    past: 5,
    future: 5,
    child_of: parent_event,
  })

  assert.ok(found_child)
  assert.equal(found_child.event_id, child_event_id)
})

test('find child_of filters to correct parent among siblings', async () => {
  const bus = new EventBus('FindCorrectParentBus')

  bus.on(NavigateEvent, async (event) => {
    await event.emit(TabCreatedEvent({ tab_id: `tab_for_${event.url}` })).now()
  })
  bus.on(TabCreatedEvent, () => {})

  const nav_1 = bus.emit(NavigateEvent({ url: 'site1' }))
  const nav_2 = bus.emit(NavigateEvent({ url: 'site2' }))
  await nav_1.now()
  await nav_2.now()

  const tab_1 = await bus.find(TabCreatedEvent, {
    child_of: nav_1,
    past: true,
    future: false,
  })
  const tab_2 = await bus.find(TabCreatedEvent, {
    child_of: nav_2,
    past: true,
    future: false,
  })

  assert.ok(tab_1)
  assert.ok(tab_2)
  assert.equal(tab_1.tab_id, 'tab_for_site1')
  assert.equal(tab_2.tab_id, 'tab_for_site2')
})

test('find future with child_of waits for matching child', async () => {
  const bus = new EventBus('FindFutureChildBus')

  bus.on(ParentEvent, async (event) => {
    await delay(30)
    await event.emit(ChildEvent({})).now()
  })

  const parent_event = bus.emit(ParentEvent({}))

  const find_promise = bus.find(ChildEvent, {
    child_of: parent_event,
    past: false,
    future: 0.3,
  })

  const child_event = await find_promise
  assert.ok(child_event)
  assert.equal(child_event.event_parent_id, parent_event.event_id)
})

test('find with past float and where filter', async () => {
  const bus = new EventBus('FindWherePastFloatBus')

  const old_event = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_OLD }))
  await old_event.now()
  await delay(120)
  const new_event = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_NEW }))
  await new_event.now()

  const found_tab2 = await bus.find(ScreenshotEvent, (event) => event.target_id === FIND_TARGET_NEW, { past: 0.1, future: false })

  assert.ok(found_tab2)
  assert.equal(found_tab2.event_id, new_event.event_id)

  const found_tab1 = await bus.find(ScreenshotEvent, (event) => event.target_id === FIND_TARGET_OLD, { past: 0.1, future: false })
  assert.equal(found_tab1, null)
})

test('find with child_of and past float', async () => {
  const bus = new EventBus('FindChildPastFloatBus')

  let child_event_id: string | null = null
  bus.on(ParentEvent, async (event) => {
    const child = await event.emit(ChildEvent({})).now()
    child_event_id = child?.event_id ?? null
  })

  const parent_event = bus.emit(ParentEvent({}))
  await parent_event.now()
  await bus.waitUntilIdle()

  const found_child = await bus.find(ChildEvent, {
    child_of: parent_event,
    past: 5,
    future: false,
  })

  assert.ok(found_child)
  assert.equal(found_child.event_id, child_event_id)
})

test('find with all parameters combined', async () => {
  const bus = new EventBus('FindAllParamsBus')

  let child_event_id: string | null = null
  bus.on(ParentEvent, async (event) => {
    const child = await event.emit(ScreenshotEvent({ target_id: FIND_TARGET_CHILD })).now()
    child_event_id = child?.event_id ?? null
  })

  const parent_event = bus.emit(ParentEvent({}))
  await parent_event.now()
  await bus.waitUntilIdle()

  const found_child = await bus.find(ScreenshotEvent, (event) => event.target_id === FIND_TARGET_CHILD, {
    child_of: parent_event,
    past: 5,
    future: false,
  })

  assert.ok(found_child)
  assert.equal(found_child.event_id, child_event_id)
})

test('find past includes in-progress dispatched events', async () => {
  const bus = new EventBus('FindDispatchedPastBus')

  bus.on(ParentEvent, async () => {
    await delay(80)
  })

  const dispatched = bus.emit(ParentEvent({}))
  await delay(10)

  const found = await bus.find(ParentEvent, { past: true, future: false })
  assert.ok(found)
  assert.equal(found.event_id, dispatched.event_id)
  assert.notEqual(found.event_status, 'completed')

  await dispatched.now()
})

test('find future resolves on dispatch before completion', async () => {
  const bus = new EventBus('FindOnDispatchBus')
  const release_pause = bus.locks._requestRunloopPause()

  bus.on(ParentEvent, async () => {
    await delay(80)
  })

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 })

  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 20)

  const found_event = await find_promise
  assert.ok(found_event)
  assert.equal(found_event.event_status, 'pending')

  release_pause()
  await found_event.now()
  assert.equal(found_event.event_status, 'completed')
})

test('find catches child event that fired during parent handler', async () => {
  const bus = new EventBus('FindRaceConditionBus')

  let tab_event_id: string | null = null
  bus.on(NavigateEvent, async (event) => {
    const tab_event = await event.emit(TabCreatedEvent({ tab_id: '06bee4cf-9f51-7e5d-82d3-65f35169329c' })).now()
    tab_event_id = tab_event?.event_id ?? null
  })
  bus.on(TabCreatedEvent, () => {})

  const nav_event = bus.emit(NavigateEvent({ url: 'https://example.com' }))
  await nav_event.now()

  const found_tab = await bus.find(TabCreatedEvent, {
    child_of: nav_event,
    past: true,
    future: false,
  })

  assert.ok(found_tab)
  assert.equal(found_tab.event_id, tab_event_id)
})

test('find returns promise that can be awaited later', async () => {
  const bus = new EventBus('FindPromiseBus')

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 })
  assert.ok(find_promise instanceof Promise)

  bus.emit(ParentEvent({}))
  const found_event = await find_promise
  assert.ok(found_event)
})

test('filter past returns all matches newest first', async () => {
  const bus = new EventBus('FilterAllBus')

  const first = bus.emit(ParentEvent({}))
  const second = bus.emit(ParentEvent({}))
  const third = bus.emit(ParentEvent({}))
  await first.now()
  await second.now()
  await third.now()

  const matches = await bus.filter(ParentEvent, { past: true, future: false })
  assert.equal(matches.length, 3)
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [third.event_id, second.event_id, first.event_id]
  )
})

test('filter returns empty array when no matches', async () => {
  const bus = new EventBus('FilterEmptyBus')
  const matches = await bus.filter(ParentEvent, { past: true, future: false })
  assert.deepEqual(matches, [])
})

test('filter respects limit', async () => {
  const bus = new EventBus('FilterLimitBus')

  bus.emit(ParentEvent({}))
  const second = bus.emit(ParentEvent({}))
  const third = bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const matches = await bus.filter(ParentEvent, { past: true, future: false, limit: 2 })
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [third.event_id, second.event_id]
  )
})

test('filter respects where predicate', async () => {
  const bus = new EventBus('FilterWhereBus')

  const a = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_A }))
  bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_B }))
  const a2 = bus.emit(ScreenshotEvent({ target_id: FIND_TARGET_A }))
  await bus.waitUntilIdle()

  const matches = await bus.filter(ScreenshotEvent, (event) => event.target_id === FIND_TARGET_A, { past: true, future: false })
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [a2.event_id, a.event_id]
  )
})

test('filter supports field equality filters', async () => {
  const bus = new EventBus('FilterFieldBus')

  bus.emit(UserActionEvent({ action: 'login', user_id: FIND_USER_1 }))
  const target = bus.emit(UserActionEvent({ action: 'logout', user_id: FIND_USER_2 }))
  await bus.waitUntilIdle()

  const matches = await bus.filter(UserActionEvent, { past: true, future: false, action: 'logout' })
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [target.event_id]
  )
})

test('filter wildcard matches all event types newest first', async () => {
  const bus = new EventBus('FilterWildcardBus')

  const user_event = bus.emit(UserActionEvent({ action: 'login', user_id: FIND_USER_1 }))
  const system_event = bus.emit(SystemEvent({}))
  await bus.waitUntilIdle()

  const matches = await bus.filter('*', { past: true, future: false })
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [system_event.event_id, user_event.event_id]
  )
})

test('filter past time window filters by age', async () => {
  const bus = new EventBus('FilterPastWindowBus')

  const old_event = bus.emit(ParentEvent({}))
  await old_event.now()
  await delay(120)
  const new_event = bus.emit(ParentEvent({}))
  await new_event.now()

  const matches = await bus.filter(ParentEvent, { past: 0.1, future: false })
  assert.deepEqual(
    matches.map((e) => e.event_id),
    [new_event.event_id]
  )
})

test('filter past=false future=false returns empty array', async () => {
  const bus = new EventBus('FilterNeitherBus')
  bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const matches = await bus.filter(ParentEvent, { past: false, future: false })
  assert.deepEqual(matches, [])
})

test('filter future appends match after past results', async () => {
  const bus = new EventBus('FilterFutureAppendBus')

  const past_event = bus.emit(ParentEvent({}))
  await past_event.now()

  const find_promise = bus.filter(ParentEvent, { past: true, future: 0.5 })
  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 20)

  const matches = await find_promise
  assert.equal(matches.length, 2)
  assert.equal(matches[0].event_id, past_event.event_id)
})

test('filter limit short-circuits future wait', async () => {
  const bus = new EventBus('FilterLimitShortCircuitBus')

  const past_event = bus.emit(ParentEvent({}))
  await past_event.now()

  const start = Date.now()
  const matches = await bus.filter(ParentEvent, { past: true, future: 2.0, limit: 1 })
  const elapsed_ms = Date.now() - start

  assert.equal(matches.length, 1)
  assert.equal(matches[0].event_id, past_event.event_id)
  assert.ok(elapsed_ms < 200)
})

test('filter future only returns dispatched event', async () => {
  const bus = new EventBus('FilterFutureOnlyBus')

  const find_promise = bus.filter(ParentEvent, { past: false, future: 0.5 })
  setTimeout(() => {
    bus.emit(ParentEvent({}))
  }, 30)

  const matches = await find_promise
  assert.equal(matches.length, 1)
  assert.equal(matches[0].event_type, 'ParentEvent')
})

test('filter future only times out to empty array', async () => {
  const bus = new EventBus('FilterFutureTimeoutBus')
  const matches = await bus.filter(ParentEvent, { past: false, future: 0.05 })
  assert.deepEqual(matches, [])
})

test('find returns first filter result', async () => {
  const bus = new EventBus('FindEqualsFilterFirstBus')

  bus.emit(ParentEvent({}))
  const second = bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const found = await bus.find(ParentEvent, { past: true, future: false })
  const filtered = await bus.filter(ParentEvent, { past: true, future: false, limit: 1 })

  assert.ok(found)
  assert.equal(filtered.length, 1)
  assert.equal(found.event_id, filtered[0].event_id)
  assert.equal(found.event_id, second.event_id)
})

test('filter limit=0 returns empty array immediately without waiting for future', async () => {
  const bus = new EventBus('FilterZeroLimitBus')

  bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const start = Date.now()
  const matches = await bus.filter(ParentEvent, { past: true, future: 2.0, limit: 0 })
  const elapsed_ms = Date.now() - start

  assert.deepEqual(matches, [])
  assert.ok(elapsed_ms < 200)
})

test('filter limit=-1 returns empty array', async () => {
  const bus = new EventBus('FilterNegativeLimitBus')

  bus.emit(ParentEvent({}))
  await bus.waitUntilIdle()

  const matches = await bus.filter(ParentEvent, { past: true, future: false, limit: -1 })
  assert.deepEqual(matches, [])
})

test('find treats limit option as a field-equality filter', async () => {
  const LimitFieldEvent = BaseEvent.extend('LimitFieldEvent', { limit: z.number() })
  const bus = new EventBus('FindLimitFieldBus')

  const no_match = bus.emit(LimitFieldEvent({ limit: 3 }))
  const target = bus.emit(LimitFieldEvent({ limit: 5 }))
  await bus.waitUntilIdle()

  const match = await bus.find(LimitFieldEvent, { past: true, future: false, limit: 5 })
  assert.ok(match)
  assert.equal(match.event_id, target.event_id)
  assert.notEqual(match.event_id, no_match.event_id)
})
