import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, monotonicDatetime } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

test('BaseEvent lifecycle transitions are explicit and awaitable', async () => {
  const LifecycleEvent = BaseEvent.extend('BaseEventLifecycleTestEvent', {})

  const standalone = LifecycleEvent({})
  assert.equal(standalone.event_status, 'pending')
  assert.equal(standalone.event_started_at, null)
  assert.equal(standalone.event_completed_at, null)

  standalone._markStarted()
  assert.equal(standalone.event_status, 'started')
  assert.equal(typeof standalone.event_started_at, 'string')

  standalone._markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  assert.equal(typeof standalone.event_completed_at, 'string')
  await standalone.wait()
})

test('eventResult({ raise_if_any: true }) re-raises processing exceptions after now()', async () => {
  const ErrorEvent = BaseEvent.extend('BaseEventResultRaisesFirstErrorEvent', {})
  const bus = new EventBus('BaseEventResultRaisesFirstErrorBus', {
    event_handler_concurrency: 'parallel',
    event_timeout: 0,
  })

  bus.on(ErrorEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 1))
    throw new Error('first failure')
  })

  bus.on(ErrorEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
    throw new Error('second failure')
  })

  const event = bus.emit(ErrorEvent({}))
  await event.now()
  await assert.rejects(() => event.eventResult({ raise_if_any: true }), AggregateError)

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_results.size, 2)
  assert.equal(
    Array.from(event.event_results.values()).every((result) => result.status === 'error'),
    true
  )
})

test('now() waits outside handlers without re-raising', async () => {
  const ErrorEvent = BaseEvent.extend('BaseEventNowArgsOutsideEvent', {})
  const bus = new EventBus('BaseEventNowArgsOutsideBus', {
    event_timeout: 0,
  })

  bus.on(ErrorEvent, async () => {
    throw new Error('outside suppressed failure')
  })

  const event = await bus.emit(ErrorEvent({})).now()

  assert.equal(event.event_status, 'completed')
  assert.equal(
    Array.from(event.event_results.values()).some((result) => result.status === 'error'),
    true
  )
  bus.destroy()
})

test('BaseEvent.eventResultUpdate creates and updates typed handler results', async () => {
  const TypedEvent = BaseEvent.extend('BaseEventEventResultUpdateEvent', { event_result_type: z.string() })
  const bus = new EventBus('BaseEventEventResultUpdateBus')
  const event = TypedEvent({})
  const handler_entry = bus.on(TypedEvent, async () => 'ok')

  const pending = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'pending' })
  assert.equal(event.event_results.get(handler_entry.id), pending)
  assert.equal(pending.status, 'pending')

  const completed = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'completed', result: 'seeded' })
  assert.equal(completed, pending)
  assert.equal(completed.status, 'completed')
  assert.equal(completed.result, 'seeded')

  bus.destroy()
})

test('BaseEvent.eventResultUpdate status-only update does not implicitly pass undefined result/error keys', () => {
  const TypedEvent = BaseEvent.extend('BaseEventEventResultUpdateStatusOnlyEvent', { event_result_type: z.string() })
  const bus = new EventBus('BaseEventEventResultUpdateStatusOnlyBus')
  const event = TypedEvent({})
  const handler_entry = bus.on(TypedEvent, async () => 'ok')

  const errored = event.eventResultUpdate(handler_entry, { eventbus: bus, error: new Error('seeded error') })
  assert.equal(errored.status, 'error')
  assert.ok(errored.error instanceof Error)

  const status_only = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'pending' })
  assert.equal(status_only.status, 'pending')
  assert.ok(status_only.error instanceof Error)
  assert.equal(status_only.result, undefined)

  bus.destroy()
})

test('await event.now() queue-jumps child processing inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventImmediateParentEvent', {})
  const ChildEvent = BaseEvent.extend('BaseEventImmediateChildEvent', {})
  const SiblingEvent = BaseEvent.extend('BaseEventImmediateSiblingEvent', {})

  const bus = new EventBus('BaseEventImmediateQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    event.emit(SiblingEvent({}))
    const child = event.emit(ChildEvent({}))
    assert.ok(child)
    await child.now()
    order.push('parent_end')
  })

  bus.on(ChildEvent, async () => {
    order.push('child')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling')
  })

  await bus.emit(ParentEvent({})).now()
  await bus.waitUntilIdle()

  assert.deepEqual(order, ['parent_start', 'child', 'parent_end', 'sibling'])
  bus.destroy()
})

test('await event.now() queue-jumps child processing inside handlers without re-raising', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventImmediateArgsParentEvent', {})
  const ChildEvent = BaseEvent.extend('BaseEventImmediateArgsChildEvent', {})
  const SiblingEvent = BaseEvent.extend('BaseEventImmediateArgsSiblingEvent', {})

  const bus = new EventBus('BaseEventImmediateQueueJumpArgsBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let child_ref: BaseEvent | undefined

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    event.emit(SiblingEvent({}))
    child_ref = event.emit(ChildEvent({}))
    assert.ok(child_ref)
    await child_ref.now()
    order.push('parent_end')
  })

  bus.on(ChildEvent, async () => {
    order.push('child')
    throw new Error('child failure')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling')
  })

  await bus.emit(ParentEvent({})).now()
  await bus.waitUntilIdle()

  assert.deepEqual(order, ['parent_start', 'child', 'parent_end', 'sibling'])
  assert.equal(child_ref?.event_status, 'completed')
  assert.equal(
    Array.from(child_ref?.event_results.values() ?? []).some((result) => result.status === 'error'),
    true
  )
  bus.destroy()
})

test('wait: outside handler preserves normal queue order', async () => {
  const BlockerEvent = BaseEvent.extend('WaitOutsideHandlerBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('WaitOutsideHandlerTargetEvent', {})
  const bus = new EventBus('WaitOutsideHandlerQueueOrderBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })

  bus.on(TargetEvent, async () => {
    order.push('target')
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({}))
  const targetDone = target.wait()
  await delay(50)
  assert.deepEqual(order, ['blocker_start'])
  releaseBlocker?.()
  assert.equal(await targetDone, target)
  await bus.waitUntilIdle()
  assert.deepEqual(order, ['blocker_start', 'blocker_end', 'target'])
  bus.destroy()
})

test('wait: outside handler allows normal parallel processing', async () => {
  const BlockerEvent = BaseEvent.extend('WaitOutsideHandlerParallelBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('WaitOutsideHandlerParallelTargetEvent', {})
  const bus = new EventBus('WaitOutsideHandlerParallelQueueOrderBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })

  bus.on(TargetEvent, async () => {
    order.push('target')
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({ event_concurrency: 'parallel' }))
  const targetDone = target.wait()
  await delay(50)
  assert.deepEqual(order, ['blocker_start', 'target'])
  releaseBlocker?.()
  assert.equal(await targetDone, target)
  await bus.waitUntilIdle()
  assert.deepEqual(order, ['blocker_start', 'target', 'blocker_end'])
  bus.destroy()
})

test('wait: returns event without forcing queued execution', async () => {
  const BlockerEvent = BaseEvent.extend('WaitPassiveBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('WaitPassiveTargetEvent', { event_result_type: z.string() })
  const bus = new EventBus('WaitPassiveQueueOrderBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })
  bus.on(TargetEvent, async () => {
    order.push('target')
    return 'target'
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({}))
  const waitTask = target.wait({ timeout: 1 })
  await delay(50)
  assert.deepEqual(order, ['blocker_start'])
  releaseBlocker?.()
  assert.equal(await waitTask, target)
  assert.deepEqual(order, ['blocker_start', 'blocker_end', 'target'])
  bus.destroy()
})

test('now: returns event and queue-jumps queued execution', async () => {
  const BlockerEvent = BaseEvent.extend('NowActiveBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('NowActiveTargetEvent', { event_result_type: z.string() })
  const bus = new EventBus('NowActiveQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })
  bus.on(TargetEvent, async () => {
    order.push('target')
    return 'target'
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({}))
  const nowTask = target.now({ timeout: 1 })
  await delay(50)
  assert.deepEqual(order, ['blocker_start', 'target'])
  assert.equal(await nowTask, target)
  releaseBlocker?.()
  await bus.waitUntilIdle()
  assert.deepEqual(order, ['blocker_start', 'target', 'blocker_end'])
  bus.destroy()
})

test('wait: first_result returns before event completion', async () => {
  const FirstResultEvent = BaseEvent.extend('WaitFirstResultEvent', { event_result_type: z.string() })
  const bus = new EventBus('WaitFirstResultBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
    event_timeout: 0,
  })
  let slowFinished = false

  bus.on(FirstResultEvent, async () => {
    await delay(30)
    return 'medium'
  })
  bus.on(FirstResultEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(FirstResultEvent, async () => {
    await delay(250)
    slowFinished = true
    return 'slow'
  })

  const event = bus.emit(FirstResultEvent({ event_concurrency: 'parallel' }))
  assert.equal(await event.wait({ timeout: 1, first_result: true }), event)
  assert.equal(await event.eventResult({ raise_if_any: false }), 'fast')
  await delay(50)
  assert.deepEqual(await event.eventResultsList({ raise_if_any: false }), ['medium', 'fast'])
  assert.equal(slowFinished, false)
  assert.notEqual(event.event_status, 'completed')
  await delay(300)
  await bus.waitUntilIdle()
  assert.equal(slowFinished, true)
  assert.equal(event.event_status, 'completed')
  bus.destroy()
})

test('now: first_result returns before event completion', async () => {
  const FirstResultEvent = BaseEvent.extend('NowFirstResultEvent', { event_result_type: z.string() })
  const bus = new EventBus('NowFirstResultBus', {
    event_handler_concurrency: 'parallel',
    event_timeout: 0,
  })
  let slowFinished = false

  bus.on(FirstResultEvent, async () => {
    await delay(30)
    return 'medium'
  })
  bus.on(FirstResultEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(FirstResultEvent, async () => {
    await delay(250)
    slowFinished = true
    return 'slow'
  })

  const event = bus.emit(FirstResultEvent({ event_concurrency: 'parallel' }))
  assert.equal(await event.now({ timeout: 1, first_result: true }), event)
  assert.equal(await event.eventResult({ raise_if_any: false }), 'fast')
  await delay(50)
  assert.deepEqual(await event.eventResultsList({ raise_if_any: false }), ['medium', 'fast'])
  assert.equal(slowFinished, false)
  assert.notEqual(event.event_status, 'completed')
  await delay(300)
  await bus.waitUntilIdle()
  assert.equal(slowFinished, true)
  assert.equal(event.event_status, 'completed')
  bus.destroy()
})

test('eventResult: starts never-started event and returns first result', async () => {
  const BlockerEvent = BaseEvent.extend('EventResultShortcutBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('EventResultShortcutTargetEvent', { event_result_type: z.string() })
  const bus = new EventBus('EventResultShortcutQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })
  bus.on(TargetEvent, async () => {
    order.push('target')
    return 'target'
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({}))
  const resultTask = target.eventResult()
  await delay(50)
  assert.deepEqual(order, ['blocker_start', 'target'])
  assert.equal(await resultTask, 'target')
  releaseBlocker?.()
  await bus.waitUntilIdle()
  bus.destroy()
})

test('eventResultsList: starts never-started event and returns all results', async () => {
  const BlockerEvent = BaseEvent.extend('EventResultsShortcutBlockerEvent', {})
  const TargetEvent = BaseEvent.extend('EventResultsShortcutTargetEvent', { event_result_type: z.string() })
  const bus = new EventBus('EventResultsShortcutQueueJumpBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let releaseBlocker: (() => void) | undefined
  const blockerReleased = new Promise<void>((resolve) => {
    releaseBlocker = resolve
  })
  let blockerStartedResolve: (() => void) | undefined
  const blockerStarted = new Promise<void>((resolve) => {
    blockerStartedResolve = resolve
  })

  bus.on(BlockerEvent, async () => {
    order.push('blocker_start')
    blockerStartedResolve?.()
    await blockerReleased
    order.push('blocker_end')
  })
  bus.on(TargetEvent, async () => {
    order.push('first')
    return 'first'
  })
  bus.on(TargetEvent, async () => {
    order.push('second')
    return 'second'
  })

  bus.emit(BlockerEvent({}))
  await blockerStarted
  const target = bus.emit(TargetEvent({}))
  const resultsTask = target.eventResultsList()
  await delay(50)
  assert.deepEqual(order, ['blocker_start', 'first', 'second'])
  assert.deepEqual(await resultsTask, ['first', 'second'])
  assert.equal(target.event_results instanceof Map, true)
  assert.equal(target.event_results.size, 2)
  assert.deepEqual(
    Array.from(target.event_results.values()).map((eventResult) => eventResult.result),
    ['first', 'second']
  )
  releaseBlocker?.()
  await bus.waitUntilIdle()
  bus.destroy()
})

test('now: already executing event waits without duplicate execution', async () => {
  const ExecutingEvent = BaseEvent.extend('NowAlreadyExecutingEvent', { event_result_type: z.string() })
  const bus = new EventBus('NowAlreadyExecutingBus', {
    event_handler_concurrency: 'serial',
    event_timeout: 0,
  })
  let runCount = 0
  let releaseHandler: (() => void) | undefined
  const release = new Promise<void>((resolve) => {
    releaseHandler = resolve
  })
  let startedResolve: (() => void) | undefined
  const started = new Promise<void>((resolve) => {
    startedResolve = resolve
  })

  bus.on(ExecutingEvent, async () => {
    runCount += 1
    startedResolve?.()
    await release
    return 'done'
  })

  const event = bus.emit(ExecutingEvent({}))
  await started
  const nowTask = event.now({ timeout: 1 })
  await delay(50)
  assert.equal(runCount, 1)
  releaseHandler?.()
  assert.equal(await nowTask, event)
  assert.equal(await event.eventResult(), 'done')
  assert.equal(runCount, 1)
  bus.destroy()
})

test('eventResult: options apply to current results', async () => {
  const ResultOptionsEvent = BaseEvent.extend('EventResultOptionsCurrentResultsEvent', { event_result_type: z.string() })
  const bus = new EventBus('EventResultOptionsCurrentResultsBus', {
    event_handler_concurrency: 'parallel',
    event_timeout: 0,
  })
  let releaseSlow: (() => void) | undefined
  const slowReleased = new Promise<void>((resolve) => {
    releaseSlow = resolve
  })

  bus.on(ResultOptionsEvent, async () => {
    throw new Error('option boom')
  })
  bus.on(ResultOptionsEvent, async () => 'keep')
  bus.on(ResultOptionsEvent, async () => {
    await slowReleased
    return 'late'
  })

  const event = await bus.emit(ResultOptionsEvent({})).now({ timeout: 1, first_result: true })
  assert.equal(await event.eventResult({ raise_if_any: false }), 'keep')
  try {
    await assert.rejects(() => event.eventResult({ raise_if_any: true }), AggregateError)
    assert.deepEqual(
      await event.eventResultsList({
        include: (result) => result === 'missing',
        raise_if_any: false,
        raise_if_none: false,
      }),
      []
    )
  } finally {
    releaseSlow?.()
  }
  bus.destroy()
})

test('parallel event concurrency plus immediate execution races child events inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventParallelImmediateParentEvent', {})
  const SomeChildEvent1 = BaseEvent.extend('BaseEventParallelImmediateChildEvent1', {})
  const SomeChildEvent2 = BaseEvent.extend('BaseEventParallelImmediateChildEvent2', {})
  const SomeChildEvent3 = BaseEvent.extend('BaseEventParallelImmediateChildEvent3', {})

  const bus = new EventBus('BaseEventParallelImmediateRaceBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })
  const order: string[] = []
  let in_flight = 0
  let max_in_flight = 0

  let release_resolve: (() => void) | undefined
  const release = new Promise<void>((resolve) => {
    release_resolve = resolve
  })

  let all_started_resolve: (() => void) | undefined
  const all_started = new Promise<void>((resolve) => {
    all_started_resolve = resolve
  })

  const trackChild = async (label: string): Promise<string> => {
    order.push(`${label}_start`)
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    if (in_flight === 3) {
      all_started_resolve?.()
    }
    await release
    order.push(`${label}_end`)
    in_flight -= 1
    return label
  }

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    const settled = await Promise.allSettled([
      event.emit(SomeChildEvent1({})).now(),
      event.emit(SomeChildEvent2({})).now(),
      event.emit(SomeChildEvent3({})).now(),
    ])
    order.push('parent_end')
    assert.equal(settled.length, 3)
    assert.equal(
      settled.every((result) => result.status === 'fulfilled'),
      true
    )
  })

  bus.on(SomeChildEvent1, async () => trackChild('child1'))
  bus.on(SomeChildEvent2, async () => trackChild('child2'))
  bus.on(SomeChildEvent3, async () => trackChild('child3'))

  const parent = bus.emit(ParentEvent({}))
  await all_started
  assert.ok(max_in_flight >= 3)
  assert.equal(order.includes('parent_end'), false)

  assert.ok(release_resolve)
  release_resolve()
  await parent.now()
  await bus.waitUntilIdle()

  const parent_end_index = order.indexOf('parent_end')
  for (const label of ['child1', 'child2', 'child3']) {
    assert.ok(order.indexOf(`${label}_start`) < parent_end_index)
    assert.ok(order.indexOf(`${label}_end`) < parent_end_index)
  }

  bus.destroy()
})

test('await event.wait() preserves normal queue order inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('BaseEventQueuedParentEvent', {})
  const ChildEvent = BaseEvent.extend('BaseEventQueuedChildEvent', {})
  const SiblingEvent = BaseEvent.extend('BaseEventQueuedSiblingEvent', {})

  const bus = new EventBus('BaseEventQueueOrderBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    event.emit(SiblingEvent({}))
    const child = event.emit(ChildEvent({}))
    assert.ok(child)
    await child.wait()
    order.push('parent_end')
  })

  bus.on(ChildEvent, async () => {
    order.push('child_start')
    await new Promise((resolve) => setTimeout(resolve, 1))
    order.push('child_end')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling_start')
    await new Promise((resolve) => setTimeout(resolve, 1))
    order.push('sibling_end')
  })

  await bus.emit(ParentEvent({})).now()
  await bus.waitUntilIdle()

  assert.ok(order.indexOf('sibling_start') < order.indexOf('child_start'))
  assert.ok(order.indexOf('child_end') < order.indexOf('parent_end'))
  bus.destroy()
})

test('wait: is passive inside handlers and times out for serial events', async () => {
  const ParentEvent = BaseEvent.extend('PassiveSerialParentEvent', {})
  const SerialEmittedEvent = BaseEvent.extend('PassiveSerialEmittedEvent', {})
  const SerialFoundEvent = BaseEvent.extend('PassiveSerialFoundEvent', {})

  const bus = new EventBus('PassiveSerialWaitBus', { event_concurrency: 'bus-serial' })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    const emitted = event.emit(SerialEmittedEvent({}))
    const foundSource = event.emit(SerialFoundEvent({}))
    const found = await bus.find(SerialFoundEvent, { past: true, future: false })
    assert.equal(found?.event_id, foundSource.event_id)

    await assert.rejects(() => emitted.wait({ timeout: 0.02 }), /Timed out waiting/)
    order.push('emitted_timeout')
    await assert.rejects(() => found!.wait({ timeout: 0.02 }), /Timed out waiting/)
    order.push('found_timeout')
    assert.equal(order.includes('emitted_start'), false)
    assert.equal(order.includes('found_start'), false)
    assert.equal(emitted.event_blocks_parent_completion, false)
    assert.equal(found!.event_blocks_parent_completion, false)
    order.push('parent_end')
  })

  bus.on(SerialEmittedEvent, () => {
    order.push('emitted_start')
  })
  bus.on(SerialFoundEvent, () => {
    order.push('found_start')
  })

  await bus.emit(ParentEvent({})).now()
  await bus.waitUntilIdle()
  assert.deepEqual(order, ['parent_start', 'emitted_timeout', 'found_timeout', 'parent_end', 'emitted_start', 'found_start'])
  bus.destroy()
})

test('wait: serial wait inside handler times out and warns about slow handler', async () => {
  const ParentEvent = BaseEvent.extend('WaitSerialDeadlockWarningParentEvent', {})
  const SerialChildEvent = BaseEvent.extend('WaitSerialDeadlockWarningChildEvent', {})
  const bus = new EventBus('WaitSerialDeadlockWarningBus', {
    event_concurrency: 'bus-serial',
    event_slow_timeout: null,
    event_handler_slow_timeout: 0.01,
  })
  const warnings: string[] = []
  const originalWarn = console.warn
  console.warn = (message?: unknown, ...args: unknown[]) => {
    warnings.push(String(message))
    if (args.length > 0) {
      warnings.push(args.map(String).join(' '))
    }
  }
  const order: string[] = []

  try {
    bus.on(ParentEvent, async (event) => {
      order.push('parent_start')
      const child = event.emit(SerialChildEvent({}))
      const found = await bus.find(SerialChildEvent, { past: true, future: false })
      assert.equal(found?.event_id, child.event_id)
      await assert.rejects(() => found!.wait({ timeout: 0.05 }), /timed out|timeout/i)
      order.push('child_timeout')
      assert.equal(order.includes('child_start'), false)
      assert.equal(found!.event_blocks_parent_completion, false)
      order.push('parent_end')
    })

    bus.on(SerialChildEvent, async () => {
      order.push('child_start')
    })

    await bus.emit(ParentEvent({})).now()
    await bus.waitUntilIdle()
    assert.deepEqual(order, ['parent_start', 'child_timeout', 'parent_end', 'child_start'])
    assert.ok(
      warnings.some((message) => message.toLowerCase().includes('slow event handler')),
      'Expected slow handler warning'
    )
  } finally {
    console.warn = originalWarn
    bus.destroy()
  }
})

test('wait: waits for normal parallel processing inside handlers', async () => {
  const ParentEvent = BaseEvent.extend('PassiveParallelParentEvent', {})
  const ParallelEmittedEvent = BaseEvent.extend('PassiveParallelEmittedEvent', {})
  const ParallelFoundEvent = BaseEvent.extend('PassiveParallelFoundEvent', {})

  const bus = new EventBus('PassiveParallelWaitBus', { event_concurrency: 'parallel' })
  const order: string[] = []

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    const emitted = event.emit(ParallelEmittedEvent({ event_concurrency: 'parallel' }))
    const foundSource = event.emit(ParallelFoundEvent({ event_concurrency: 'parallel' }))
    const found = await bus.find(ParallelFoundEvent, { past: true, future: false })
    assert.equal(found?.event_id, foundSource.event_id)

    await emitted.wait({ timeout: 1 })
    order.push('emitted_completed')
    await found!.wait({ timeout: 1 })
    order.push('found_completed')
    assert.equal(emitted.event_blocks_parent_completion, false)
    assert.equal(found!.event_blocks_parent_completion, false)
    order.push('parent_end')
  })

  bus.on(ParallelEmittedEvent, async () => {
    order.push('emitted_start')
    await delay(1)
    order.push('emitted_end')
  })
  bus.on(ParallelFoundEvent, async () => {
    order.push('found_start')
    await delay(1)
    order.push('found_end')
  })

  await bus.emit(ParentEvent({})).now()
  await bus.waitUntilIdle()
  assert.ok(order.indexOf('emitted_end') < order.indexOf('emitted_completed'))
  assert.ok(order.indexOf('found_end') < order.indexOf('found_completed'))
  assert.equal(order.at(-1), 'parent_end')
  bus.destroy()
})

test('wait: waits for future parallel event found after handler starts', async () => {
  const SomeOtherEvent = BaseEvent.extend('FutureParallelSomeOtherEvent', {})
  const ParallelEvent = BaseEvent.extend('FutureParallelEvent', {})

  const bus = new EventBus('FutureParallelWaitBus', { event_concurrency: 'bus-serial' })
  let resolveOtherStarted!: () => void
  const otherStarted = new Promise<void>((resolve) => {
    resolveOtherStarted = resolve
  })
  let resolveReleaseFind!: () => void
  const releaseFind = new Promise<void>((resolve) => {
    resolveReleaseFind = resolve
  })
  let resolveParallelStarted!: () => void
  const parallelStarted = new Promise<void>((resolve) => {
    resolveParallelStarted = resolve
  })
  let resolveContinued!: () => void
  const continued = new Promise<void>((resolve) => {
    resolveContinued = resolve
  })
  const waitedFor: number[] = []

  bus.on(SomeOtherEvent, async () => {
    resolveOtherStarted()
    await releaseFind
    const found = await bus.find(ParallelEvent, { past: true, future: false })
    assert.ok(found)
    const startedAt = performance.now()
    await found.wait({ timeout: 1 })
    waitedFor.push((performance.now() - startedAt) / 1000)
    resolveContinued()
  })

  bus.on(ParallelEvent, async () => {
    resolveParallelStarted()
    await delay(250)
  })

  const other = bus.emit(SomeOtherEvent({}))
  await otherStarted
  bus.emit(ParallelEvent({ event_concurrency: 'parallel' }))
  await parallelStarted
  resolveReleaseFind()
  await continued
  await other.now()
  await bus.waitUntilIdle()
  assert.ok(waitedFor[0] >= 0.15)
  bus.destroy()
})

test('wait: returns event, accepts timeout, and rejects unattached pending event', async () => {
  const PendingEvent = BaseEvent.extend('WaitPendingNoBusEvent', {})
  await assert.rejects(() => PendingEvent({}).wait({ timeout: 0.01 }), /no bus attached/)

  const CompletedEvent = BaseEvent.extend('WaitCompletedNoBusEvent', {})
  const completed = CompletedEvent({})
  completed._markCompleted(false)
  assert.equal(await completed.wait({ timeout: 0.01 }), completed)

  const SlowEvent = BaseEvent.extend('WaitTimeoutEvent', {})
  const bus = new EventBus('WaitTimeoutBus', { event_concurrency: 'bus-serial' })
  let releaseHandler: (() => void) | undefined
  const handlerReleased = new Promise<void>((resolve) => {
    releaseHandler = resolve
  })

  bus.on(SlowEvent, async () => {
    await handlerReleased
  })

  const event = bus.emit(SlowEvent({}))
  await assert.rejects(() => event.wait({ timeout: 0.01 }), /Timed out waiting/)
  releaseHandler?.()
  assert.equal((await event.wait({ timeout: 1 })).event_id, event.event_id)
  bus.destroy()
})

test('monotonicDatetime emits parseable, monotonic ISO timestamps', () => {
  const first = monotonicDatetime()
  const second = monotonicDatetime()

  assert.equal(typeof first, 'string')
  assert.equal(typeof second, 'string')
  assert.equal(Number.isInteger(Date.parse(first)), true)
  assert.equal(Number.isInteger(Date.parse(second)), true)
  assert.ok(second > first)
})

test('BaseEvent rejects reserved runtime fields in payload and event shape', () => {
  const ReservedFieldEvent = BaseEvent.extend('BaseEventReservedFieldEvent', {})

  assert.throws(() => {
    void ReservedFieldEvent({ bus: 'payload_bus_field' } as unknown as never)
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFieldShapeEvent', { bus: z.string() })
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ wait: 'payload_wait_field' } as unknown as never)
  }, /field "wait" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedWaitShapeEvent', { wait: z.string() })
  }, /field "wait" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ now: 'payload_now_field' } as unknown as never)
  }, /field "now" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedNowShapeEvent', { now: z.string() })
  }, /field "now" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ eventResult: 'payload_event_result_field' } as unknown as never)
  }, /field "eventResult" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedEventResultShapeEvent', { eventResult: z.string() })
  }, /field "eventResult" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ eventResultsList: 'payload_event_results_list_field' } as unknown as never)
  }, /field "eventResultsList" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedEventResultsListShapeEvent', { eventResultsList: z.string() })
  }, /field "eventResultsList" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toString: 'payload_to_string_field' } as unknown as never)
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToStringShapeEvent', { toString: z.string() })
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toJSON: 'payload_to_json_field' } as unknown as never)
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToJSONShapeEvent', { toJSON: z.string() })
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ fromJSON: 'payload_from_json_field' } as unknown as never)
  }, /field "fromJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFromJSONShapeEvent', { fromJSON: z.string() })
  }, /field "fromJSON" is reserved/i)
})

test('BaseEvent rejects unknown event_* fields while allowing known event_* overrides', () => {
  const AllowedEvent = BaseEvent.extend('BaseEventAllowedEventConfigEvent', {
    event_timeout: 123,
    event_slow_timeout: 9,
    event_handler_timeout: 45,
    value: z.string(),
  })

  const event = AllowedEvent({ value: 'ok' })
  assert.equal(event.event_timeout, 123)
  assert.equal(event.event_slow_timeout, 9)
  assert.equal(event.event_handler_timeout, 45)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventUnknownEventShapeFieldEvent', { event_some_field_we_dont_recognize: 1 })
  }, /starts with "event_" but is not a recognized BaseEvent field/i)

  assert.throws(() => {
    void AllowedEvent({
      value: 'ok',
      event_some_field_we_dont_recognize: 1,
    } as unknown as never)
  }, /starts with "event_" but is not a recognized BaseEvent field/i)
})

test('BaseEvent rejects model_* fields in payload and event shape', () => {
  const ModelReservedEvent = BaseEvent.extend('BaseEventModelReservedEvent', {})

  assert.throws(() => {
    void BaseEvent.extend('BaseEventModelReservedShapeEvent', { model_something_random: 1 })
  }, /starts with "model_" and is reserved/i)

  assert.throws(() => {
    void ModelReservedEvent({ model_something_random: 1 } as unknown as never)
  }, /starts with "model_" and is reserved/i)
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('BaseEventRuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('BaseEventRuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.emit(RuntimeEvent({}))
  await event.now()

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_at, 'string')
  assert.equal(typeof json.event_started_at, 'string')
  assert.equal(typeof json.event_completed_at, 'string')
  assert.match(String(json.event_created_at), /Z$/)
  assert.match(String(json.event_started_at), /Z$/)
  assert.match(String(json.event_completed_at), /Z$/)
  assert.equal(json.event_pending_bus_count, 0)

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.deepEqual(JSON.parse(JSON.stringify(restored.toJSON())), JSON.parse(JSON.stringify(json)))
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_at, event.event_created_at)
  assert.equal(restored.event_results.size, 1)
  assert.equal(Array.from(restored.event_results.values())[0].result, 'ok')

  bus.destroy()
})

test('BaseEvent event_*_at fields are recognized and normalized', () => {
  const AtFieldEvent = BaseEvent.extend('BaseEventAtFieldRecognitionEvent', {})
  const event = AtFieldEvent({
    event_created_at: '2025-01-02T03:04:05.678901234Z',
    event_started_at: '2025-01-02T03:04:06.100000000Z',
    event_completed_at: '2025-01-02T03:04:07.200000000Z',
    event_slow_timeout: 1.5,
    event_emitted_by_handler_id: '018f8e40-1234-7000-8000-000000000301',
    event_pending_bus_count: 2,
  } as never)

  assert.equal(event.event_created_at, '2025-01-02T03:04:05.678901234Z')
  assert.equal(event.event_started_at, '2025-01-02T03:04:06.100000000Z')
  assert.equal(event.event_completed_at, '2025-01-02T03:04:07.200000000Z')
  assert.equal(event.event_slow_timeout, 1.5)
  assert.equal(event.event_emitted_by_handler_id, '018f8e40-1234-7000-8000-000000000301')
  assert.equal(event.event_pending_bus_count, 2)
})

test('BaseEvent reset returns a fresh pending event that can be redispatched', async () => {
  const ResetEvent = BaseEvent.extend('BaseEventResetEvent', {
    label: z.string(),
  })

  const bus_a = new EventBus('BaseEventResetBusA')
  const bus_b = new EventBus('BaseEventResetBusB')

  bus_a.on(ResetEvent, (event) => `a:${event.label}`)
  bus_b.on(ResetEvent, (event) => `b:${event.label}`)

  const completed = await bus_a.emit(ResetEvent({ label: 'hello' })).now()
  const fresh = completed.eventReset()

  assert.notEqual(fresh.event_id, completed.event_id)
  assert.equal(fresh.event_status, 'pending')
  assert.equal(fresh.event_results.size, 0)
  assert.equal(fresh.event_started_at, null)
  assert.equal(fresh.event_completed_at, null)

  const forwarded = await bus_b.emit(fresh).now()
  assert.equal(forwarded.event_status, 'completed')
  assert.equal(
    Array.from(forwarded.event_results.values()).some((result) => result.result === 'b:hello'),
    true
  )

  bus_a.destroy()
  bus_b.destroy()
})

test('BaseEvent fromJSON preserves nullable parent/emitted metadata', () => {
  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'BaseEventFromJsonNullFieldsEvent',
    event_parent_id: null,
    event_emitted_by_handler_id: null,
    event_timeout: 0,
  })

  assert.equal(event.event_parent_id, null)
  assert.equal(event.event_emitted_by_handler_id, null)

  const roundtrip = event.toJSON() as Record<string, unknown>
  assert.equal(roundtrip.event_parent_id, null)
  assert.equal(roundtrip.event_emitted_by_handler_id, null)
})

test('BaseEvent status hooks capture bus reference before event gc', async () => {
  const HookEvent = BaseEvent.extend('BaseEventHookCaptureEvent', {})

  class HookCaptureBus extends EventBus {
    seen_statuses: string[] = []

    async onEventChange(_event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      this.seen_statuses.push(status)
    }
  }

  const bus = new HookCaptureBus('BaseEventHookCaptureBus')
  const event = HookEvent({})
  event.event_bus = bus

  event._markStarted()
  event._markCompleted()
  event._gc()

  assert.deepEqual(bus.seen_statuses, ['started', 'completed'])

  bus.destroy()
})
