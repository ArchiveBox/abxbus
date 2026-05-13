import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, EventResult, events_suck, monotonicDatetime } from '../src/index.js'

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

test('BaseEvent.now() outside handler no args', async () => {
  const ErrorEvent = BaseEvent.extend('BaseEventNowNoArgsOutsideEvent', {})
  const bus = new EventBus('BaseEventNowNoArgsOutsideBus', {
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

test('BaseEvent.now() outside handler with args', async () => {
  const ErrorEvent = BaseEvent.extend('BaseEventNowArgsOutsideEvent', {})
  const bus = new EventBus('BaseEventNowArgsOutsideBus', {
    event_timeout: 0,
  })

  bus.on(ErrorEvent, async () => {
    throw new Error('outside suppressed failure')
  })

  const event = await bus.emit(ErrorEvent({})).now({ timeout: 1 })

  assert.equal(event.event_status, 'completed')
  assert.equal(
    Array.from(event.event_results.values()).some((result) => result.status === 'error'),
    true
  )
  assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
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

test('BaseEvent.now() inside handler no args', async () => {
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

test('BaseEvent.now() inside handler with args', async () => {
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
    await child_ref.now({ timeout: 1 })
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

test('event result helpers do not wait for started event', async () => {
  const StartedEvent = BaseEvent.extend('EventResultHelpersStartedEvent', { event_result_type: z.string() })
  const bus = new EventBus('EventResultHelpersStartedBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
    event_timeout: 0,
  })
  let releaseHandler: (() => void) | undefined
  const handlerReleased = new Promise<void>((resolve) => {
    releaseHandler = resolve
  })
  let handlerStartedResolve: (() => void) | undefined
  const handlerStarted = new Promise<void>((resolve) => {
    handlerStartedResolve = resolve
  })

  bus.on(StartedEvent, async () => {
    handlerStartedResolve?.()
    await handlerReleased
    return 'late'
  })

  const event = bus.emit(StartedEvent({}))
  await handlerStarted

  assert.equal(event.event_status, 'started')
  assert.equal(await event.eventResult({ raise_if_none: false }), undefined)
  assert.deepEqual(await event.eventResultsList({ raise_if_none: false }), [])
  assert.equal(event.event_status, 'started')

  releaseHandler?.()
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

test('now: rapid handler churn does not duplicate execution', async () => {
  const ChurnEvent = BaseEvent.extend('NowRapidHandlerChurnEvent', { event_result_type: z.string() })
  const totalEvents = 200
  const bus = new EventBus('NowRapidHandlerChurnBus', {
    event_timeout: 0,
    max_history_size: 512,
    max_history_drop: true,
  })
  let runCount = 0

  for (let index = 0; index < totalEvents; index += 1) {
    const handler = bus.on(ChurnEvent, async () => {
      runCount += 1
      await delay(0)
      return 'done'
    })
    const event = bus.emit(ChurnEvent({}))
    assert.equal(await event.now({ timeout: 1 }), event)
    await delay(0)
    await bus.waitUntilIdle()
    bus.off(ChurnEvent, handler)
  }

  assert.equal(runCount, totalEvents)
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
  bus.on(ResultOptionsEvent, async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
    return 'keep'
  })
  bus.on(ResultOptionsEvent, async () => {
    await slowReleased
    return 'late'
  })

  const event = await bus.emit(ResultOptionsEvent({})).now({ timeout: 1, first_result: true })
  assert.equal(await event.eventResult({ raise_if_any: false }), 'keep')
  try {
    await assert.rejects(() => event.eventResult({ raise_if_any: true }), /option boom/)
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

test('deferred emit after handler completion is accepted', async () => {
  const ParentEvent = BaseEvent.extend('DeferredEmitAfterCompletionParentEvent', {})
  const DeferredChildEvent = BaseEvent.extend('DeferredEmitAfterCompletionChildEvent', {})
  const bus = new EventBus('DeferredEmitAfterCompletionBus', { event_concurrency: 'bus-serial' })
  const order: string[] = []
  let resolveEmitted!: () => void
  const emitted = new Promise<void>((resolve) => {
    resolveEmitted = resolve
  })

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    void (async () => {
      await delay(20)
      order.push('deferred_emit')
      event.emit(DeferredChildEvent({}))
      resolveEmitted()
    })()
    order.push('parent_end')
  })

  bus.on(DeferredChildEvent, async () => {
    order.push('child_start')
  })

  await bus.emit(ParentEvent({})).now()
  await emitted
  await bus.waitUntilIdle(1)
  assert.deepEqual(order, ['parent_start', 'parent_end', 'deferred_emit', 'child_start'])
  await bus.destroy()
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

// Folded from BaseEvent_EventBus_proxy.test.ts to keep test layout class-based.
const MainEvent = BaseEvent.extend('MainEvent', {})
const ChildEvent = BaseEvent.extend('ChildEvent', {})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {})

test('event.event_bus inside handler returns the dispatching bus', async () => {
  const bus = new EventBus('TestBus')

  let handler_called = false
  let handler_bus_name: string | undefined
  let child_event: BaseEvent | undefined

  bus.on(MainEvent, (event) => {
    handler_called = true
    handler_bus_name = event.event_bus?.name
    assert.equal(Reflect.get(event, 'bus'), undefined)
    assert.equal('bus' in event, false)

    // Should be able to dispatch child events from the current event.
    child_event = event.emit(ChildEvent({}))
  })

  bus.on(ChildEvent, () => {})

  bus.emit(MainEvent({}))
  await bus.waitUntilIdle()

  assert.equal(handler_called, true)
  assert.equal(handler_bus_name, 'TestBus')
  assert.ok(child_event, 'child event should have been dispatched via event.emit')
  assert.equal(child_event!.event_type, 'ChildEvent')
})

test('legacy bus property is not exposed inside handlers', async () => {
  const bus = new EventBus('NoLegacyEventBusPropertyBus')
  let legacy_bus_value: unknown = 'unset'
  let has_legacy_bus = true

  bus.on(MainEvent, (event) => {
    legacy_bus_value = Reflect.get(event, 'bus')
    has_legacy_bus = 'bus' in event
  })

  await bus.emit(MainEvent({})).now()
  assert.equal(legacy_bus_value, undefined)
  assert.equal(has_legacy_bus, false)
})

test('event.event_bus is set for child events emitted in handler', async () => {
  const bus = new EventBus('EventBusPropertyFallbackBus')
  let child_bus_name: string | undefined
  let child_legacy_bus_value: unknown = 'unset'

  bus.on(MainEvent, (event) => {
    const child = event.emit(ChildEvent({}))
    child_bus_name = child.event_bus!.name
    child_legacy_bus_value = Reflect.get(child, 'bus')
  })
  bus.on(ChildEvent, () => {})

  await bus.emit(MainEvent({})).now()
  assert.equal(child_bus_name, 'EventBusPropertyFallbackBus')
  assert.equal(child_legacy_bus_value, undefined)
})

test('event.event_bus is absent on detached events', async () => {
  const bus = new EventBus('EventBusPropertyDetachedBus')
  bus.on(MainEvent, () => {})

  const original = bus.emit(MainEvent({}))
  await original.now()

  const detached = BaseEvent.fromJSON(original.toJSON())
  assert.equal(detached.event_bus, undefined)
  assert.equal(Reflect.get(detached, 'bus'), undefined)
  assert.equal('bus' in detached, false)
  assert.deepEqual(detached.event_path, [bus.label])
})

test('event.event_bus is available outside handler context', async () => {
  const bus = new EventBus('EventBusPropertyOutsideHandlerBus')
  const event = bus.emit(MainEvent({}))
  await event.now()

  assert.equal(event.event_bus!.name, 'EventBusPropertyOutsideHandlerBus')
  assert.equal(Reflect.get(event, 'bus'), undefined)
})

test('event.event_bus returns correct bus when multiple buses exist', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  let handler1_bus_name: string | undefined
  let handler2_bus_name: string | undefined

  bus1.on(MainEvent, (event) => {
    handler1_bus_name = event.event_bus?.name
  })

  bus2.on(MainEvent, (event) => {
    handler2_bus_name = event.event_bus?.name
  })

  bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()

  bus2.emit(MainEvent({}))
  await bus2.waitUntilIdle()

  assert.equal(handler1_bus_name, 'Bus1')
  assert.equal(handler2_bus_name, 'Bus2')
})

test('event.event_bus reflects the currently-processing bus when forwarded', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  // Forward all events from bus1 to bus2
  bus1.on('*', bus2.emit)

  let bus2_handler_bus_name: string | undefined

  bus2.on(MainEvent, (event) => {
    bus2_handler_bus_name = event.event_bus?.name
  })

  const event = bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()
  await bus2.waitUntilIdle()

  // The handler on bus2 should see bus2 as event.event_bus, not bus1
  assert.equal(bus2_handler_bus_name, 'Bus2')
  assert.deepEqual(event.event_path, [bus1.label, bus2.label])
})

test('event.event_bus in nested handlers sees the same bus', async () => {
  const bus = new EventBus('MainBus')

  let outer_bus_name: string | undefined
  let inner_bus_name: string | undefined

  bus.on(MainEvent, async (event) => {
    outer_bus_name = event.event_bus?.name

    // Dispatch child using event.emit.
    const child = event.emit(ChildEvent({}))
    await child.now()
  })

  bus.on(ChildEvent, (event) => {
    inner_bus_name = event.event_bus?.name
  })

  const parent = bus.emit(MainEvent({}))
  await parent.now()

  assert.equal(outer_bus_name, 'MainBus')
  assert.equal(inner_bus_name, 'MainBus')
})

test('event.emit awaited children pass explicit handler context to immediate processing', async () => {
  const bus = new EventBus('ExplicitEventEmitHandlerContextBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const child_process_handler_contexts: boolean[] = []
  const original_process_event_immediately = EventBus.prototype._processEventImmediately

  EventBus.prototype._processEventImmediately = function <T extends BaseEvent>(event: T, handler_result?: EventResult): Promise<T> {
    if (event.event_type === 'ChildEvent') {
      child_process_handler_contexts.push(handler_result?.status === 'started')
    }
    return original_process_event_immediately.call(this, event, handler_result) as Promise<T>
  }

  try {
    bus.on(MainEvent, async (event) => {
      const child = event.emit(ChildEvent({}))
      await child.now()
    })
    bus.on(ChildEvent, () => 'child-ok')

    await bus.emit(MainEvent({})).now()
  } finally {
    EventBus.prototype._processEventImmediately = original_process_event_immediately
  }

  assert.deepEqual(child_process_handler_contexts, [true])
})

test('event.emit sets parent-child relationships through 3 levels', async () => {
  const bus = new EventBus('MainBus')

  const execution_order: string[] = []
  let child_ref: BaseEvent | undefined
  let grandchild_ref: BaseEvent | undefined

  bus.on(MainEvent, async (event) => {
    execution_order.push('parent_start')
    assert.equal(event.event_bus?.name, 'MainBus')

    child_ref = event.emit(ChildEvent({}))
    await child_ref.now()

    execution_order.push('parent_end')
  })

  bus.on(ChildEvent, async (event) => {
    execution_order.push('child_start')
    assert.equal(event.event_bus?.name, 'MainBus')

    grandchild_ref = event.emit(GrandchildEvent({}))
    await grandchild_ref.now()

    execution_order.push('child_end')
  })

  bus.on(GrandchildEvent, (event) => {
    execution_order.push('grandchild_start')
    assert.equal(event.event_bus?.name, 'MainBus')
    execution_order.push('grandchild_end')
  })

  const parent_event = bus.emit(MainEvent({}))
  await parent_event.now()

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

test('event.emit with forwarding: child dispatch goes to the correct bus', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  // Forward all events from bus1 to bus2
  bus1.on('*', bus2.emit)

  let child_handler_bus_name: string | undefined

  // Handlers only on bus2
  bus2.on(MainEvent, async (event) => {
    // Handler runs on bus2 (forwarded from bus1)
    assert.equal(event.event_bus?.name, 'Bus2')

    // Child dispatched via event.emit should go to bus2.
    const child = event.emit(ChildEvent({}))
    await child.now()
  })

  bus2.on(ChildEvent, (event) => {
    child_handler_bus_name = event.event_bus?.name
  })

  bus1.emit(MainEvent({}))
  await bus1.waitUntilIdle()
  await bus2.waitUntilIdle()

  // Child handler should have seen bus2
  assert.equal(child_handler_bus_name, 'Bus2')
})

test('event.event_bus is set on the event after dispatch (outside handler)', async () => {
  const bus = new EventBus('TestBus')

  // Before dispatch, bus is not set
  const raw_event = MainEvent({})
  assert.equal(raw_event.event_bus, undefined)

  // After dispatch, bus is set on the original event
  const dispatched = bus.emit(raw_event)
  assert.ok(dispatched.event_bus, 'event.event_bus should be set after dispatch')

  await bus.waitUntilIdle()
})

test('event.emit from handler correctly attributes event_emitted_by_handler_id', async () => {
  const bus = new EventBus('TestBus')

  bus.on(MainEvent, (event) => {
    event.emit(ChildEvent({}))
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
  assert.ok(child!.event_emitted_by_handler_id, 'event_emitted_by_handler_id should be set on child events dispatched via event.emit')

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
    event.emit(child)
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
    event.emit(LineageChildEvent({}))
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
    event.emit(LineageChildEvent({}))
  })

  bus.on(LineageChildEvent, (event) => {
    event.emit(LineageGrandchildEvent({}))
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
  await parent_event.now()
  await unrelated_event.now()

  assert.equal(bus.eventIsChildOf(unrelated_event, parent_event), false)
  assert.equal(bus.eventIsParentOf(parent_event, unrelated_event), false)
})

// Folded from events_suck.test.ts to keep test layout class-based.
test('events_suck.wrap builds imperative methods for emitting events', async () => {
  const bus = new EventBus('EventsSuckBus')
  const CreateEvent = BaseEvent.extend('EventsSuckCreateEvent', {
    name: z.string(),
    age: z.number(),
    nickname: z.string().nullable().optional(),
    event_result_type: z.string(),
  })
  const UpdateEvent = BaseEvent.extend('EventsSuckUpdateEvent', {
    id: z.string(),
    age: z.number().nullable().optional(),
    source: z.string().nullable().optional(),
    event_result_type: z.boolean(),
  })

  bus.on(CreateEvent, async (event) => {
    assert.equal(event.nickname, 'bobby')
    return `user-${event.age}`
  })

  bus.on(UpdateEvent, async (event) => {
    assert.equal(event.source, 'sync')
    return event.age === 46
  })

  const SDKClient = events_suck.wrap('SDKClient', {
    create: CreateEvent,
    update: UpdateEvent,
  })
  const client = new SDKClient(bus)

  const user_id = await client.create({ name: 'bob', age: 45 }, { nickname: 'bobby' })
  const updated = await client.update({ id: user_id ?? 'fallback-id', age: 46 }, { source: 'sync' })

  assert.equal(user_id, 'user-45')
  assert.equal(updated, true)
})

test('events_suck.make_events works with inline handlers', async () => {
  class LegacyService {
    calls: Array<[string, Record<string, unknown>]> = []

    create(id: string | null, name: string, age: number): string {
      this.calls.push(['create', { id, name, age }])
      return `${name}-${age}`
    }

    update(id: string, name?: string | null, age?: number | null, extra?: Record<string, unknown>): boolean {
      this.calls.push(['update', { id, name, age, ...(extra ?? {}) }])
      return true
    }
  }

  const ping_user = (user_id: string): string => `pong:${user_id}`
  const service = new LegacyService()

  const create_from_payload = (payload: { id: string | null; name: string; age: number }): string => {
    return service.create(payload.id, payload.name, payload.age)
  }

  const update_from_payload = (payload: { id: string; name?: string | null; age?: number | null } & Record<string, unknown>): boolean => {
    const { id, name, age, ...extra } = payload
    return service.update(id, name, age, extra)
  }

  const ping_from_payload = (payload: { user_id: string }): string => ping_user(payload.user_id)

  const events = events_suck.make_events({
    FooBarAPICreateEvent: create_from_payload,
    FooBarAPIUpdateEvent: update_from_payload,
    FooBarAPIPingEvent: ping_from_payload,
  })

  const bus = new EventBus('LegacyBus')
  bus.on(events.FooBarAPICreateEvent, (event) => create_from_payload(event))
  bus.on(events.FooBarAPIUpdateEvent, (event) => update_from_payload(event))
  bus.on(events.FooBarAPIPingEvent, (event) => ping_from_payload(event))

  const created = await bus
    .emit(events.FooBarAPICreateEvent({ id: null, name: 'bob', age: 45 }))
    .now({ first_result: true })
    .eventResult()
  assert.ok(created !== undefined)
  const updated = await bus
    .emit(events.FooBarAPIUpdateEvent({ id: created, age: 46, source: 'sync' }))
    .now({ first_result: true })
    .eventResult()
  const user_id = 'e692b6cb-ae63-773b-8557-3218f7ce5ced'
  const pong = await bus.emit(events.FooBarAPIPingEvent({ user_id })).now({ first_result: true }).eventResult()

  assert.equal(created, 'bob-45')
  assert.equal(updated, true)
  assert.equal(pong, `pong:${user_id}`)
  assert.deepEqual(service.calls[0], ['create', { id: null, name: 'bob', age: 45 }])
  assert.equal(service.calls[1]?.[0], 'update')
  assert.equal(service.calls[1]?.[1].id, 'bob-45')
  assert.equal(service.calls[1]?.[1].age, 46)
  assert.equal(service.calls[1]?.[1].source, 'sync')
})
