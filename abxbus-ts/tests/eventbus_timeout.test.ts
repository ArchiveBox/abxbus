import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  BaseEvent,
  EventBus,
  EventHandlerCancelledError,
  EventHandlerAbortedError,
  EventHandlerTimeoutError,
  RetryTimeoutError,
  retry,
} from '../src/index.js'

const TimeoutEvent = BaseEvent.extend('TimeoutEvent', {})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('event timeout aborts in-flight handler result', async () => {
  const bus = new EventBus('TimeoutBus')

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    return 'slow'
  })

  const event = bus.emit(TimeoutEvent({ event_timeout: 0.02 }))
  await event.done({ raise_if_any: false })

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerAbortedError)
})

test('event timeout does not relabel pre-existing handler timeout errors', async () => {
  const bus = new EventBus('TimeoutPreserveHandlerTimeoutBus', {
    event_handler_concurrency: 'parallel',
  })
  const MixedTimeoutEvent = BaseEvent.extend('MixedTimeoutEvent', {})

  bus.on(
    MixedTimeoutEvent,
    retry({ max_attempts: 1, timeout: 0.01 })(async () => {
      await delay(50)
      return 'handler-timeout'
    })
  )

  bus.on(MixedTimeoutEvent, async () => {
    await delay(200)
    return 'event-timeout'
  })

  const event = bus.emit(MixedTimeoutEvent({ event_timeout: 0.05 }))
  await event.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  const results = Array.from(event.event_results.values())
  assert.equal(results.length, 2)
  assert.ok(results.some((result) => result.error instanceof EventHandlerTimeoutError))
  assert.ok(results.some((result) => result.error instanceof EventHandlerAbortedError))
})

test('handler completes within timeout', async () => {
  const bus = new EventBus('TimeoutOkBus')

  bus.on(TimeoutEvent, async () => {
    await delay(5)
    return 'fast'
  })

  const event = bus.emit(TimeoutEvent({ event_timeout: 0.5 }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'fast')
})

test('event handler errors expose event_result, cause, and timeout metadata', async () => {
  const bus = new EventBus('ErrorMetadataBus')

  const ParentCancelEvent = BaseEvent.extend('ParentCancelEvent', {})
  const PendingChildEvent = BaseEvent.extend('PendingChildEvent', {})
  const ParentAbortEvent = BaseEvent.extend('ParentAbortEvent', {})
  const AbortChildEvent = BaseEvent.extend('AbortChildEvent', {})

  bus.on(TimeoutEvent, async () => {
    await delay(40)
    return 'slow'
  })

  bus.on(PendingChildEvent, async () => {
    await delay(5)
    return 'pending_child'
  })

  let pending_child = null as BaseEvent | null
  bus.on(ParentCancelEvent, async (event) => {
    pending_child = event.bus?.emit(PendingChildEvent({ event_timeout: 0.5 })) ?? null
    await delay(80)
  })

  bus.on(AbortChildEvent, async () => {
    await delay(120)
    return 'abort_child'
  })

  let aborted_child = null as BaseEvent | null
  bus.on(ParentAbortEvent, async (event) => {
    aborted_child = event.bus?.emit(AbortChildEvent({ event_timeout: 0.5 })) ?? null
    await aborted_child?.done({ raise_if_any: false })
  })

  const timeout_event = bus.emit(TimeoutEvent({ event_timeout: 0.02 }))
  await timeout_event.done({ raise_if_any: false })

  const timeout_result = Array.from(timeout_event.event_results.values())[0]
  const timeout_error = timeout_result.error as EventHandlerAbortedError
  assert.ok(timeout_error.cause instanceof Error)
  assert.equal(timeout_error.cause.name, 'EventHandlerTimeoutError')
  assert.equal(timeout_error.event_result, timeout_result)
  assert.equal(timeout_error.timeout_seconds, timeout_event.event_timeout)
  assert.equal(timeout_error.event.event_id, timeout_event.event_id)
  assert.equal(timeout_error.event_type, timeout_event.event_type)
  assert.equal(timeout_error.handler_name, timeout_result.handler_name)
  assert.equal(timeout_error.handler_id, timeout_result.handler_id)
  assert.equal(timeout_error.event_timeout, timeout_event.event_timeout)

  const cancel_parent = bus.emit(ParentCancelEvent({ event_timeout: 0.02 }))
  await cancel_parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  assert.ok(pending_child, 'pending_child should have been emitted')
  const pending_results = Array.from(pending_child!.event_results.values())
  const cancel_parent_result = Array.from(cancel_parent.event_results.values())[0]
  const cancel_parent_error = cancel_parent_result.error as EventHandlerAbortedError
  const cancel_parent_timeout = cancel_parent_error.cause
  assert.ok(cancel_parent_timeout instanceof EventHandlerTimeoutError)
  const pending_error_result = pending_results.find((result) => result.error !== undefined)
  if (
    pending_error_result?.error instanceof EventHandlerCancelledError ||
    pending_error_result?.error instanceof EventHandlerAbortedError
  ) {
    const cancelled_error = pending_error_result.error
    assert.equal(cancelled_error.cause, cancel_parent_timeout)
    assert.equal(cancelled_error.event_result, pending_error_result)
    assert.equal(cancelled_error.event.event_id, pending_child!.event_id)
    assert.equal(cancelled_error.timeout_seconds, pending_child!.event_timeout)
    assert.equal(cancelled_error.event_type, pending_child!.event_type)
    assert.equal(cancelled_error.handler_name, pending_error_result.handler_name)
    assert.equal(cancelled_error.handler_id, pending_error_result.handler_id)
  } else if (pending_error_result?.error instanceof EventHandlerTimeoutError) {
    assert.equal(pending_error_result.error, cancel_parent_timeout)
  } else {
    assert.equal(pending_child!.event_status, 'completed')
  }

  const abort_parent = bus.emit(ParentAbortEvent({ event_timeout: 0.05 }))
  await abort_parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  assert.ok(aborted_child, 'aborted_child should have been emitted')
  const aborted_result = Array.from(aborted_child!.event_results.values()).find(
    (result) => result.error instanceof EventHandlerAbortedError
  )
  assert.ok(aborted_result)
  const aborted_error = aborted_result.error as EventHandlerAbortedError
  const abort_parent_result = Array.from(abort_parent.event_results.values())[0]
  const abort_parent_error = abort_parent_result.error as EventHandlerAbortedError
  const abort_parent_timeout = abort_parent_error.cause
  assert.ok(abort_parent_timeout instanceof EventHandlerTimeoutError)
  assert.equal(aborted_error.cause, abort_parent_timeout)
  assert.equal(aborted_error.event_result, aborted_result)
  assert.equal(aborted_error.event.event_id, aborted_child!.event_id)
  assert.equal(aborted_error.timeout_seconds, aborted_child!.event_timeout)
  assert.equal(aborted_error.event_type, aborted_child!.event_type)
  assert.equal(aborted_error.handler_name, aborted_result.handler_name)
  assert.equal(aborted_error.handler_id, aborted_result.handler_id)
})

test('event timeouts abort handlers across concurrency modes', async () => {
  const event_modes = ['global-serial', 'bus-serial', 'parallel'] as const
  const handler_modes = [
    { label: 'serial', concurrency: 'serial', global_lock: false },
    { label: 'parallel', concurrency: 'parallel', global_lock: false },
    { label: 'serial-global', concurrency: 'serial', global_lock: true },
  ] as const

  for (const event_mode of event_modes) {
    for (const handler_mode of handler_modes) {
      const bus = new EventBus(`Timeout-${event_mode}-${handler_mode.label}`, {
        event_concurrency: event_mode,
        event_handler_concurrency: handler_mode.concurrency,
      })

      const handler = handler_mode.global_lock
        ? retry({ semaphore_scope: 'global', semaphore_name: 'timeout_handler', semaphore_limit: 1 })(async () => {
            await delay(50)
            return 'slow'
          })
        : async () => {
            await delay(50)
            return 'slow'
          }

      bus.on(TimeoutEvent, handler)

      const event = bus.emit(TimeoutEvent({ event_timeout: 0.01 }))
      await event.done({ raise_if_any: false })

      const result = Array.from(event.event_results.values())[0]
      assert.equal(result.status, 'error', `Expected timeout-driven error for event=${event_mode} handler=${handler_mode.label}`)
      assert.ok(
        result.error instanceof EventHandlerAbortedError,
        `Expected EventHandlerAbortedError for event=${event_mode} handler=${handler_mode.label}`
      )

      await bus.waitUntilIdle()
    }
  }
})

test('timeout still marks event failed when other handlers finish', async () => {
  const bus = new EventBus('TimeoutParallelHandlers', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  const results: string[] = []

  bus.on(TimeoutEvent, async () => {
    await delay(1)
    results.push('fast')
    return 'fast'
  })

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    results.push('slow')
    return 'slow'
  })

  const event = bus.emit(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done({ raise_if_any: false })

  const statuses = Array.from(event.event_results.values()).map((result) => result.status)
  assert.ok(statuses.includes('completed'))
  assert.ok(statuses.includes('error'))
  assert.equal(event.event_status, 'completed')
  assert.ok(event.event_errors.length > 0)
  assert.ok(results.includes('fast'))
})

test('event-level timeout marks started parallel handlers as aborted or timed out', async () => {
  const bus = new EventBus('TimeoutParallelAbortedOnlyBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  const ParallelAbortOnlyEvent = BaseEvent.extend('ParallelAbortOnlyEvent', {})
  const deferred = <T>() => {
    let resolve!: (value: T | PromiseLike<T>) => void
    let reject!: (reason?: unknown) => void
    const promise = new Promise<T>((resolve_fn, reject_fn) => {
      resolve = resolve_fn
      reject = reject_fn
    })
    return { promise, resolve, reject }
  }

  const started_a = deferred<void>()
  const started_b = deferred<void>()
  const both_started = deferred<void>()
  let a_started = false
  let b_started = false

  bus.on(ParallelAbortOnlyEvent, async () => {
    a_started = true
    started_a.resolve()
    if (b_started) {
      both_started.resolve()
    }
    await both_started.promise
    await delay(200)
    return 'a'
  })

  bus.on(ParallelAbortOnlyEvent, async () => {
    b_started = true
    started_b.resolve()
    if (a_started) {
      both_started.resolve()
    }
    await both_started.promise
    await delay(200)
    return 'b'
  })

  const event = bus.emit(ParallelAbortOnlyEvent({ event_timeout: 0.03 }))
  await Promise.all([started_a.promise, started_b.promise])
  both_started.resolve()
  await event.done({ raise_if_any: false })

  const results = Array.from(event.event_results.values())
  assert.equal(results.length, 2)
  assert.ok(results.every((result) => result.status === 'error'))
  assert.ok(results.every((result) => result.error instanceof EventHandlerAbortedError || result.error instanceof EventHandlerTimeoutError))
  assert.ok(!results.some((result) => result.error instanceof EventHandlerCancelledError))
})

test('slow event warning fires when event exceeds event_slow_timeout', async () => {
  const bus = new EventBus('SlowEventWarnBus', {
    event_slow_timeout: 0.01,
    event_handler_slow_timeout: null,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.emit(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event processing')),
    'Expected slow event warning'
  )
})

test('slow handler warning fires when handler runs long', async () => {
  const bus = new EventBus('SlowHandlerWarnBus', {
    event_handler_slow_timeout: 0.01,
    event_slow_timeout: null,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.emit(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event handler')),
    'Expected slow handler warning'
  )
})

test('slow handler and slow event warnings can both fire', async () => {
  const bus = new EventBus('SlowComboWarnBus', {
    event_handler_slow_timeout: 0.01,
    event_slow_timeout: 0.01,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.emit(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event handler')),
    'Expected slow handler warning'
  )
  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event processing')),
    'Expected slow event warning'
  )
})

test('event-level concurrency overrides do not bypass timeout aborts', async () => {
  const bus = new EventBus('TimeoutEventOverrideBus', {
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'serial',
  })

  bus.on(
    TimeoutEvent,
    retry({ semaphore_scope: 'global', semaphore_name: 'timeout_override_event', semaphore_limit: 1 })(async () => {
      await delay(50)
      return 'slow'
    })
  )

  const event = bus.emit(
    TimeoutEvent({
      event_timeout: 0.01,
      event_concurrency: 'parallel',
      event_handler_concurrency: 'parallel',
    })
  )
  await event.done({ raise_if_any: false })

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerAbortedError)
})

test('retry-based handler locks do not bypass timeouts', async () => {
  const bus = new EventBus('TimeoutHandlerOverrideBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  bus.on(
    TimeoutEvent,
    retry({ semaphore_scope: 'global', semaphore_name: 'timeout_override_handler', semaphore_limit: 1 })(async () => {
      await delay(50)
      return 'slow'
    })
  )

  bus.on(
    TimeoutEvent,
    retry({ semaphore_scope: 'global', semaphore_name: 'timeout_override_handler', semaphore_limit: 1 })(async () => {
      await delay(1)
      return 'fast'
    })
  )

  const event = bus.emit(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done({ raise_if_any: false })

  const statuses = Array.from(event.event_results.values()).map((result) => result.status)
  assert.ok(statuses.includes('error'))
})

test('forwarded event timeout aborts apply across buses', async () => {
  const bus_a = new EventBus('TimeoutForwardA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('TimeoutForwardB', { event_concurrency: 'bus-serial' })

  bus_a.on(TimeoutEvent, async (event) => {
    bus_b.emit(event)
  })

  bus_b.on(TimeoutEvent, async () => {
    await delay(50)
    return 'slow'
  })

  const event = bus_a.emit(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done({ raise_if_any: false })

  const results = Array.from(event.event_results.values())
  const bus_b_result = results.find((result) => result.eventbus_id === bus_b.id)
  assert.ok(bus_b_result)
  assert.equal(bus_b_result?.status, 'error')
  assert.ok(bus_b_result?.error instanceof EventHandlerAbortedError)
})

test('queue-jump awaited child timeout aborts still fire across buses', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutParentEvent', {})
  const ChildEvent = BaseEvent.extend('TimeoutChildEvent', {})

  const bus_a = new EventBus('TimeoutQueueJumpA', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('TimeoutQueueJumpB', { event_concurrency: 'global-serial' })

  let child_ref = null as InstanceType<typeof ChildEvent> | null

  bus_b.on(ChildEvent, async () => {
    await delay(50)
    return 'slow'
  })

  bus_a.on(ParentEvent, async (event) => {
    // Use scoped bus emit to set parent tracking (event_parent_id, event_emitted_by_handler_id),
    // then also dispatch on bus_b for cross-bus handler execution.
    // Without parent tracking, _processEventImmediately can't detect the queue-jump context
    // and falls back to eventCompleted(), which deadlocks with global-serial.
    const child = event.bus?.emit(ChildEvent({ event_timeout: 0.01 }))!
    bus_b.emit(child)
    child_ref = child
    await child.done()
  })

  const parent = bus_a.emit(ParentEvent({ event_timeout: 0.5 }))
  await parent.done({ raise_if_any: false })

  assert.ok(child_ref)
  const child_results = Array.from(child_ref!.event_results.values())
  const aborted_result = child_results.find((result) => result.error instanceof EventHandlerAbortedError)
  assert.ok(aborted_result)
})

const STEP1_HANDLER_MODES = [
  { label: 'serial', global_lock: false },
  { label: 'serial-global', global_lock: true },
] as const

const get_handler_lock = (bus: EventBus, event: BaseEvent) => {
  const lock = event._getHandlerLock(bus.event_handler_concurrency)
  if (!lock) {
    throw new Error('expected handler lock')
  }
  return lock
}

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: parent timeout while reacquire waits behind third serial handler is lock-safe [${handler_mode.label}]`, async () => {
    const ParentEvent = BaseEvent.extend(`TimeoutContentionParent-${handler_mode.label}`, {})
    const ChildEvent = BaseEvent.extend(`TimeoutContentionChild-${handler_mode.label}`, {})

    const bus = new EventBus(`TimeoutContentionBus-${handler_mode.label}`, {
      event_concurrency: 'bus-serial',
      event_handler_concurrency: 'serial',
    })

    const parent = ParentEvent({ event_timeout: 0.01 })
    const lock = get_handler_lock(bus, parent)
    const baseline_in_use = lock.in_use
    const withGlobalLock = <T extends (...args: any[]) => any>(fn: T): T =>
      handler_mode.global_lock
        ? retry({ semaphore_scope: 'global', semaphore_name: `timeout_contention_${handler_mode.label}`, semaphore_limit: 1 })(fn)
        : fn

    bus.on(
      ChildEvent,
      withGlobalLock(async () => {
        await delay(2)
        return 'child_done'
      })
    )

    bus.on(
      ParentEvent,
      withGlobalLock(async (event) => {
        const child = event.bus?.emit(ChildEvent({ event_timeout: 0.2, event_handler_concurrency: 'parallel' }))!
        await child.done()
        return 'parent_main'
      })
    )

    // This handler queues behind parent_main, then holds the serial lock
    // while parent_main is trying to reclaim after child.done() completes.
    bus.on(
      ParentEvent,
      withGlobalLock(async () => {
        await delay(40)
        return 'parent_blocker'
      })
    )

    bus.emit(parent)
    await parent.done({ raise_if_any: false })
    await bus.waitUntilIdle()

    const parent_results = Array.from(parent.event_results.values())
    const aborted_results = parent_results.filter((result) => result.error instanceof EventHandlerAbortedError)
    assert.ok(aborted_results.length >= 1, `expected at least one aborted result in ${handler_mode.label}`)
    assert.equal(lock.in_use, baseline_in_use)
  })
}

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: nested queue-jump with timeout cancellation remains lock-safe [${handler_mode.label}]`, async () => {
    const ParentEvent = BaseEvent.extend(`NestedPermitParent-${handler_mode.label}`, {})
    const ChildEvent = BaseEvent.extend(`NestedPermitChild-${handler_mode.label}`, {})
    const GrandchildEvent = BaseEvent.extend(`NestedPermitGrandchild-${handler_mode.label}`, {})
    const QueuedSiblingEvent = BaseEvent.extend(`NestedPermitQueuedSibling-${handler_mode.label}`, {})
    const TailEvent = BaseEvent.extend(`NestedPermitTail-${handler_mode.label}`, {})

    const bus = new EventBus(`NestedPermitBus-${handler_mode.label}`, {
      event_concurrency: 'bus-serial',
      event_handler_concurrency: 'serial',
    })
    const parent = ParentEvent({ event_timeout: 0.01 })
    const lock = get_handler_lock(bus, parent)
    const baseline_in_use = lock.in_use
    const withGlobalLock = <T extends (...args: any[]) => any>(fn: T): T =>
      handler_mode.global_lock
        ? retry({ semaphore_scope: 'global', semaphore_name: `timeout_nested_${handler_mode.label}`, semaphore_limit: 1 })(fn)
        : fn

    let queued_sibling_runs = 0
    let tail_runs = 0
    let queued_sibling_ref = null as InstanceType<typeof QueuedSiblingEvent> | null

    bus.on(
      GrandchildEvent,
      withGlobalLock(async () => {
        await delay(1)
        return 'grandchild_done'
      })
    )

    bus.on(
      ChildEvent,
      withGlobalLock(async (event) => {
        const grandchild = event.bus?.emit(GrandchildEvent({ event_timeout: 0.2 }))!
        await grandchild.done()
        await delay(40)
        return 'child_done'
      })
    )

    bus.on(
      QueuedSiblingEvent,
      withGlobalLock(async () => {
        queued_sibling_runs += 1
        return 'queued_sibling_done'
      })
    )

    bus.on(
      ParentEvent,
      withGlobalLock(async (event) => {
        queued_sibling_ref = event.bus?.emit(QueuedSiblingEvent({ event_timeout: 0.2 }))!
        const child = event.bus?.emit(ChildEvent({ event_timeout: 0.02 }))!
        await child.done({ raise_if_any: false })
        await delay(40)
      })
    )

    bus.on(
      TailEvent,
      withGlobalLock(async () => {
        tail_runs += 1
        return 'tail_done'
      })
    )

    const dispatched_parent = bus.emit(ParentEvent({ event_timeout: 0.03 }))
    await dispatched_parent.done({ raise_if_any: false })
    await bus.waitUntilIdle()

    const parent_result = Array.from(dispatched_parent.event_results.values())[0]
    assert.equal(parent_result.status, 'error')
    assert.ok(parent_result.error instanceof EventHandlerAbortedError)

    assert.ok(queued_sibling_ref)
    assert.equal(queued_sibling_runs, 0)
    const queued_sibling_results = Array.from(queued_sibling_ref!.event_results.values())
    assert.ok(queued_sibling_results.some((result) => result.error instanceof EventHandlerCancelledError))

    assert.equal(lock.in_use, baseline_in_use)

    const tail = bus.emit(TailEvent({ event_timeout: 0.05 }))
    const tail_completed = await Promise.race([tail.done().then(() => true), delay(100).then(() => false)])
    assert.equal(tail_completed, true)
    assert.equal(tail_runs, 1)
    assert.equal(lock.in_use, baseline_in_use)
  })
}

test('parent timeout cancels pending child handler results under serial handler lock', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutCancelParentEvent', {})
  const ChildEvent = BaseEvent.extend('TimeoutCancelChildEvent', {})

  const bus = new EventBus('TimeoutCancelBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  let child_runs = 0

  bus.on(ChildEvent, async () => {
    child_runs += 1
    await delay(30)
    return 'first'
  })

  bus.on(ChildEvent, async () => {
    child_runs += 1
    await delay(10)
    return 'second'
  })

  bus.on(ParentEvent, async (event) => {
    event.bus?.emit(ChildEvent({ event_timeout: 0.2 }))
    await delay(50)
  })

  const parent = bus.emit(ParentEvent({ event_timeout: 0.01 }))
  await parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  const child = parent.event_children[0]
  assert.ok(child)

  assert.equal(child_runs, 0)

  const cancelled_results = Array.from(child.event_results.values()).filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.ok(cancelled_results.length > 0)
})

test('retry timeout cancels pending child handler results', async () => {
  const ParentEvent = BaseEvent.extend('RetryTimeoutCancelParentEvent', {})
  const ChildEvent = BaseEvent.extend('RetryTimeoutCancelChildEvent', {})

  const bus = new EventBus('RetryTimeoutCancelBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  bus.on(ChildEvent, async () => {
    await delay(20)
    return 'child_done'
  })

  bus.on(
    ParentEvent,
    retry({ max_attempts: 1, timeout: 0.01 })(async (event) => {
      event.bus?.emit(ChildEvent({ event_timeout: 0.2 }))
      await delay(50)
      return 'parent_done'
    })
  )

  const parent = bus.emit(ParentEvent({ event_timeout: 0.5 }))
  await parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  const parent_result = Array.from(parent.event_results.values())[0]
  assert.equal(parent_result.status, 'error')
  assert.ok(parent_result.error instanceof EventHandlerTimeoutError)
  assert.ok(parent_result.error.cause instanceof RetryTimeoutError)

  const child = parent.event_children[0]
  assert.ok(child)
  const cancelled_results = Array.from(child.event_results.values()).filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.ok(cancelled_results.length > 0)
})

test('handler_timeout stops in-flight retries and cancels child events', async () => {
  const ParentEvent = BaseEvent.extend('RetryTimeoutHandlerTimeoutParentEvent', {})
  const ChildEvent = BaseEvent.extend('RetryTimeoutHandlerTimeoutChildEvent', {})

  const bus = new EventBus('RetryTimeoutHandlerTimeoutBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })

  let child_started = 0
  bus.on(ChildEvent, async () => {
    child_started += 1
    await delay(500)
    return 'child_done'
  })

  let child_ref = null as InstanceType<typeof ChildEvent> | null
  let emitted = false
  let attempts_started = 0

  const handler = retry({ max_attempts: 10, timeout: 0.1 })(async (event) => {
    attempts_started += 1
    if (!emitted) {
      emitted = true
      child_ref = event.bus?.emit(ChildEvent({ event_timeout: 2 })) ?? null
      await delay(10)
    }
    await delay(200)
    return 'parent_attempt_done'
  })

  const handler_entry = bus.on(ParentEvent, handler, { handler_timeout: 0.35 })

  const parent = bus.emit(ParentEvent({ event_timeout: 2 }))
  await parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  const parent_result = parent.event_results.get(handler_entry.id)
  assert.ok(parent_result)
  assert.equal(parent_result.status, 'error')
  assert.ok(parent_result.error instanceof EventHandlerTimeoutError)
  assert.ok(attempts_started >= 2)
  assert.ok(attempts_started < 10)

  assert.ok(child_ref)
  assert.ok(child_started > 0)
  const cancelled_results = Array.from(child_ref!.event_results.values()).filter(
    (result) => result.error instanceof EventHandlerCancelledError || result.error instanceof EventHandlerAbortedError
  )
  assert.ok(cancelled_results.length > 0)
})

test('event_timeout null falls back to bus default', async () => {
  const bus = new EventBus('TimeoutDefaultBus', { event_timeout: 0.01 })

  bus.on(TimeoutEvent, async (_event: BaseEvent) => {
    await delay(50)
    return 'slow'
  })

  const event = bus.emit(TimeoutEvent({ event_timeout: null }))
  await event.done({ raise_if_any: false })

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerAbortedError)
})

test('bus default null disables timeouts when event_timeout is null', async () => {
  const bus = new EventBus('TimeoutDisabledBus', { event_timeout: null })

  bus.on(TimeoutEvent, async () => {
    await delay(20)
    return 'ok'
  })

  const event = bus.emit(TimeoutEvent({ event_timeout: null }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'ok')
})

test('multi-level timeout cascade with mixed cancellations', async () => {
  const TopEvent = BaseEvent.extend('TimeoutCascadeTop', {})
  const QueuedChildEvent = BaseEvent.extend('TimeoutCascadeQueuedChild', {})
  const AwaitedChildEvent = BaseEvent.extend('TimeoutCascadeAwaitedChild', {})
  const ImmediateGrandchildEvent = BaseEvent.extend('TimeoutCascadeImmediateGrandchild', {})
  const QueuedGrandchildEvent = BaseEvent.extend('TimeoutCascadeQueuedGrandchild', {})

  const bus = new EventBus('TimeoutCascadeBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  let queued_child = null as InstanceType<typeof QueuedChildEvent> | null
  let awaited_child = null as InstanceType<typeof AwaitedChildEvent> | null
  let immediate_grandchild = null as InstanceType<typeof ImmediateGrandchildEvent> | null
  let queued_grandchild = null as InstanceType<typeof QueuedGrandchildEvent> | null

  let queued_child_runs = 0
  let immediate_grandchild_runs = 0
  let queued_grandchild_runs = 0

  const queued_child_fast = async () => {
    queued_child_runs += 1
    await delay(5)
    return 'queued_fast'
  }

  const queued_child_slow = async () => {
    queued_child_runs += 1
    await delay(50)
    return 'queued_slow'
  }

  const awaited_child_fast = async () => {
    await delay(5)
    return 'awaited_fast'
  }

  const awaited_child_slow = async (event: BaseEvent) => {
    queued_grandchild = event.bus?.emit(QueuedGrandchildEvent({ event_timeout: 0.2 }))!
    immediate_grandchild = event.bus?.emit(ImmediateGrandchildEvent({ event_timeout: 0.2 }))!
    await immediate_grandchild.done()
    await delay(100)
    return 'awaited_slow'
  }

  const immediate_grandchild_slow = async () => {
    immediate_grandchild_runs += 1
    await delay(50)
    return 'immediate_grandchild_slow'
  }

  const immediate_grandchild_fast = async () => {
    immediate_grandchild_runs += 1
    await delay(10)
    return 'immediate_grandchild_fast'
  }

  const queued_grandchild_slow = async () => {
    queued_grandchild_runs += 1
    await delay(50)
    return 'queued_grandchild_slow'
  }

  const queued_grandchild_fast = async () => {
    queued_grandchild_runs += 1
    await delay(10)
    return 'queued_grandchild_fast'
  }

  bus.on(QueuedChildEvent, queued_child_fast)
  bus.on(QueuedChildEvent, queued_child_slow)
  bus.on(AwaitedChildEvent, awaited_child_fast)
  bus.on(AwaitedChildEvent, awaited_child_slow)
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_slow)
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_fast)
  bus.on(QueuedGrandchildEvent, queued_grandchild_slow)
  bus.on(QueuedGrandchildEvent, queued_grandchild_fast)

  bus.on(TopEvent, async (event) => {
    queued_child = event.bus?.emit(QueuedChildEvent({ event_timeout: 0.2 }))!
    awaited_child = event.bus?.emit(AwaitedChildEvent({ event_timeout: 0.03 }))!
    await awaited_child.done({ raise_if_any: false })
    await delay(80)
  })

  const top = bus.emit(TopEvent({ event_timeout: 0.04 }))
  await top.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  const top_result = Array.from(top.event_results.values())[0]
  assert.equal(top_result.status, 'error')
  assert.ok(top_result.error instanceof EventHandlerAbortedError)

  assert.ok(queued_child)
  const queued_results = Array.from(queued_child!.event_results.values())
  assert.equal(queued_child_runs, 0)
  assert.ok(queued_results.length >= 2)
  for (const result of queued_results) {
    assert.equal(result.status, 'error')
    assert.ok(result.error instanceof EventHandlerCancelledError)
    assert.ok((result.error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError)
  }

  assert.ok(awaited_child)
  const awaited_results = Array.from(awaited_child!.event_results.values())
  const awaited_completed = awaited_results.filter((result) => result.status === 'completed')
  const awaited_aborted = awaited_results.filter((result) => result.error instanceof EventHandlerAbortedError)
  assert.equal(awaited_completed.length, 1)
  assert.equal(awaited_aborted.length, 1)

  assert.ok(immediate_grandchild)
  const immediate_results = Array.from(immediate_grandchild!.event_results.values())
  // With serial handler concurrency (no longer bypassed during queue-jump),
  // only the first grandchild handler starts before the awaited child's 30ms timeout fires.
  // The second handler is still pending (waiting for lock) → cancelled.
  // The first handler was already started → aborted (EventHandlerAbortedError).
  assert.equal(immediate_grandchild_runs, 1)
  const immediate_aborted = immediate_results.filter((result) => result.error instanceof EventHandlerAbortedError)
  assert.equal(immediate_aborted.length, 1)
  const immediate_cancelled = immediate_results.filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.equal(immediate_cancelled.length, 1)

  assert.ok(queued_grandchild)
  const queued_grandchild_results = Array.from(queued_grandchild!.event_results.values())
  assert.equal(queued_grandchild_runs, 0)
  const queued_cancelled = queued_grandchild_results.filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.ok(queued_cancelled.length >= 2)
})

// =============================================================================
// Three-level timeout cascade (mirrors Python test_eventbus_timeout.py)
//
// This test creates a deep event hierarchy:
//   TopEvent (250ms timeout)
//     ├── ChildEvent (80ms timeout) — awaited by top_handler_main
//     │     ├── GrandchildEvent (35ms timeout) — awaited by child_handler
//     │     │     └── 5 handlers (parallel): 3 slow (timeout), 2 fast (complete)
//     │     └── QueuedGrandchildEvent — emitted but NOT awaited, stays in queue
//     │           └── 1 handler: never runs, CANCELLED when child_handler times out
//     └── SiblingEvent — emitted but NOT awaited, stays in queue
//           └── 1 handler: never runs, CANCELLED when top_handler_main times out
//
// KEY MECHANIC: When a child event is awaited via event.done() inside a handler,
// it triggers "queue-jumping" via _processEventImmediately (cross-bus).
// Queue-jumped events use yield-and-reacquire: the parent handler's lock is
// temporarily released so child handlers can acquire it normally. This means
// child handlers run SERIALLY on a serial handler bus (respecting concurrency limits).
// Non-awaited child events stay in the pending_event_queue and are blocked by
// immediate_processing_stack_depth > 0 (runloop is paused during queue-jump).
//
// TIMEOUT BEHAVIOR: Each handler gets its OWN timeout window starting from when
// that handler begins execution — NOT from when the event was dispatched.
// With serial handlers, each timeout starts when the handler acquires the lock.
//
// CANCELLATION CASCADE: When a handler times out, bus._cancelPendingChildProcessing()
// walks the event's children tree and marks any "pending" handler results as
// EventHandlerCancelledError. Only "pending" results are cancelled — handlers
// that already started ("started" status) continue running in the background.
// =============================================================================

test('three-level timeout cascade with per-level timeouts and cascading cancellation', async () => {
  const TopEvent = BaseEvent.extend('Cascade3LTop', {})
  const ChildEvent = BaseEvent.extend('Cascade3LChild', {})
  const GrandchildEvent = BaseEvent.extend('Cascade3LGrandchild', {})
  const QueuedGrandchildEvent = BaseEvent.extend('Cascade3LQueuedGC', {})
  const SiblingEvent = BaseEvent.extend('Cascade3LSibling', {})

  const bus = new EventBus('Cascade3LevelBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  const execution_log: string[] = []
  let child_ref = null as InstanceType<typeof ChildEvent> | null
  let grandchild_ref = null as InstanceType<typeof GrandchildEvent> | null
  let queued_grandchild_ref = null as InstanceType<typeof QueuedGrandchildEvent> | null
  let sibling_ref = null as InstanceType<typeof SiblingEvent> | null

  // ── GrandchildEvent handlers ──────────────────────────────────────────
  // These run SERIALLY because queue-jumped events respect the serial
  // handler lock (yield-and-reacquire). Each handler gets its own 35ms
  // timeout window starting from when that handler acquires the lock.
  //
  // Serial order: a(35ms timeout) → b(sync) → c(35ms timeout) → d(10ms) → e(35ms timeout)
  // Total time for all 5: ~35+0+35+10+35 = ~115ms (within child's 150ms timeout)

  const gc_handler_a = async () => {
    execution_log.push('gc_a_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_a_end') // should never reach here before assertions
    return 'gc_a_done'
  }

  const gc_handler_b = () => {
    execution_log.push('gc_b_complete')
    return 'gc_b_done'
  }

  const gc_handler_c = async () => {
    execution_log.push('gc_c_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_c_end') // should never reach here before assertions
    return 'gc_c_done'
  }

  const gc_handler_d = async () => {
    execution_log.push('gc_d_start')
    await delay(10) // fast enough to complete within 35ms
    execution_log.push('gc_d_complete')
    return 'gc_d_done'
  }

  const gc_handler_e = async () => {
    execution_log.push('gc_e_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_e_end') // should never reach here before assertions
    return 'gc_e_done'
  }

  // ── QueuedGrandchildEvent handler ─────────────────────────────────────
  // This event is emitted by child_handler but NOT awaited, so it sits in
  // pending_event_queue. When child_handler times out at 80ms,
  // bus._cancelPendingChildProcessing walks ChildEvent.event_children and finds
  // this event still pending → its handler results are marked as cancelled.
  const queued_gc_handler = () => {
    execution_log.push('queued_gc_start') // should never reach here
    return 'queued_gc_done'
  }

  // ── ChildEvent handler ────────────────────────────────────────────────
  // Emits GrandchildEvent (awaited → queue-jump, ~35ms to complete)
  // Emits QueuedGrandchildEvent (NOT awaited → stays in queue)
  // After grandchild completes, sleeps 300ms → times out at 80ms total
  const child_handler = async (event: InstanceType<typeof ChildEvent>) => {
    execution_log.push('child_start')
    grandchild_ref = event.bus?.emit(GrandchildEvent({ event_timeout: 0.035 }))!
    queued_grandchild_ref = event.bus?.emit(QueuedGrandchildEvent({ event_timeout: 0.5 }))!
    // Queue-jump: processes GrandchildEvent immediately via yield-and-reacquire.
    // All 5 GC handlers run serially. Completes in ~115ms (within 150ms child timeout).
    await grandchild_ref.done({ raise_if_any: false })
    execution_log.push('child_after_grandchild')
    await delay(300) // will be interrupted: child started at ~t=0, timeout at 150ms
    execution_log.push('child_end') // should never reach here
    return 'child_done'
  }

  // ── SiblingEvent handler ──────────────────────────────────────────────
  // This event is emitted by top_handler_main but NOT awaited. Stays in
  // pending_event_queue until top_handler_main times out at 250ms →
  // cancelled by bus._cancelPendingChildProcessing.
  const sibling_handler = () => {
    execution_log.push('sibling_start') // should never reach here
    return 'sibling_done'
  }

  // ── TopEvent handlers ─────────────────────────────────────────────────
  // These run SERIALLY (per-event handler lock) because TopEvent is
  // processed by the normal runloop (not queue-jumped). top_handler_fast
  // goes first, completes quickly, then top_handler_main starts.

  const top_handler_fast = async () => {
    execution_log.push('top_fast_start')
    await delay(2)
    execution_log.push('top_fast_complete')
    return 'top_fast_done'
  }

  const top_handler_main = async (event: InstanceType<typeof TopEvent>) => {
    execution_log.push('top_main_start')
    child_ref = event.bus?.emit(ChildEvent({ event_timeout: 0.15 }))!
    sibling_ref = event.bus?.emit(SiblingEvent({ event_timeout: 0.5 }))!
    // Queue-jump: processes ChildEvent immediately (which in turn queue-jumps
    // GrandchildEvent). This entire subtree resolves in ~80ms (child timeout).
    await child_ref.done({ raise_if_any: false })
    execution_log.push('top_main_after_child')
    await delay(300) // will be interrupted: top_handler_main started at ~t=2, timeout at 250ms
    execution_log.push('top_main_end') // should never reach here
    return 'top_main_done'
  }

  // Register handlers (registration order = execution order for serial)
  bus.on(TopEvent, top_handler_fast)
  bus.on(TopEvent, top_handler_main)
  bus.on(ChildEvent, child_handler)
  bus.on(GrandchildEvent, gc_handler_a)
  bus.on(GrandchildEvent, gc_handler_b)
  bus.on(GrandchildEvent, gc_handler_c)
  bus.on(GrandchildEvent, gc_handler_d)
  bus.on(GrandchildEvent, gc_handler_e)
  bus.on(QueuedGrandchildEvent, queued_gc_handler)
  bus.on(SiblingEvent, sibling_handler)

  // ── Dispatch and wait ─────────────────────────────────────────────────
  const top = bus.emit(TopEvent({ event_timeout: 0.25 }))
  await top.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  // ═══════════════════════════════════════════════════════════════════════
  // ASSERTIONS
  // ═══════════════════════════════════════════════════════════════════════

  // ── TopEvent: 2 handler results (1 completed, 1 aborted by event timeout) ──────────
  assert.equal(top.event_status, 'completed')
  assert.ok(top.event_errors.length >= 1, 'TopEvent should have at least 1 error')

  const top_results = Array.from(top.event_results.values())
  assert.equal(top_results.length, 2, 'TopEvent should have 2 handler results')

  const top_fast_result = top_results.find((r) => r.handler_name === 'top_handler_fast')
  assert.ok(top_fast_result, 'top_handler_fast result should exist')
  assert.equal(top_fast_result!.status, 'completed')
  assert.equal(top_fast_result!.result, 'top_fast_done')

  const top_main_result = top_results.find((r) => r.handler_name === 'top_handler_main')
  assert.ok(top_main_result, 'top_handler_main result should exist')
  assert.equal(top_main_result!.status, 'error')
  assert.ok(top_main_result!.error instanceof EventHandlerAbortedError, 'top_handler_main should have been aborted by event timeout')

  // ── ChildEvent: 1 handler result (aborted by event timeout at ~150ms) ────────────────
  assert.ok(child_ref, 'ChildEvent should have been emitted')
  assert.equal(child_ref!.event_status, 'completed')

  const child_results = Array.from(child_ref!.event_results.values())
  assert.equal(child_results.length, 1, 'ChildEvent should have 1 handler result')
  assert.equal(child_results[0].handler_name, 'child_handler')
  assert.equal(child_results[0].status, 'error')
  assert.ok(child_results[0].error instanceof EventHandlerAbortedError, 'child_handler should have been aborted by event timeout')

  // ── GrandchildEvent: 5 handler results (event hard-timeout after first handler starts) ──
  assert.ok(grandchild_ref, 'GrandchildEvent should have been emitted')
  assert.equal(grandchild_ref!.event_status, 'completed')

  const gc_results = Array.from(grandchild_ref!.event_results.values())
  assert.equal(gc_results.length, 5, 'GrandchildEvent should have 5 handler results')

  const gc_aborted = gc_results.filter((result) => result.error instanceof EventHandlerAbortedError)
  assert.equal(gc_aborted.length, 1, 'GrandchildEvent should have exactly one started handler aborted by event timeout')
  const gc_aborted_handler = gc_aborted[0]!.handler_name
  assert.equal(gc_aborted_handler, 'gc_handler_a')

  const gc_cancelled_or_aborted = gc_results.filter(
    (result) => result.error instanceof EventHandlerCancelledError || result.error instanceof EventHandlerAbortedError
  )
  assert.equal(gc_cancelled_or_aborted.length, 5, 'Grandchild handlers should all be cancelled or aborted by hard event timeout')

  // ── QueuedGrandchildEvent: CANCELLED by child_handler timeout ───────
  // This event was emitted but never awaited. It sat in pending_event_queue
  // until child_handler timed out, which triggered bus._cancelPendingChildProcessing
  // to walk ChildEvent.event_children and cancel all pending handlers.
  assert.ok(queued_grandchild_ref, 'QueuedGrandchildEvent should have been emitted')
  assert.equal(queued_grandchild_ref!.event_status, 'completed')

  const queued_gc_results = Array.from(queued_grandchild_ref!.event_results.values())
  assert.equal(queued_gc_results.length, 1, 'QueuedGC should have 1 handler result')
  assert.equal(queued_gc_results[0].status, 'error')
  assert.ok(
    queued_gc_results[0].error instanceof EventHandlerCancelledError,
    'QueuedGC handler should be EventHandlerCancelledError (not timeout — it never ran)'
  )
  // Verify the cancellation error chain: CancelledError.cause → TimeoutError
  assert.ok(
    (queued_gc_results[0].error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError,
    "QueuedGC cancellation should reference the child_handler's timeout as cause"
  )

  // ── SiblingEvent: CANCELLED by top_handler_main timeout ─────────────
  // Same pattern: emitted but never awaited, stays in queue, cancelled when
  // top_handler_main times out and bus._cancelPendingChildProcessing runs.
  assert.ok(sibling_ref, 'SiblingEvent should have been emitted')
  assert.equal(sibling_ref!.event_status, 'completed')

  const sibling_results = Array.from(sibling_ref!.event_results.values())
  assert.equal(sibling_results.length, 1, 'SiblingEvent should have 1 handler result')
  assert.equal(sibling_results[0].status, 'error')
  assert.ok(sibling_results[0].error instanceof EventHandlerCancelledError, 'SiblingEvent handler should be EventHandlerCancelledError')
  assert.ok(
    (sibling_results[0].error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError,
    "SiblingEvent cancellation should reference top_handler_main's timeout as cause"
  )

  // ── Execution log: verify what ran and what didn't ──────────────────
  // These handlers started AND completed:
  assert.ok(execution_log.includes('top_fast_start'), 'top_fast should have started')
  assert.ok(execution_log.includes('top_fast_complete'), 'top_fast should have completed')
  assert.ok(!execution_log.includes('gc_b_complete'), 'gc_b should not have started before hard event timeout')
  assert.ok(!execution_log.includes('gc_d_start'), 'gc_d should not have started before hard event timeout')
  assert.ok(!execution_log.includes('gc_d_complete'), 'gc_d should not have completed before hard event timeout')

  // These handlers started but were interrupted by their own timeout:
  assert.ok(execution_log.includes('gc_a_start'), 'gc_a should have started')
  assert.ok(!execution_log.includes('gc_a_end'), 'gc_a should NOT have finished (timed out)')
  assert.ok(!execution_log.includes('gc_c_start'), 'gc_c should not have started before hard event timeout')
  assert.ok(!execution_log.includes('gc_c_end'), 'gc_c should never have finished')
  assert.ok(!execution_log.includes('gc_e_start'), 'gc_e should not have started before hard event timeout')
  assert.ok(!execution_log.includes('gc_e_end'), 'gc_e should never have finished')

  // These handlers started and progressed, then parent timeout interrupted:
  assert.ok(execution_log.includes('top_main_start'), 'top_main should have started')
  assert.ok(execution_log.includes('child_start'), 'child should have started')
  assert.ok(execution_log.includes('child_after_grandchild'), 'child should have continued after grandchild completed')
  assert.ok(execution_log.includes('top_main_after_child'), 'top_main should have continued after child completed')
  assert.ok(!execution_log.includes('child_end'), 'child should NOT have finished (timed out)')
  assert.ok(!execution_log.includes('top_main_end'), 'top_main should NOT have finished (timed out)')

  // These handlers never ran at all (cancelled before starting):
  assert.ok(!execution_log.includes('queued_gc_start'), 'queued_gc should never have started')
  assert.ok(!execution_log.includes('sibling_start'), 'sibling should never have started')

  // ── Parent-child tree structure ─────────────────────────────────────
  assert.ok(
    top.event_children.some((c) => c.event_id === child_ref!.event_id),
    'ChildEvent should be in TopEvent.event_children'
  )
  assert.ok(
    top.event_children.some((c) => c.event_id === sibling_ref!.event_id),
    'SiblingEvent should be in TopEvent.event_children'
  )
  assert.ok(
    child_ref!.event_children.some((c) => c.event_id === grandchild_ref!.event_id),
    'GrandchildEvent should be in ChildEvent.event_children'
  )
  assert.ok(
    child_ref!.event_children.some((c) => c.event_id === queued_grandchild_ref!.event_id),
    'QueuedGrandchildEvent should be in ChildEvent.event_children'
  )

  // ── Timing invariants ──────────────────────────────────────────────
  // All events should have completion timestamps
  for (const evt of [top, child_ref!, grandchild_ref!, queued_grandchild_ref!, sibling_ref!]) {
    assert.ok(evt.event_completed_at, `${evt.event_type} should have event_completed_at`)
  }
  // All handler results should have started_at and completed_at
  for (const result of top_results) {
    assert.ok(result.started_at, `${result.handler_name} should have started_at`)
    assert.ok(result.completed_at, `${result.handler_name} should have completed_at`)
  }
  for (const result of gc_results) {
    if (!(result.error instanceof EventHandlerCancelledError)) {
      assert.ok(result.started_at, `${result.handler_name} should have started_at`)
    }
    assert.ok(result.completed_at, `${result.handler_name} should have completed_at`)
  }
})

// =============================================================================
// Verify the timeout→cancellation error chain is intact at every level.
// When a parent handler times out and cancels a child's pending handlers,
// the EventHandlerCancelledError.cause must reference the specific
// EventHandlerTimeoutError that caused the cascade. This test creates a
// 2-level chain where each level's cancellation error can be inspected.
// =============================================================================

test('cancellation error chain preserves cause references through hierarchy', async () => {
  const OuterEvent = BaseEvent.extend('ErrorChainOuter', {})
  const InnerEvent = BaseEvent.extend('ErrorChainInner', {})
  const DeepEvent = BaseEvent.extend('ErrorChainDeep', {})

  const bus = new EventBus('ErrorChainBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  let inner_ref = null as InstanceType<typeof InnerEvent> | null
  let deep_ref = null as InstanceType<typeof DeepEvent> | null

  // DeepEvent handler: sleeps long, will be still pending when inner times out
  // Because DeepEvent is emitted but NOT awaited, it stays in the queue.
  const deep_handler = async () => {
    await delay(200)
    return 'deep_done'
  }

  // InnerEvent handler: emits DeepEvent (not awaited), then sleeps long → times out
  const inner_handler = async (event: InstanceType<typeof InnerEvent>) => {
    deep_ref = event.bus?.emit(DeepEvent({ event_timeout: 0.5 }))!
    await delay(200) // interrupted by inner timeout
    return 'inner_done'
  }

  // OuterEvent handler: emits InnerEvent (awaited), then sleeps long → times out
  const outer_handler = async (event: InstanceType<typeof OuterEvent>) => {
    inner_ref = event.bus?.emit(InnerEvent({ event_timeout: 0.04 }))!
    await inner_ref.done({ raise_if_any: false })
    await delay(200) // interrupted by outer timeout
    return 'outer_done'
  }

  bus.on(OuterEvent, outer_handler)
  bus.on(InnerEvent, inner_handler)
  bus.on(DeepEvent, deep_handler)

  const outer = bus.emit(OuterEvent({ event_timeout: 0.15 }))
  await outer.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  // Outer handler was aborted by event-level timeout
  const outer_result = Array.from(outer.event_results.values())[0]
  assert.equal(outer_result.status, 'error')
  assert.ok(outer_result.error instanceof EventHandlerAbortedError)
  // Inner handler was aborted by its own event-level timeout (40ms), not outer's.
  assert.ok(inner_ref)
  const inner_result = Array.from(inner_ref!.event_results.values())[0]
  assert.equal(inner_result.status, 'error')
  assert.ok(inner_result.error instanceof EventHandlerAbortedError)
  const inner_abort = inner_result.error as EventHandlerAbortedError

  // Inner's abort is from InnerEvent's own event_timeout (40ms), not inherited from outer.
  assert.ok(inner_abort.message.includes('event timeout'), 'Inner abort should indicate event timeout')

  // DeepEvent was cancelled when inner_handler timed out.
  // The cancellation error should reference inner_handler's timeout (not outer's).
  assert.ok(deep_ref)
  const deep_result = Array.from(deep_ref!.event_results.values())[0]
  assert.equal(deep_result.status, 'error')
  assert.ok(
    deep_result.error instanceof EventHandlerCancelledError,
    'DeepEvent handler should be cancelled, not timed out (it never started)'
  )
  const deep_cancel = deep_result.error as EventHandlerCancelledError
  assert.ok(deep_cancel.cause instanceof EventHandlerTimeoutError, 'Cancellation should reference parent timeout')
  // The cause should be the INNER handler's timeout, because that's
  // the handler whose bus._cancelPendingChildProcessing actually cancelled DeepEvent.
  assert.ok(
    deep_cancel.cause.message.includes('inner_handler') || deep_cancel.cause.message.includes('child_handler'),
    'cause should reference the handler that directly caused cancellation'
  )
})

// =============================================================================
// When a parent has a timeout but a child has event_timeout: null (no timeout),
// the child's handlers run indefinitely on their own — but if the PARENT times
// out, bus._cancelPendingChildProcessing still cancels any pending child handlers.
// This tests that cancellation works across timeout/no-timeout boundaries.
// =============================================================================

test('parent timeout cancels children that have no timeout of their own', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutBoundaryParent', {})
  const NoTimeoutChild = BaseEvent.extend('TimeoutBoundaryChild', {})

  const bus = new EventBus('TimeoutBoundaryBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
    event_timeout: null, // no bus-level default
  })

  let child_ref = null as InstanceType<typeof NoTimeoutChild> | null
  let child_handler_ran = false

  // Child handler: would run forever but should be cancelled
  const child_slow_handler = async () => {
    child_handler_ran = true
    await delay(500)
    return 'child_done'
  }

  // Parent handler: emits child (not awaited), then sleeps → parent times out
  const parent_handler = async (event: InstanceType<typeof ParentEvent>) => {
    // event_timeout: null means the child has no timeout of its own.
    // It would run forever if the parent didn't cancel it.
    child_ref = event.bus?.emit(NoTimeoutChild({ event_timeout: null }))!
    await delay(200)
    return 'parent_done'
  }

  bus.on(ParentEvent, parent_handler)
  bus.on(NoTimeoutChild, child_slow_handler)

  const parent = bus.emit(ParentEvent({ event_timeout: 0.03 }))
  await parent.done({ raise_if_any: false })
  await bus.waitUntilIdle()

  // Parent timed out
  const parent_result = Array.from(parent.event_results.values())[0]
  assert.equal(parent_result.status, 'error')
  assert.ok(parent_result.error instanceof EventHandlerAbortedError)

  // Child should exist and be cancelled (it was in the queue, never started)
  assert.ok(child_ref, 'Child event should have been emitted')
  assert.equal(child_ref!.event_status, 'completed')
  assert.equal(child_handler_ran, false, 'Child handler should never have started')

  const child_results = Array.from(child_ref!.event_results.values())
  assert.equal(child_results.length, 1)
  assert.ok(
    child_results[0].error instanceof EventHandlerCancelledError,
    'Child handler should be cancelled by parent timeout, even though it has no timeout'
  )
})
