import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { z } from 'zod'

import { BaseEvent, clearSemaphoreRegistry, EventBus, retry, RetryTimeoutError, SemaphoreTimeoutError } from '../src/index.js'

/*
Potential failure modes

A) Event concurrency modes
- global-serial not enforcing strict FIFO across multiple buses (events interleave).
- bus-serial allows cross-bus interleaving but still must be FIFO within a bus; breaks under forwarding.
- parallel accidentally serializes (e.g., lock still used) or breaks queue-jump semantics.
- null not resolving correctly to bus defaults.

B) Handler concurrency modes
- serial not enforcing strict handler order per event.
- parallel accidentally serializes or fails to enforce per-handler ordering.
- null not resolving correctly to bus defaults.

C) Precedence resolution
- Event overrides not taking precedence over bus defaults.
- Conflicting settings (event says parallel, bus says serial) choose wrong winner.

D) Queue-jump / awaited events
- event.now() inside handler doesn’t jump the queue across buses.
- Queue-jump bypasses locks incorrectly in contexts where it shouldn’t.
- Queue-jump fails when event already in-flight.

E) FIFO correctness
- FIFO order broken under bus-serial with interleaved emissions.
- FIFO order broken under global-serial across buses.
- FIFO order broken with forwarded events.

F) Forwarding & bus context
- Forwarded event’s event.event_bus mutates current handler context (wrong bus).
- Child events emitted after forwarding are mis-parented.
- event.event_path diverges between buses.
- Handler attribution lost when forwarded across buses (tree/log issues).

G) Parent/child tracking
- Child events not correctly linked to the parent handler when emitted via event.emit.
- event_children missing under concurrency due to async timing.
- event_pending_bus_count not decremented properly, leaving events stuck.

H) Find semantics under concurrency
- find(past) returns event not yet completed.
- find(future) doesn’t resolve when event finishes in another bus.
- find with child_of returns mismatched events under concurrency.

I) Timeouts + cancellation propagation
- Timeout doesn’t cancel pending child handlers.
- Cancelled results not marked or mis-attributed to the wrong handler.
- Timeout doesn’t propagate across forwarded buses (event still waits forever).

J) Handler result validation
- event_result_type not enforced under parallel handler completion.
- Invalid result doesn’t mark handler error or event failure.
- Timeout + schema error ordering wrong (e.g., schema error overwrites timeout).

K) Idle / completion
- waitUntilIdle() returns early with in-flight events.
- event.now() resolves before children complete.
- event.now() never resolves due to deadlock in runloop.

L) Reentrancy / nested awaits
- Nested awaited child events starve sibling handlers.
- Awaited child events skip lock incorrectly (deadlocks or ordering regressions).

M) Edge-cases
- Multiple handlers for same event type with different options collide.
- Handler throws synchronously before await (still counted, no leaks).
- Handler returns a rejected promise (properly surfaced).
- Event emitted with event_concurrency/event_handler_concurrency invalid value (schema rejects).
- Event emitted with no bus set (done should reject).
*/

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const withResolvers = <T>() => {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolve_fn, reject_fn) => {
    resolve = resolve_fn
    reject = reject_fn
  })
  return { promise, resolve, reject }
}

test('global-serial: only one event processes at a time across buses', async () => {
  const SerialEvent = BaseEvent.extend('SerialEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('GlobalSerialA', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('GlobalSerialB', { event_concurrency: 'global-serial' })

  let in_flight = 0
  let max_in_flight = 0
  const starts: string[] = []

  const handler = async (event: InstanceType<typeof SerialEvent>) => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    starts.push(`${event.source}:${event.order}`)
    await sleep(10)
    in_flight -= 1
  }

  bus_a.on(SerialEvent, handler)
  bus_b.on(SerialEvent, handler)

  for (let i = 0; i < 3; i += 1) {
    bus_a.emit(SerialEvent({ order: i, source: 'a' }))
    bus_b.emit(SerialEvent({ order: i, source: 'b' }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight, 1)

  const starts_a = starts.filter((value) => value.startsWith('a:')).map((value) => Number(value.split(':')[1]))
  const starts_b = starts.filter((value) => value.startsWith('b:')).map((value) => Number(value.split(':')[1]))

  assert.deepEqual(starts_a, [0, 1, 2])
  assert.deepEqual(starts_b, [0, 1, 2])
})

test('test_global_serial_awaited_child_jumps_ahead_of_queued_events_across_buses', async () => {
  const ParentEvent = BaseEvent.extend('ParentEvent', {})
  const ChildEvent = BaseEvent.extend('ChildEvent', {})
  const QueuedEvent = BaseEvent.extend('QueuedEvent', {})

  const bus_a = new EventBus('GlobalSerialParent', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('GlobalSerialChild', { event_concurrency: 'global-serial' })

  const order: string[] = []

  bus_b.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(5)
    order.push('child_end')
  })

  bus_b.on(QueuedEvent, async () => {
    order.push('queued_start')
    await sleep(1)
    order.push('queued_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus_b.emit(QueuedEvent({}))
    // Emit through the scoped proxy so parent tracking is set up,
    // then also dispatch to bus_b for cross-bus processing.
    const child = event.emit(ChildEvent({}))!
    bus_b.emit(child)
    order.push('child_dispatched')
    await child.now()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.emit(ParentEvent({}))
  await parent.now()
  await bus_b.waitUntilIdle()

  const child_start_idx = order.indexOf('child_start')
  const child_end_idx = order.indexOf('child_end')
  const queued_start_idx = order.indexOf('queued_start')

  assert.ok(child_start_idx !== -1)
  assert.ok(child_end_idx !== -1)
  assert.ok(queued_start_idx !== -1)
  assert.ok(child_start_idx < queued_start_idx)
  assert.ok(child_end_idx < queued_start_idx)
})

test('global handler lock via retry serializes handlers across buses', async () => {
  const HandlerEvent = BaseEvent.extend('HandlerEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('GlobalHandlerA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })
  const bus_b = new EventBus('GlobalHandlerB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })

  let in_flight = 0
  let max_in_flight = 0

  const handler = retry({ semaphore_scope: 'global', semaphore_name: 'handler_lock_global', semaphore_limit: 1 })(async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await sleep(5)
    in_flight -= 1
  })

  bus_a.on(HandlerEvent, handler)
  bus_b.on(HandlerEvent, handler)

  for (let i = 0; i < 4; i += 1) {
    bus_a.emit(HandlerEvent({ order: i, source: 'a' }))
    bus_b.emit(HandlerEvent({ order: i, source: 'b' }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('bus-serial: events serialize per bus but overlap across buses', async () => {
  const SerialEvent = BaseEvent.extend('SerialPerBusEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('BusSerialA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialB', { event_concurrency: 'bus-serial' })

  let in_flight_global = 0
  let max_in_flight_global = 0
  let in_flight_a = 0
  let in_flight_b = 0
  let max_in_flight_a = 0
  let max_in_flight_b = 0

  let resolve_b_started: (() => void) | null = null
  const b_started = new Promise<void>((resolve) => {
    resolve_b_started = resolve
  })

  bus_a.on(SerialEvent, async () => {
    in_flight_global += 1
    in_flight_a += 1
    max_in_flight_global = Math.max(max_in_flight_global, in_flight_global)
    max_in_flight_a = Math.max(max_in_flight_a, in_flight_a)
    await b_started
    await sleep(10)
    in_flight_global -= 1
    in_flight_a -= 1
  })

  bus_b.on(SerialEvent, async () => {
    in_flight_global += 1
    in_flight_b += 1
    max_in_flight_global = Math.max(max_in_flight_global, in_flight_global)
    max_in_flight_b = Math.max(max_in_flight_b, in_flight_b)
    if (resolve_b_started) {
      resolve_b_started()
      resolve_b_started = null
    }
    await sleep(10)
    in_flight_global -= 1
    in_flight_b -= 1
  })

  bus_a.emit(SerialEvent({ order: 0, source: 'a' }))
  bus_b.emit(SerialEvent({ order: 0, source: 'b' }))

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight_a, 1)
  assert.equal(max_in_flight_b, 1)
  assert.ok(max_in_flight_global >= 2)
})

test('bus-serial: FIFO order preserved per bus with interleaving', async () => {
  const SerialEvent = BaseEvent.extend('SerialInterleavedEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('BusSerialOrderA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialOrderB', { event_concurrency: 'bus-serial' })

  const starts_a: number[] = []
  const starts_b: number[] = []

  bus_a.on(SerialEvent, async (event) => {
    starts_a.push(event.order)
    await sleep(2)
  })

  bus_b.on(SerialEvent, async (event) => {
    starts_b.push(event.order)
    await sleep(2)
  })

  for (let i = 0; i < 4; i += 1) {
    bus_a.emit(SerialEvent({ order: i, source: 'a' }))
    bus_b.emit(SerialEvent({ order: i, source: 'b' }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.deepEqual(starts_a, [0, 1, 2, 3])
  assert.deepEqual(starts_b, [0, 1, 2, 3])
})

test('bus-serial: awaiting child on one bus does not block other bus queue', async () => {
  const ParentEvent = BaseEvent.extend('BusSerialParent', {})
  const ChildEvent = BaseEvent.extend('BusSerialChild', {})
  const OtherEvent = BaseEvent.extend('BusSerialOther', {})

  const bus_a = new EventBus('BusSerialParentBus', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialOtherBus', { event_concurrency: 'bus-serial' })

  const order: string[] = []

  bus_a.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(10)
    order.push('child_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    const child = event.emit(ChildEvent({}))!
    await child.now()
    order.push('parent_end')
  })

  bus_b.on(OtherEvent, async () => {
    order.push('other_start')
    await sleep(2)
    order.push('other_end')
  })

  const parent = bus_a.emit(ParentEvent({}))
  await sleep(0)
  bus_b.emit(OtherEvent({}))

  await parent.now()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  const other_start_idx = order.indexOf('other_start')
  const parent_end_idx = order.indexOf('parent_end')
  assert.ok(other_start_idx !== -1)
  assert.ok(parent_end_idx !== -1)
  assert.ok(other_start_idx < parent_end_idx)
})

test('parallel: events overlap on same bus when event_concurrency is parallel', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEvent', { order: z.number() })
  const bus = new EventBus('ParallelEventBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()
  setTimeout(() => resolve(), 20)

  bus.on(ParallelEvent, async (_event) => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    await sleep(10)
    in_flight -= 1
  })

  bus.emit(ParallelEvent({ order: 0 }))
  bus.emit(ParallelEvent({ order: 1 }))

  await bus.waitUntilIdle()
  assert.ok(max_in_flight >= 2)
})

test('parallel: handlers overlap for same event when event_handler_concurrency is parallel', async () => {
  const ParallelHandlerEvent = BaseEvent.extend('ParallelHandlerEvent', {})
  const bus = new EventBus('ParallelHandlerBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler_a = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  const handler_b = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(ParallelHandlerEvent, handler_a)
  bus.on(ParallelHandlerEvent, handler_b)

  const event = bus.emit(ParallelHandlerEvent({}))
  await sleep(0)
  resolve()
  await event.now()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('parallel: global handler lock via retry still serializes across buses', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventGlobalHandler', {
    source: z.string(),
  })

  const bus_a = new EventBus('ParallelHandlerGlobalA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })
  const bus_b = new EventBus('ParallelHandlerGlobalB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = retry({
    semaphore_scope: 'global',
    semaphore_name: (event: BaseEvent) => `handler_lock_${event.event_type}`,
    semaphore_limit: 1,
  })(async (_event: BaseEvent) => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus_a.on(ParallelEvent, handler)
  bus_b.on(ParallelEvent, handler)

  bus_a.emit(ParallelEvent({ source: 'a' }))
  bus_b.emit(ParallelEvent({ source: 'b' }))

  await sleep(0)
  resolve()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('retry: instance scope serializes selected handlers per event in parallel mode', async () => {
  const SerializedEvent = BaseEvent.extend('RetryInstanceSerializedHandlers', {})
  const bus = new EventBus('RetryInstanceSerializedBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  const log: string[] = []

  class HandlerSuite {
    @retry({ semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: (event: BaseEvent) => `serial-${event.event_id}` })
    async step1(event: BaseEvent) {
      log.push(`step1_start_${event.event_id}`)
      await sleep(10)
      log.push(`step1_end_${event.event_id}`)
    }

    @retry({ semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: (event: BaseEvent) => `serial-${event.event_id}` })
    async step2(event: BaseEvent) {
      log.push(`step2_start_${event.event_id}`)
      await sleep(5)
      log.push(`step2_end_${event.event_id}`)
    }

    async parallel(_event: BaseEvent) {
      log.push('parallel')
    }
  }

  const handlers = new HandlerSuite()

  bus.on(SerializedEvent, handlers.step1.bind(handlers))
  bus.on(SerializedEvent, handlers.step2.bind(handlers))
  bus.on(SerializedEvent, handlers.parallel.bind(handlers))

  const event = bus.emit(SerializedEvent({}))
  await event.now()
  await bus.waitUntilIdle()

  const step1_end = log.findIndex((entry) => entry.startsWith('step1_end_'))
  const step2_start = log.findIndex((entry) => entry.startsWith('step2_start_'))
  assert.ok(step1_end !== -1 && step2_start !== -1, 'serialized handlers should have run')
  assert.ok(step1_end < step2_start, `instance scope: step2 should start after step1 ends. Got: [${log.join(', ')}]`)
})

test('precedence: event event_concurrency overrides bus defaults to parallel', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEventParallelEvents', {
    event_concurrency: z.literal('parallel'),
    order: z.number(),
  })
  const bus = new EventBus('OverrideParallelEventsBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  bus.on(OverrideEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus.emit(OverrideEvent({ order: 0, event_concurrency: 'parallel' }))
  bus.emit(OverrideEvent({ order: 1, event_concurrency: 'parallel' }))

  await sleep(0)
  resolve()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('precedence: event event_concurrency overrides bus defaults to bus-serial', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEventBusSerial', {
    event_concurrency: z.literal('bus-serial'),
    order: z.number(),
  })
  const bus = new EventBus('OverrideBusSerialEventsBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  bus.on(OverrideEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus.emit(OverrideEvent({ order: 0, event_concurrency: 'bus-serial' }))
  bus.emit(OverrideEvent({ order: 1, event_concurrency: 'bus-serial' }))

  await sleep(0)
  assert.equal(max_in_flight, 1)
  resolve()
  await bus.waitUntilIdle()
})

test('global-serial + handler parallel: handlers overlap but events do not across buses', async () => {
  const SerialParallelEvent = BaseEvent.extend('GlobalSerialParallelHandlers', {})

  const bus_a = new EventBus('GlobalSerialParallelA', {
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'parallel',
  })
  const bus_b = new EventBus('GlobalSerialParallelB', {
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(SerialParallelEvent, handler)
  bus_a.on(SerialParallelEvent, handler)
  bus_b.on(SerialParallelEvent, handler)
  bus_b.on(SerialParallelEvent, handler)

  bus_a.emit(SerialParallelEvent({}))
  bus_b.emit(SerialParallelEvent({}))

  await sleep(0)
  assert.equal(max_in_flight, 2)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('event parallel + handler serial: handlers serialize within each event', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventsSerialHandlers', { order: z.number() })
  const bus = new EventBus('ParallelEventsSerialHandlersBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })

  let global_in_flight = 0
  let global_max = 0
  const per_event_in_flight = new Map<string, number>()
  const per_event_max = new Map<string, number>()
  const { promise, resolve } = withResolvers<void>()
  const { promise: started_promise, resolve: resolve_started } = withResolvers<void>()
  let started_handlers = 0
  const started_timeout = setTimeout(resolve_started, 50)

  const handler = async (event: BaseEvent) => {
    global_in_flight += 1
    global_max = Math.max(global_max, global_in_flight)
    const event_count = (per_event_in_flight.get(event.event_id) ?? 0) + 1
    per_event_in_flight.set(event.event_id, event_count)
    per_event_max.set(event.event_id, Math.max(per_event_max.get(event.event_id) ?? 0, event_count))
    started_handlers += 1
    if (started_handlers === 2) {
      clearTimeout(started_timeout)
      resolve_started()
    }
    await promise
    global_in_flight -= 1
    per_event_in_flight.set(event.event_id, Math.max(0, (per_event_in_flight.get(event.event_id) ?? 1) - 1))
  }

  bus.on(ParallelEvent, handler)
  bus.on(ParallelEvent, handler)

  const event_a = bus.emit(ParallelEvent({ order: 0 }))
  const event_b = bus.emit(ParallelEvent({ order: 1 }))

  await started_promise
  assert.equal(per_event_max.get(event_a.event_id), 1)
  assert.equal(per_event_max.get(event_b.event_id), 1)
  assert.ok(global_max >= 2)
  resolve()
  await bus.waitUntilIdle()
})

test('event parallel + handler serial: handlers overlap across buses', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventsBusHandlers', { source: z.string() })

  const bus_a = new EventBus('ParallelBusHandlersA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })
  const bus_b = new EventBus('ParallelBusHandlersB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(ParallelEvent, handler)
  bus_b.on(ParallelEvent, handler)

  bus_a.emit(ParallelEvent({ source: 'a' }))
  bus_b.emit(ParallelEvent({ source: 'b' }))

  await sleep(0)
  assert.ok(max_in_flight >= 2)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('retry can enforce global lock even when bus defaults to parallel', async () => {
  const HandlerEvent = BaseEvent.extend('HandlerOptionsGlobalSerial', { source: z.string() })

  const bus_a = new EventBus('HandlerOptionsGlobalA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  const bus_b = new EventBus('HandlerOptionsGlobalB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = retry({ semaphore_scope: 'global', semaphore_name: 'handler_lock_options', semaphore_limit: 1 })(async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus_a.on(HandlerEvent, handler)
  bus_b.on(HandlerEvent, handler)

  bus_a.emit(HandlerEvent({ source: 'a' }))
  bus_b.emit(HandlerEvent({ source: 'b' }))

  await sleep(0)
  assert.equal(max_in_flight, 1)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('null: event_concurrency null resolves to bus defaults', async () => {
  const AutoEvent = BaseEvent.extend('AutoEvent', {
    event_concurrency: z.null(),
  })
  const bus = new EventBus('AutoBus', { event_concurrency: 'bus-serial' })

  let in_flight = 0
  let max_in_flight = 0

  bus.on(AutoEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await sleep(5)
    in_flight -= 1
  })

  bus.emit(AutoEvent({ event_concurrency: null }))
  bus.emit(AutoEvent({ event_concurrency: null }))

  await bus.waitUntilIdle()
  assert.equal(max_in_flight, 1)
})

test('null: event_handler_concurrency null resolves to bus defaults', async () => {
  const AutoHandlerEvent = BaseEvent.extend('AutoHandlerEvent', {
    event_handler_concurrency: z.null(),
  })
  const bus = new EventBus('AutoHandlerBus', { event_handler_concurrency: 'serial' })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(AutoHandlerEvent, handler)
  bus.on(AutoHandlerEvent, handler)

  const event = bus.emit(AutoHandlerEvent({ event_handler_concurrency: null }))
  await sleep(0)
  resolve()
  await event.now()
  await bus.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('queue-jump: awaited child preempts queued sibling on same bus', async () => {
  const ParentEvent = BaseEvent.extend('QueueJumpParent', {})
  const ChildEvent = BaseEvent.extend('QueueJumpChild', {})
  const SiblingEvent = BaseEvent.extend('QueueJumpSibling', {})

  const bus = new EventBus('QueueJumpBus', { event_concurrency: 'bus-serial' })
  const order: string[] = []

  bus.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(5)
    order.push('child_end')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling_start')
    await sleep(1)
    order.push('sibling_end')
  })

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus.emit(SiblingEvent({}))
    const child = event.emit(ChildEvent({}))!
    order.push('child_dispatched')
    await child.now()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus.emit(ParentEvent({}))
  await parent.now()
  await bus.waitUntilIdle()

  const child_start_idx = order.indexOf('child_start')
  const child_end_idx = order.indexOf('child_end')
  const sibling_start_idx = order.indexOf('sibling_start')

  assert.ok(child_start_idx !== -1)
  assert.ok(child_end_idx !== -1)
  assert.ok(sibling_start_idx !== -1)
  assert.ok(child_start_idx < sibling_start_idx)
  assert.ok(child_end_idx < sibling_start_idx)
})

test('queue-jump: same event handlers on separate buses stay isolated without forwarding', async () => {
  const ParentEvent = BaseEvent.extend('QueueJumpIsolatedParent', {})
  const SharedEvent = BaseEvent.extend('QueueJumpIsolatedShared', {})
  const SiblingEvent = BaseEvent.extend('QueueJumpIsolatedSibling', {})

  const bus_a = new EventBus('QueueJumpIsolatedA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('QueueJumpIsolatedB', { event_concurrency: 'bus-serial' })

  const order: string[] = []
  let bus_a_shared_runs = 0
  let bus_b_shared_runs = 0

  bus_a.on(SharedEvent, async () => {
    bus_a_shared_runs += 1
    order.push('bus_a_shared_start')
    await sleep(2)
    order.push('bus_a_shared_end')
  })

  bus_b.on(SharedEvent, async () => {
    bus_b_shared_runs += 1
    order.push('bus_b_shared_start')
    await sleep(2)
    order.push('bus_b_shared_end')
  })

  bus_a.on(SiblingEvent, async () => {
    order.push('bus_a_sibling_start')
    await sleep(1)
    order.push('bus_a_sibling_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus_a.emit(SiblingEvent({}))
    const shared = event.emit(SharedEvent({}))!
    order.push('shared_dispatched')
    await shared.now()
    order.push('shared_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.emit(ParentEvent({}))
  await parent.now()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  assert.equal(bus_a_shared_runs, 1)
  assert.equal(bus_b_shared_runs, 0)
  assert.equal(order.includes('bus_b_shared_start'), false)

  const bus_a_shared_end_idx = order.indexOf('bus_a_shared_end')
  const bus_a_sibling_start_idx = order.indexOf('bus_a_sibling_start')
  assert.ok(bus_a_shared_end_idx !== -1)
  assert.ok(bus_a_sibling_start_idx !== -1)
  assert.ok(bus_a_shared_end_idx < bus_a_sibling_start_idx)
})

test('queue-jump: awaiting in-flight event does not double-run handlers', async () => {
  const InFlightEvent = BaseEvent.extend('InFlightEvent', {})
  const bus = new EventBus('InFlightBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let handler_runs = 0
  let resolve_started: (() => void) | null = null
  const started = new Promise<void>((resolve) => {
    resolve_started = resolve
  })
  const { promise: release_child, resolve: resolve_child } = withResolvers<void>()

  bus.on(InFlightEvent, async () => {
    handler_runs += 1
    if (resolve_started) {
      resolve_started()
      resolve_started = null
    }
    await release_child
  })

  const child = bus.emit(InFlightEvent({}))
  await started

  let done_resolved = false
  const done_promise = child.now().then(() => {
    done_resolved = true
  })

  await sleep(0)
  assert.equal(done_resolved, false)

  resolve_child()
  await done_promise
  await bus.waitUntilIdle()

  assert.equal(handler_runs, 1)
})

test('edge-case: event with no handlers completes immediately', async () => {
  const NoHandlerEvent = BaseEvent.extend('NoHandlerEvent', {})
  const bus = new EventBus('NoHandlerBus')

  const event = bus.emit(NoHandlerEvent({}))
  await event.now()
  await bus.waitUntilIdle()

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_pending_bus_count, 0)
})

test('fifo: forwarded events preserve order on target bus (bus-serial)', async () => {
  const OrderedEvent = BaseEvent.extend('ForwardOrderEvent', { order: z.number() })

  const bus_a = new EventBus('ForwardOrderA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('ForwardOrderB', { event_concurrency: 'bus-serial' })

  const order_a: number[] = []
  const order_b: number[] = []

  bus_a.on(OrderedEvent, async (event) => {
    order_a.push(event.order)
    bus_b.emit(event)
    await sleep(2)
  })

  bus_b.on(OrderedEvent, async (event) => {
    const bus_b_results = Array.from(event.event_results.values()).filter((result) => result.eventbus_id === bus_b.id)
    const in_flight = bus_b_results.filter((result) => result.status === 'pending' || result.status === 'started')
    assert.ok(in_flight.length <= 1)
    order_b.push(event.order)
    await sleep(1)
  })

  for (let i = 0; i < 5; i += 1) {
    bus_a.emit(OrderedEvent({ order: i }))
  }

  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  const history_orders = Array.from(bus_b.event_history.values()).map((event) => (event as { order?: number }).order)
  const results_sizes = Array.from(bus_b.event_history.values()).map((event) => event.event_results.size)
  const bus_b_result_counts = Array.from(bus_b.event_history.values()).map(
    (event) => Array.from(event.event_results.values()).filter((result) => result.eventbus_id === bus_b.id).length
  )
  const processed_flags = Array.from(bus_b.event_history.values()).map((event) =>
    Array.from(event.event_results.values())
      .filter((result) => result.eventbus_id === bus_b.id)
      .every((result) => result.status === 'completed' || result.status === 'error')
  )
  const pending_counts = Array.from(bus_b.event_history.values()).map(
    (event) => Array.from(event.event_results.values()).filter((result) => result.status === 'pending').length
  )
  assert.deepEqual(order_a, [0, 1, 2, 3, 4])
  assert.deepEqual(order_b, [0, 1, 2, 3, 4])
  assert.deepEqual(history_orders, [0, 1, 2, 3, 4])
  assert.deepEqual(results_sizes, [2, 2, 2, 2, 2])
  assert.deepEqual(bus_b_result_counts, [1, 1, 1, 1, 1])
  assert.deepEqual(processed_flags, [true, true, true, true, true])
  assert.deepEqual(pending_counts, [0, 0, 0, 0, 0])
})

test('fifo: forwarded events preserve order across chained buses (bus-serial)', async () => {
  const OrderedEvent = BaseEvent.extend('ForwardChainEvent', { order: z.number() })

  const bus_a = new EventBus('ForwardChainA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('ForwardChainB', { event_concurrency: 'bus-serial' })
  const bus_c = new EventBus('ForwardChainC', { event_concurrency: 'bus-serial' })

  const order_c: number[] = []

  bus_b.on(OrderedEvent, async () => {
    await sleep(2)
  })

  bus_c.on(OrderedEvent, async (event) => {
    order_c.push(event.order)
    await sleep(1)
  })

  bus_a.on('*', bus_b.emit)
  bus_b.on('*', bus_c.emit)

  for (let i = 0; i < 6; i += 1) {
    bus_a.emit(OrderedEvent({ order: i }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  assert.deepEqual(order_c, [0, 1, 2, 3, 4, 5])
})

test('find: past returns most recent completed event (bus-scoped)', async () => {
  const DebounceEvent = BaseEvent.extend('FindPastEvent', { value: z.number() })
  const bus = new EventBus('FindPastBus')

  bus.on(DebounceEvent, async () => {})

  bus.emit(DebounceEvent({ value: 1 }))
  bus.emit(DebounceEvent({ value: 2 }))

  await bus.waitUntilIdle()

  const found = await bus.find(DebounceEvent, { past: true, future: false })
  assert.ok(found)
  assert.equal(found.value, 2)
  assert.equal(found.event_status, 'completed')
  assert.ok(found.event_bus)
  assert.equal(found.event_bus.name, 'FindPastBus')
  assert.equal(typeof found.emit, 'function')
})

test('find: past returns in-flight dispatched event and done waits', async () => {
  const DebounceEvent = BaseEvent.extend('FindFutureEvent', { value: z.number() })
  const bus = new EventBus('FindFutureBus')
  const { promise, resolve } = withResolvers<void>()

  bus.on(DebounceEvent, async () => {
    await promise
  })

  bus.emit(DebounceEvent({ value: 1 }))

  const found = await bus.find(DebounceEvent, { past: true, future: false })
  assert.ok(found)
  assert.equal(found.value, 1)
  assert.ok(found.event_status !== 'completed')
  assert.ok(found.event_bus)
  assert.equal(found.event_bus.name, 'FindFutureBus')

  resolve()
  const completed = await found.now()
  assert.equal(completed.event_status, 'completed')
})

test('find: future waits for next event when none in-flight', async () => {
  const DebounceEvent = BaseEvent.extend('FindWaitEvent', { value: z.number() })
  const bus = new EventBus('FindWaitBus')

  bus.on(DebounceEvent, async () => {})

  setTimeout(() => {
    bus.emit(DebounceEvent({ value: 99 }))
  }, 10)

  const found = await bus.find(DebounceEvent, { past: false, future: 0.2 })
  assert.ok(found)
  assert.equal(found.value, 99)
  assert.ok(found.event_bus)
  assert.equal(found.event_bus.name, 'FindWaitBus')
  await found.now()
})

test('find: most recent wins across completed and in-flight', async () => {
  const DebounceEvent = BaseEvent.extend('FindMostRecentEvent', { value: z.number() })
  const bus = new EventBus('FindMostRecentBus')
  const { promise, resolve } = withResolvers<void>()

  bus.on(DebounceEvent, async (event) => {
    if (event.value === 2) {
      await promise
    }
  })

  bus.emit(DebounceEvent({ value: 1 }))
  await bus.waitUntilIdle()

  bus.emit(DebounceEvent({ value: 2 }))

  const found = await bus.find(DebounceEvent, { past: true, future: true })
  assert.ok(found)
  assert.equal(found.value, 2)
  assert.ok(found.event_status !== 'completed')

  resolve()
  await found.now()
})

// Folded from retry.test.ts to keep test layout class-based.
const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

// ─── Basic retry behavior ────────────────────────────────────────────────────

test('retry: function succeeds on first attempt with no retries needed', async () => {
  const fn = retry({ max_attempts: 3 })(async () => 'ok')
  assert.equal(await fn(), 'ok')
})

test('retry: function retries on failure and eventually succeeds', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(async () => {
    calls++
    if (calls < 3) throw new Error(`fail ${calls}`)
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: throws after exhausting all attempts', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(async () => {
    calls++
    throw new Error('always fails')
  })
  await assert.rejects(fn, { message: 'always fails' })
  assert.equal(calls, 3)
})

test('retry: max_attempts=1 means no retries (single attempt)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 1 })(async () => {
    calls++
    throw new Error('fail')
  })
  await assert.rejects(fn, { message: 'fail' })
  assert.equal(calls, 1)
})

test('retry: default max_attempts=1 means single attempt', async () => {
  let calls = 0
  const fn = retry()(async () => {
    calls++
    throw new Error('fail')
  })
  await assert.rejects(fn, { message: 'fail' })
  assert.equal(calls, 1)
})

// ─── retry_after delay ───────────────────────────────────────────────────────

test('retry: retry_after introduces delay between attempts', async () => {
  let calls = 0
  const timestamps: number[] = []
  const fn = retry({ max_attempts: 3, retry_after: 0.05 })(async () => {
    calls++
    timestamps.push(performance.now())
    if (calls < 3) throw new Error('fail')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)

  // Check that delays were at least ~50ms between attempts
  const gap1 = timestamps[1] - timestamps[0]
  const gap2 = timestamps[2] - timestamps[1]
  assert.ok(gap1 >= 40, `expected >=40ms gap, got ${gap1.toFixed(1)}ms`)
  assert.ok(gap2 >= 40, `expected >=40ms gap, got ${gap2.toFixed(1)}ms`)
})

// ─── Exponential backoff ─────────────────────────────────────────────────────

test('retry: retry_backoff_factor increases delay between attempts', async () => {
  let calls = 0
  const timestamps: number[] = []
  const fn = retry({ max_attempts: 4, retry_after: 0.03, retry_backoff_factor: 2.0 })(async () => {
    calls++
    timestamps.push(performance.now())
    if (calls < 4) throw new Error('fail')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 4)

  // Delays: 30ms, 60ms, 120ms (0.03 * 2^0, 0.03 * 2^1, 0.03 * 2^2)
  const gap1 = timestamps[1] - timestamps[0]
  const gap2 = timestamps[2] - timestamps[1]
  const gap3 = timestamps[3] - timestamps[2]

  assert.ok(gap1 >= 20, `gap1=${gap1.toFixed(1)}ms, expected >=20ms`)
  assert.ok(gap2 >= 45, `gap2=${gap2.toFixed(1)}ms, expected >=45ms (should be ~60ms)`)
  assert.ok(gap3 >= 90, `gap3=${gap3.toFixed(1)}ms, expected >=90ms (should be ~120ms)`)
  // Verify backoff is actually increasing
  assert.ok(gap2 > gap1, 'gap2 should be larger than gap1')
  assert.ok(gap3 > gap2, 'gap3 should be larger than gap2')
})

// ─── retry_on_errors filtering ───────────────────────────────────────────────

class NetworkError extends Error {
  constructor(message: string = 'network error') {
    super(message)
    this.name = 'NetworkError'
  }
}

class ValidationError extends Error {
  constructor(message: string = 'validation error') {
    super(message)
    this.name = 'ValidationError'
  }
}

test('retry: retry_on_errors retries only matching error types', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError()
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors does not retry non-matching errors', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async () => {
    calls++
    throw new ValidationError()
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  // Should have thrown immediately without retrying
  assert.equal(calls, 1)
})

test('retry: retry_on_errors accepts string error name', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: ['NetworkError'] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError()
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors string matcher does not retry non-matching names', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: ['NetworkError'] })(async () => {
    calls++
    throw new ValidationError()
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  assert.equal(calls, 1)
})

test('retry: retry_on_errors accepts RegExp pattern', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [/network/i] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError('Network timeout occurred')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors RegExp does not retry non-matching errors', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [/network/i] })(async () => {
    calls++
    throw new ValidationError('bad input')
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  assert.equal(calls, 1)
})

test('retry: retry_on_errors mixes class, string, and RegExp matchers', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 5, retry_on_errors: [TypeError, 'NetworkError', /timeout/i] })(async () => {
    calls++
    if (calls === 1) throw new TypeError('type error')
    if (calls === 2) throw new NetworkError()
    if (calls === 3) throw new Error('Connection timeout')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 4)
})

test('retry: retry_on_errors with multiple error types', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 5, retry_on_errors: [NetworkError, TypeError] })(async () => {
    calls++
    if (calls === 1) throw new NetworkError()
    if (calls === 2) throw new TypeError('type error')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

// ─── Per-attempt timeout ─────────────────────────────────────────────────────

test('retry: timeout triggers RetryTimeoutError on slow attempts', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 1, timeout: 0.05 })(async () => {
    calls++
    await delay(200)
    return 'ok'
  })
  await assert.rejects(fn, (error: unknown) => {
    assert.ok(error instanceof RetryTimeoutError)
    assert.equal(error.attempt, 1)
    return true
  })
  assert.equal(calls, 1)
})

test('retry: timeout allows fast attempts to succeed', async () => {
  const fn = retry({ max_attempts: 1, timeout: 1 })(async () => {
    await delay(5)
    return 'fast'
  })
  assert.equal(await fn(), 'fast')
})

test('retry: timed-out attempts are retried when max_attempts > 1', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, timeout: 0.05 })(async () => {
    calls++
    if (calls < 3) {
      await delay(200) // will timeout
      return 'slow'
    }
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

// ─── Semaphore concurrency control ──────────────────────────────────────────

test('retry: semaphore_limit controls max concurrent executions', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({ max_attempts: 1, semaphore_limit: 2, semaphore_name: 'test_sem_limit' })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(50)
    active--
  })

  // Launch 6 concurrent calls — should only run 2 at a time
  await Promise.all([fn(), fn(), fn(), fn(), fn(), fn()])
  assert.equal(max_active, 2, 'should never exceed semaphore_limit=2')
})

test('retry: semaphore handoff keeps concurrency bounded during nextTick scheduling', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0
  let unblock_first!: () => void
  const first_block = new Promise<void>((resolve) => {
    unblock_first = resolve
  })
  let third_done_resolve!: () => void
  let third_done_reject!: (reason?: unknown) => void
  const third_done = new Promise<void>((resolve, reject) => {
    third_done_resolve = resolve
    third_done_reject = reject
  })

  let call_count = 0
  const fn = retry({ max_attempts: 1, semaphore_limit: 1, semaphore_name: 'test_sem_handoff' })(async () => {
    call_count += 1
    const current_call = call_count

    active += 1
    max_active = Math.max(max_active, active)
    try {
      if (current_call === 1) {
        await first_block
      }
      await delay(5)
    } finally {
      active -= 1
    }
  })

  const first = fn()
  await delay(5)
  const second = fn()
  await delay(5)
  unblock_first()

  void Promise.resolve().then(() => {
    process.nextTick(() => {
      void fn().then(
        () => third_done_resolve(),
        (error) => third_done_reject(error)
      )
    })
  })

  await Promise.all([first, second, third_done])
  assert.equal(call_count, 3)
  assert.equal(max_active, 1, 'should never exceed semaphore_limit=1 during handoff')
})

test('retry: semaphore_lax=false throws SemaphoreTimeoutError when slots are full', async () => {
  clearSemaphoreRegistry()

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_lax_false',
    semaphore_lax: false,
    semaphore_timeout: 0.05,
  })(async () => {
    await delay(200) // hold the semaphore for a while
    return 'ok'
  })

  // Start one call to grab the semaphore
  const first = fn()

  // Give the first call time to acquire the semaphore
  await delay(10)

  // Second call should timeout trying to acquire semaphore
  await assert.rejects(fn(), (error: unknown) => {
    assert.ok(error instanceof SemaphoreTimeoutError)
    assert.equal(error.semaphore_name, 'test_sem_lax_false')
    return true
  })

  // Let the first call finish
  assert.equal(await first, 'ok')
})

test('retry: semaphore_lax=true (default) proceeds without semaphore on timeout', async () => {
  clearSemaphoreRegistry()

  let calls = 0
  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_lax_true',
    semaphore_lax: true,
    semaphore_timeout: 0.05,
  })(async () => {
    calls++
    await delay(200)
    return 'ok'
  })

  // Start first call to grab the semaphore
  const first = fn()
  await delay(10)

  // Second call should proceed anyway (lax mode)
  const second = fn()
  const results = await Promise.all([first, second])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(calls, 2)
})

// ─── Preserves function metadata ─────────────────────────────────────────────

test('retry: preserves function name', () => {
  async function myNamedFunction(): Promise<string> {
    return 'ok'
  }
  const wrapped = retry()(myNamedFunction)
  assert.equal(wrapped.name, 'myNamedFunction')
})

// ─── Preserves `this` context ────────────────────────────────────────────────

test('retry: preserves this context for methods', async () => {
  class MyService {
    value = 42
    fetch = retry({ max_attempts: 2 })(async function (this: MyService) {
      return this.value
    })
  }

  const svc = new MyService()
  assert.equal(await svc.fetch(), 42)
})

// ─── Works with synchronous functions ────────────────────────────────────────

test('retry: wraps sync functions (result becomes a promise)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(() => {
    calls++
    if (calls < 2) throw new Error('sync fail')
    return 'sync ok'
  })
  assert.equal(await fn(), 'sync ok')
  assert.equal(calls, 2)
})

// ─── Edge cases ──────────────────────────────────────────────────────────────

test('retry: max_attempts=0 is treated as 1 (minimum)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 0 })(async () => {
    calls++
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 1)
})

test('retry: passes arguments through to wrapped function', async () => {
  const fn = retry({ max_attempts: 1 })(async (a: number, b: string) => `${a}-${b}`)
  assert.equal(await fn(1, 'hello'), '1-hello')
})

test('retry: semaphore is held across all retry attempts', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0
  let total_calls = 0

  const fn = retry({
    max_attempts: 3,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_across_retries',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    total_calls++
    await delay(10)
    active--
    // Odd calls fail, even calls succeed — each invocation needs 2 attempts
    if (total_calls % 2 === 1) throw new Error('fail')
    return 'ok'
  })

  // Run 3 calls concurrently — they should run serially because semaphore_limit=1
  // The semaphore should be held across retries, so only 1 active at a time
  const results = await Promise.all([fn(), fn(), fn()])
  assert.equal(max_active, 1, 'semaphore should enforce serial execution even during retries')
  assert.deepEqual(results, ['ok', 'ok', 'ok'])
  assert.equal(total_calls, 6, 'each of 3 calls should have taken 2 attempts')
})

test('retry: semaphore released even when all attempts fail', async () => {
  clearSemaphoreRegistry()

  const fn = retry({
    max_attempts: 2,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_release_on_fail',
  })(async () => {
    throw new Error('always fails')
  })

  // First call fails, should release semaphore
  await assert.rejects(fn)

  // Second call should be able to acquire the semaphore (not deadlocked)
  await assert.rejects(fn)
})

// ─── TC39 decorator syntax on class methods ──────────────────────────────────

test('retry: works on class method via manual wrapping pattern', async () => {
  // Since TC39 Stage 3 decorators require experimentalDecorators or TS 5.0+ native support,
  // we test the equivalent pattern: applying retry() to a method post-definition.
  class ApiClient {
    base_url = 'https://example.com'
    calls = 0

    fetchData = retry({ max_attempts: 3 })(async function (this: ApiClient) {
      this.calls++
      if (this.calls < 3) throw new Error('api error')
      return `data from ${this.base_url}`
    })
  }

  const client = new ApiClient()
  assert.equal(await client.fetchData(), 'data from https://example.com')
  assert.equal(client.calls, 3)
})

// ─── Re-entrancy / deadlock prevention ───────────────────────────────────────

test('retry: re-entrant call on same semaphore does not deadlock', async () => {
  clearSemaphoreRegistry()

  const inner = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'shared_sem',
  })(async () => {
    return 'inner ok'
  })

  const outer = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'shared_sem',
  })(async () => {
    // This would deadlock without re-entrancy tracking:
    // outer holds the semaphore, inner tries to acquire the same one
    const result = await inner()
    return `outer got: ${result}`
  })

  assert.equal(await outer(), 'outer got: inner ok')
})

test('retry: recursive function with semaphore does not deadlock', async () => {
  clearSemaphoreRegistry()

  let depth = 0
  const recurse: (n: number) => Promise<number> = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'recursive_sem',
  })(async (n: number): Promise<number> => {
    depth++
    if (n <= 1) return 1
    return n + (await recurse(n - 1))
  })

  const result = await recurse(5)
  assert.equal(result, 15) // 5 + 4 + 3 + 2 + 1
  assert.equal(depth, 5)
})

test('retry: different semaphore names do not interfere with re-entrancy', async () => {
  clearSemaphoreRegistry()

  let inner_active = 0
  let inner_max_active = 0

  const inner = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'inner_sem',
  })(async () => {
    inner_active++
    inner_max_active = Math.max(inner_max_active, inner_active)
    await delay(20)
    inner_active--
    return 'inner ok'
  })

  const outer = retry({
    max_attempts: 1,
    semaphore_limit: 2,
    semaphore_name: 'outer_sem',
  })(async () => {
    return await inner()
  })

  // Run 3 outer calls concurrently
  // outer_sem allows 2 concurrent, but inner_sem only allows 1
  const results = await Promise.all([outer(), outer(), outer()])
  assert.deepEqual(results, ['inner ok', 'inner ok', 'inner ok'])
  assert.equal(inner_max_active, 1, 'inner semaphore should still enforce limit=1')
})

test('retry: three-level nested re-entrancy does not deadlock', async () => {
  clearSemaphoreRegistry()

  const level3 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => 'level3')

  const level2 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => {
    const r = await level3()
    return `level2>${r}`
  })

  const level1 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => {
    const r = await level2()
    return `level1>${r}`
  })

  assert.equal(await level1(), 'level1>level2>level3')
})

// ─── Semaphore scope ─────────────────────────────────────────────────────────

test('retry: semaphore_scope=class shares semaphore across instances of same class', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  const b = new Worker()
  const c = new Worker()

  await Promise.all([a.run(), b.run(), c.run()])
  assert.equal(max_active, 1, 'class scope: all instances should share one semaphore')
})

test('retry: semaphore_scope=instance gives each instance its own semaphore', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  const b = new Worker()

  // Same instance: serialized (limit=1 per instance)
  // Different instances: can run in parallel (separate semaphores)
  await Promise.all([a.run(), b.run()])
  assert.equal(max_active, 2, 'instance scope: different instances should get separate semaphores')
})

test('retry: semaphore_scope=instance serializes calls on same instance', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(20)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  await Promise.all([a.run(), a.run(), a.run()])
  assert.equal(max_active, 1, 'instance scope: same instance calls should serialize')
})

test('retry: semaphore_name function uses call args for keying', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const work = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_scope: 'global',
    semaphore_name: (a: string, b: string) => `${a}-${b}`,
  })(async (_a: string, _b: string) => {
    active++
    max_active = Math.max(max_active, active)
    await delay(20)
    active--
    return 'done'
  })

  await Promise.all([work('a', 'b'), work('a', 'b')])
  assert.equal(max_active, 1, 'semaphore_name(args): same args should serialize')

  active = 0
  max_active = 0
  await Promise.all([work('a', 'b'), work('c', 'd')])
  assert.ok(max_active >= 2, 'semaphore_name(args): different args should not share a semaphore')
})

test('retry: semaphore_scope=class isolates different classes', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Alpha {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'run',
    })(async function (this: Alpha) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
    })
  }

  class Beta {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'run',
    })(async function (this: Beta) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
    })
  }

  await Promise.all([new Alpha().run(), new Beta().run()])
  assert.equal(max_active, 2, 'class scope: different classes should get separate semaphores')
})

// ─── TC39 Stage 3 decorator syntax (RECOMMENDED PATTERN) ────────────────────
//
// The primary supported pattern for event bus handlers is:
//
//   class Service {
//     constructor(bus) {
//       bus.on(Event, this.on_Event.bind(this))
//     }
//
//     @retry({ max_attempts: 3, ... })
//     async on_Event(event) { ... }
//   }
//
// Retry/timeout is a handler-level concern. Event processing itself has no error
// state — only individual handlers produce errors/timeouts that need retrying.
// Event-level and handler-level concurrency on the bus is still controllable via
// event_concurrency / event_handler_concurrency options (those are separate).

test('retry: @retry() TC39 decorator on class method retries on failure', async () => {
  clearSemaphoreRegistry()

  class ApiService {
    calls = 0

    @retry({ max_attempts: 3 })
    async fetchData(): Promise<string> {
      this.calls++
      if (this.calls < 3) throw new Error('api error')
      return 'data'
    }
  }

  const svc = new ApiService()
  assert.equal(await svc.fetchData(), 'data')
  assert.equal(svc.calls, 3)
})

test('retry: @retry() TC39 decorator preserves this context', async () => {
  class Config {
    endpoint = 'https://api.example.com'

    @retry({ max_attempts: 2 })
    async getEndpoint(): Promise<string> {
      return this.endpoint
    }
  }

  const cfg = new Config()
  assert.equal(await cfg.getEndpoint(), 'https://api.example.com')
})

test('retry: @retry() TC39 decorator with semaphore_scope=class', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Service {
    @retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'handle',
    })
    async handle(): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  const a = new Service()
  const b = new Service()
  await Promise.all([a.handle(), b.handle()])
  assert.equal(max_active, 1, '@retry class scope: all instances share one semaphore')
})

test('retry: @retry() TC39 decorator with semaphore_scope=instance', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Service {
    @retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'handle',
    })
    async handle(): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  const a = new Service()
  const b = new Service()
  await Promise.all([a.handle(), b.handle()])
  assert.equal(max_active, 2, '@retry instance scope: different instances get separate semaphores')
})

// ─── Scope fallback to global ───────────────────────────────────────────────

test('retry: semaphore_scope=class falls back to global for standalone functions', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_scope: 'class',
    semaphore_name: 'standalone_class',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  // Two concurrent calls should serialize since they share the same global-fallback semaphore
  const results = await Promise.all([fn(), fn()])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(max_active, 1, 'class scope on standalone fn should fall back to global and serialize')
})

test('retry: semaphore_scope=instance falls back to global for standalone functions', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_scope: 'instance',
    semaphore_name: 'standalone_instance',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  // Two concurrent calls should serialize since they share the same global-fallback semaphore
  const results = await Promise.all([fn(), fn()])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(max_active, 1, 'instance scope on standalone fn should fall back to global and serialize')
})

// ─── HOF pattern: retry({...})(fn.bind(instance)) — bind BEFORE wrapping ────
// NOTE: This falls back to global scope because JS cannot extract [[BoundThis]]
// from a bound function. The handler works correctly (this is preserved inside
// the handler), but the semaphore scoping cannot see the bound instance.
// Recommendation: use retry({...})(fn).bind(instance) instead.

test('retry: HOF retry()(fn.bind(instance)) — scope falls back to global (bind before wrap)', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const instance_a = { name: 'a' }
  const instance_b = { name: 'b' }

  const make_handler = (inst: object) =>
    retry({
      max_attempts: 1,
      semaphore_scope: 'instance',
      semaphore_limit: 1,
      semaphore_name: 'handler_bind_before',
    })(
      async function (this: any, _event: any): Promise<string> {
        active++
        max_active = Math.max(max_active, active)
        await delay(30)
        active--
        return 'ok'
      }.bind(inst)
    )

  const handler_a = make_handler(instance_a)
  const handler_b = make_handler(instance_b)

  // Both handlers fall back to global scope (same semaphore), so they serialize
  await Promise.all([handler_a('event1'), handler_b('event2')])
  assert.equal(max_active, 1, 'bind-before-wrap: scoping falls back to global (serialized)')
})

// Folded from retry_multiprocess.test.ts to keep test layout class-based.
const tests_dir = dirname(fileURLToPath(import.meta.url))
const worker_path = resolve(tests_dir, 'retry_multiprocess_worker.ts')
const repo_root = resolve(tests_dir, '..', '..')

const runWorker = async (
  worker_id: number,
  start_ms: number,
  hold_ms: number,
  semaphore_name: string,
  semaphore_limit: number
): Promise<{ code: number | null; events: Array<Record<string, unknown>>; stderr: string }> => {
  const proc = spawn(
    process.execPath,
    ['--import', 'tsx', worker_path, String(worker_id), String(start_ms), String(hold_ms), semaphore_name, String(semaphore_limit)],
    {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    }
  )

  return await new Promise((resolvePromise, reject) => {
    const events: Array<Record<string, unknown>> = []
    let stdout = ''
    let stderr = ''

    proc.stdout?.on('data', (chunk: Buffer | string) => {
      stdout += String(chunk)
      const lines = stdout.split(/\r?\n/)
      stdout = lines.pop() ?? ''
      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed) continue
        events.push(JSON.parse(trimmed) as Record<string, unknown>)
      }
    })
    proc.stderr?.on('data', (chunk: Buffer | string) => {
      stderr += String(chunk)
    })
    proc.once('error', reject)
    proc.once('close', (code) => {
      if (stdout.trim()) {
        events.push(JSON.parse(stdout.trim()) as Record<string, unknown>)
      }
      resolvePromise({ code, events, stderr })
    })
  })
}

test('retry: semaphore_scope=multiprocess serializes across JS processes', async () => {
  const semaphore_name = `retry-multiprocess-${Date.now()}-${Math.random().toString(16).slice(2)}`
  const start_ms = Date.now()

  const first_batch = [runWorker(0, start_ms, 700, semaphore_name, 2), runWorker(1, start_ms, 700, semaphore_name, 2)]
  await delay(150)
  const second_batch = [runWorker(2, start_ms, 200, semaphore_name, 2), runWorker(3, start_ms, 200, semaphore_name, 2)]
  const results = await Promise.all([...first_batch, ...second_batch])

  for (const result of results) {
    assert.equal(result.code, 0, result.stderr || JSON.stringify(result.events))
  }

  const acquired = results
    .flatMap((result) => result.events)
    .filter((event) => event.type === 'acquired')
    .sort((a, b) => Number(a.at_ms) - Number(b.at_ms))
  const timeline = results
    .flatMap((result) => result.events)
    .filter((event) => event.type === 'acquired' || event.type === 'released')
    .sort((a, b) => {
      const delta = Number(a.at_ms) - Number(b.at_ms)
      if (delta !== 0) return delta
      return a.type === 'released' ? -1 : 1
    })

  const completed = results.flatMap((result) => result.events).filter((event) => event.type === 'completed')
  assert.equal(acquired.length, 4)
  assert.equal(completed.length, 4)

  let in_flight = 0
  for (const event of timeline) {
    in_flight += event.type === 'acquired' ? 1 : -1
    assert.ok(in_flight >= 0, `negative in-flight count after ${JSON.stringify(event)}`)
    assert.ok(in_flight <= 2, `semaphore limit exceeded by ${JSON.stringify(event)}`)
  }
})

test('retry: semaphore_scope=multiprocess contends with Python retry() using the same semaphore name', async () => {
  const local_venv_python = resolve(
    repo_root,
    '.venv',
    process.platform === 'win32' ? 'Scripts' : 'bin',
    process.platform === 'win32' ? 'python.exe' : 'python'
  )
  const candidates = [
    ...(existsSync(local_venv_python) ? [{ executable: local_venv_python, args: [] as string[] }] : []),
    ...(spawnSync('uv', ['--version'], { stdio: 'ignore' }).status === 0 ? [{ executable: 'uv', args: ['run', 'python'] }] : []),
    ...(spawnSync('python3', ['-c', 'print("ok")'], { stdio: 'ignore' }).status === 0
      ? [{ executable: 'python3', args: [] as string[] }]
      : []),
    { executable: 'python', args: [] as string[] },
  ]
  const python = candidates.find((candidate) => {
    const probe = spawnSync(candidate.executable, [...candidate.args, '-c', 'import abxbus.retry'], { cwd: repo_root, stdio: 'ignore' })
    return probe.status === 0
  })
  if (!python) {
    throw new Error('python abxbus runtime is unavailable for cross-language multiprocess test')
  }

  const semaphore_name = `retry-crosslang-${Date.now()}-${Math.random().toString(16).slice(2)}`
  const python_lock = spawn(
    python.executable,
    [
      ...python.args,
      '-u',
      '-c',
      `
import asyncio
import sys
from abxbus.retry import retry

@retry(max_attempts=1, timeout=5, semaphore_limit=1, semaphore_name=sys.argv[1], semaphore_scope='multiprocess', semaphore_timeout=5, semaphore_lax=False)
async def hold_lock():
    print("LOCKED", flush=True)
    await asyncio.sleep(float(sys.argv[2]))

asyncio.run(hold_lock())
      `,
      semaphore_name,
      '0.7',
    ],
    {
      cwd: repo_root,
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  )

  let stdout = ''
  let stderr = ''
  const python_lock_exit = new Promise<number | null>((resolvePromise, reject) => {
    python_lock.once('error', reject)
    python_lock.stderr?.on('data', (chunk: Buffer | string) => {
      stderr += String(chunk)
    })
    python_lock.once('close', (code) => {
      resolvePromise(code)
    })
  })

  await new Promise<void>((resolvePromise, reject) => {
    python_lock.stdout?.on('data', (chunk: Buffer | string) => {
      stdout += String(chunk)
      if (stdout.includes('LOCKED')) {
        resolvePromise()
      }
    })
    python_lock.once('error', reject)
    python_lock.once('close', (code) => {
      if (!stdout.includes('LOCKED')) {
        reject(new Error(`python locker exited before acquiring (code=${code ?? 'null'}): ${stderr.trim()}`))
      }
    })
  })

  const guarded = retry({
    max_attempts: 1,
    timeout: 5,
    semaphore_limit: 1,
    semaphore_name,
    semaphore_scope: 'multiprocess',
    semaphore_timeout: 5,
    semaphore_lax: false,
  })(async () => Date.now())

  const started = Date.now()
  await guarded()
  const elapsed_ms = Date.now() - started

  const python_lock_code = await python_lock_exit
  assert.equal(python_lock_code, 0, stderr.trim())

  assert.ok(elapsed_ms >= 500, `expected JS acquisition to wait behind Python lock, got ${elapsed_ms}ms`)
})
