import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, retry } from '../src/index.js'

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
- event.done() inside handler doesn’t jump the queue across buses.
- Queue-jump bypasses locks incorrectly in contexts where it shouldn’t.
- Queue-jump fails when event already in-flight.

E) FIFO correctness
- FIFO order broken under bus-serial with interleaved emissions.
- FIFO order broken under global-serial across buses.
- FIFO order broken with forwarded events.

F) Forwarding & bus context
- Forwarded event’s event.bus mutates current handler context (wrong bus).
- Child events emitted after forwarding are mis-parented.
- event.event_path diverges between buses.
- Handler attribution lost when forwarded across buses (tree/log issues).

G) Parent/child tracking
- Child events not correctly linked to the parent handler when emitted via event.bus.
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
- event.done() resolves before children complete.
- event.done() never resolves due to deadlock in runloop.

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

test('global-serial: awaited child jumps ahead of queued events across buses', async () => {
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
    const child = event.bus?.emit(ChildEvent({}))!
    bus_b.emit(child)
    order.push('child_dispatched')
    await child.done()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.emit(ParentEvent({}))
  await parent.done()
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
    const child = event.bus?.emit(ChildEvent({}))!
    await child.done()
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

  await parent.done()
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
  await event.done()
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
  await event.done()
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
  await event.done()
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
    const child = event.bus?.emit(ChildEvent({}))!
    order.push('child_dispatched')
    await child.done()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus.emit(ParentEvent({}))
  await parent.done()
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
    const shared = event.bus?.emit(SharedEvent({}))!
    order.push('shared_dispatched')
    await shared.done()
    order.push('shared_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.emit(ParentEvent({}))
  await parent.done()
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
  const done_promise = child.done().then(() => {
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
  await event.done()
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
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindPastBus')
  assert.equal(typeof found.bus.emit, 'function')
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
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindFutureBus')

  resolve()
  const completed = await found.done()
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
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindWaitBus')
  await found.done()
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
  await found.done()
})
