#!/usr/bin/env -S node --import tsx
// Run: node --import tsx examples/concurrency_options.ts

import assert from 'node:assert/strict'
import { z } from 'zod'
import { BaseEvent, EventBus, EventHandlerTimeoutError } from '../src/index.js'
const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

const makeLogger = (section: string) => {
  const started_at = performance.now()
  return (message: string) => {
    const elapsed = (performance.now() - started_at).toFixed(1)
    console.log(`[${section}] +${elapsed}ms ${message}`)
  }
}
const WorkEvent = BaseEvent.extend('ConcurrencyOptionsWorkEvent', { lane: z.string(), order: z.number(), ms: z.number() })
const HandlerEvent = BaseEvent.extend('ConcurrencyOptionsHandlerEvent', { label: z.string() })
const OverrideEvent = BaseEvent.extend('ConcurrencyOptionsOverrideEvent', { label: z.string(), order: z.number(), ms: z.number() })
const TimeoutEvent = BaseEvent.extend('ConcurrencyOptionsTimeoutEvent', { ms: z.number() })

// 1) Event concurrency at bus level: global-serial vs bus-serial.
// Observe how max in-flight events differs across two buses.
async function eventConcurrencyDemo(): Promise<void> {
  const global_log = makeLogger('event:global-serial')
  const global_a = new EventBus('GlobalSerialA', { event_concurrency: 'global-serial', event_handler_concurrency: 'serial' })
  const global_b = new EventBus('GlobalSerialB', { event_concurrency: 'global-serial', event_handler_concurrency: 'serial' })
  let global_in_flight = 0
  let global_max = 0
  const global_handler = async (event: InstanceType<typeof WorkEvent>) => {
    global_in_flight += 1
    global_max = Math.max(global_max, global_in_flight)
    global_log(`${event.lane}${event.order} start (global in-flight=${global_in_flight})`)
    await sleep(event.ms)
    global_log(`${event.lane}${event.order} end`)
    global_in_flight -= 1
  }
  global_a.on(WorkEvent, global_handler)
  global_b.on(WorkEvent, global_handler)
  global_a.emit(WorkEvent({ lane: 'A', order: 0, ms: 45 }))
  global_b.emit(WorkEvent({ lane: 'B', order: 0, ms: 45 }))
  global_a.emit(WorkEvent({ lane: 'A', order: 1, ms: 45 }))
  global_b.emit(WorkEvent({ lane: 'B', order: 1, ms: 45 }))
  await Promise.all([global_a.waitUntilIdle(), global_b.waitUntilIdle()])
  global_log(`max in-flight across both buses: ${global_max} (expect 1 in global-serial)`)
  console.log('\n=== global_a.logTree() ===')
  console.log(global_a.logTree())
  console.log('\n=== global_b.logTree() ===')
  console.log(global_b.logTree())
  const bus_log = makeLogger('event:bus-serial')
  const bus_a = new EventBus('BusSerialA', { event_concurrency: 'bus-serial', event_handler_concurrency: 'serial' })
  const bus_b = new EventBus('BusSerialB', { event_concurrency: 'bus-serial', event_handler_concurrency: 'serial' })
  const per_bus_in_flight = { A: 0, B: 0 }
  const per_bus_max = { A: 0, B: 0 }
  let mixed_global_in_flight = 0
  let mixed_global_max = 0
  const bus_handler = async (event: InstanceType<typeof WorkEvent>) => {
    const lane = event.lane as 'A' | 'B'
    mixed_global_in_flight += 1
    mixed_global_max = Math.max(mixed_global_max, mixed_global_in_flight)
    per_bus_in_flight[lane] += 1
    per_bus_max[lane] = Math.max(per_bus_max[lane], per_bus_in_flight[lane])
    bus_log(`${lane}${event.order} start (global=${mixed_global_in_flight}, lane=${per_bus_in_flight[lane]})`)
    await sleep(event.ms)
    bus_log(`${lane}${event.order} end`)
    per_bus_in_flight[lane] -= 1
    mixed_global_in_flight -= 1
  }
  bus_a.on(WorkEvent, bus_handler)
  bus_b.on(WorkEvent, bus_handler)
  bus_a.emit(WorkEvent({ lane: 'A', order: 0, ms: 45 }))
  bus_b.emit(WorkEvent({ lane: 'B', order: 0, ms: 45 }))
  bus_a.emit(WorkEvent({ lane: 'A', order: 1, ms: 45 }))
  bus_b.emit(WorkEvent({ lane: 'B', order: 1, ms: 45 }))
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
  bus_log(`max in-flight global=${mixed_global_max}, per-bus A=${per_bus_max.A}, B=${per_bus_max.B} (expect global >= 2, per-bus = 1)`)
  console.log('\n=== bus_a.logTree() ===')
  console.log(bus_a.logTree())
  console.log('\n=== bus_b.logTree() ===')
  console.log(bus_b.logTree())
}

// 2) Handler concurrency at bus level: serial vs parallel on the same event.
// Observe handler overlap for one event with two handlers.
async function handlerConcurrencyDemo(): Promise<void> {
  const run_case = async (mode: 'serial' | 'parallel') => {
    const log = makeLogger(`handler:${mode}`)
    const bus = new EventBus(`HandlerMode-${mode}`, { event_concurrency: 'parallel', event_handler_concurrency: mode })
    let in_flight = 0
    let max_in_flight = 0
    const make_handler = (name: string, ms: number) => async (event: InstanceType<typeof HandlerEvent>) => {
      in_flight += 1
      max_in_flight = Math.max(max_in_flight, in_flight)
      log(`${event.label}:${name} start (handlers in-flight=${in_flight})`)
      await sleep(ms)
      log(`${event.label}:${name} end`)
      in_flight -= 1
    }
    bus.on(HandlerEvent, make_handler('slow', 60))
    bus.on(HandlerEvent, make_handler('fast', 20))
    const event = bus.emit(HandlerEvent({ label: mode }))
    await event.done()
    await bus.waitUntilIdle()
    log(`max handler overlap: ${max_in_flight} (expect 1 for serial, >= 2 for parallel)`)
    console.log(`\n=== ${bus.name}.logTree() ===`)
    console.log(bus.logTree())
  }
  await run_case('serial')
  await run_case('parallel')
}

// 3) Event-level overrides take precedence over bus defaults.
// Bus defaults are strict (bus-serial + serial), then we override both to parallel on event instances.
async function eventOverrideDemo(): Promise<void> {
  const log = makeLogger('override:precedence')
  const bus = new EventBus('OverrideBus', { event_concurrency: 'bus-serial', event_handler_concurrency: 'serial' })
  let active_events = new Set<string>()
  let per_event_handlers = new Map<string, number>()
  let active_handlers = 0
  let max_handlers = 0
  let max_events = 0

  const reset_metrics = () => {
    active_events = new Set()
    per_event_handlers = new Map()
    active_handlers = 0
    max_handlers = 0
    max_events = 0
  }
  const track_start = (event: InstanceType<typeof OverrideEvent>, handler_name: string, label: string) => {
    active_handlers += 1
    max_handlers = Math.max(max_handlers, active_handlers)
    const count = (per_event_handlers.get(event.event_id) ?? 0) + 1
    per_event_handlers.set(event.event_id, count)
    active_events.add(event.event_id)
    max_events = Math.max(max_events, active_events.size)
    log(`${label}:${event.order}:${handler_name} start (events=${active_events.size}, handlers=${active_handlers})`)
  }
  const track_end = (event: InstanceType<typeof OverrideEvent>, handler_name: string, label: string) => {
    active_handlers -= 1
    const count = (per_event_handlers.get(event.event_id) ?? 1) - 1
    if (count <= 0) {
      per_event_handlers.delete(event.event_id)
      active_events.delete(event.event_id)
    } else {
      per_event_handlers.set(event.event_id, count)
    }
    log(`${label}:${event.order}:${handler_name} end`)
  }

  const run_pair = async (label: string, use_override: boolean) => {
    reset_metrics()
    const handler_a = async (event: InstanceType<typeof OverrideEvent>) => {
      track_start(event, 'A', label)
      await sleep(event.ms)
      track_end(event, 'A', label)
    }
    const handler_b = async (event: InstanceType<typeof OverrideEvent>) => {
      track_start(event, 'B', label)
      await sleep(event.ms)
      track_end(event, 'B', label)
    }
    bus.off(OverrideEvent)
    bus.on(OverrideEvent, handler_a)
    bus.on(OverrideEvent, handler_b)
    const overrides = use_override ? ({ event_concurrency: 'parallel', event_handler_concurrency: 'parallel' } as const) : {}
    bus.emit(OverrideEvent({ label, order: 0, ms: 45, ...overrides }))
    bus.emit(OverrideEvent({ label, order: 1, ms: 45, ...overrides }))
    await bus.waitUntilIdle()
    log(`${label} summary -> max events=${max_events}, max handlers=${max_handlers}`)
  }

  await run_pair('bus-defaults', false)
  await run_pair('event-overrides', true)
  console.log('\n=== OverrideBus.logTree() ===')
  console.log(bus.logTree())
}

// 4) Handler-level timeout via bus.on(..., { handler_timeout }).
// Observe one handler timing out while another succeeds on the same event.
async function handlerTimeoutDemo(): Promise<void> {
  const log = makeLogger('timeout:handler-option')
  const bus = new EventBus('TimeoutBus', { event_concurrency: 'parallel', event_handler_concurrency: 'parallel', event_timeout: 0.2 })

  const slow_entry = bus.on(
    TimeoutEvent,
    async (event) => {
      log('slow handler start')
      await sleep(event.ms)
      log('slow handler finished body (but may already be timed out)')
      return 'slow'
    },
    { handler_timeout: 0.03 }
  )
  const fast_entry = bus.on(
    TimeoutEvent,
    async () => {
      log('fast handler start')
      await sleep(10)
      log('fast handler end')
      return 'fast'
    },
    { handler_timeout: 0.1 }
  )
  const event = bus.emit(TimeoutEvent({ ms: 60, event_handler_timeout: 0.5 }))
  await event.done({ raise_if_any: false })
  const slow_result = event.event_results.get(slow_entry.id)
  const fast_result = event.event_results.get(fast_entry.id)
  assert.ok(slow_result, 'expected slow handler result entry')
  assert.ok(slow_result.error instanceof EventHandlerTimeoutError, 'expected slow handler to time out')
  assert.equal(fast_result?.status, 'completed')
  assert.equal(fast_result?.result, 'fast')
  const handler_timed_out = slow_result?.error instanceof EventHandlerTimeoutError
  log(`slow handler status=${slow_result?.status}, timeout_error=${handler_timed_out ? 'yes' : 'no'}`)
  await bus.waitUntilIdle()
  console.log('\n=== TimeoutBus.logTree() ===')
  console.log(bus.logTree())
}

async function main(): Promise<void> {
  await eventConcurrencyDemo()
  await handlerConcurrencyDemo()
  await eventOverrideDemo()
  await handlerTimeoutDemo()
}
await main()
