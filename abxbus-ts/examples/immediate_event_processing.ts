#!/usr/bin/env -S node --import tsx
// Run: node --import tsx examples/immediate_event_processing.ts

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

// Parent handler runs two scenarios:
// 1) await child.done()               -> immediate queue-jump processing
// 2) await child.eventCompleted()     -> normal queue processing
const ParentEvent = BaseEvent.extend('ImmediateProcessingParentEvent', {
  mode: z.enum(['immediate', 'queued']),
})

const ChildEvent = BaseEvent.extend('ImmediateProcessingChildEvent', {
  scenario: z.enum(['immediate', 'queued']),
})

const SiblingEvent = BaseEvent.extend('ImmediateProcessingSiblingEvent', {
  scenario: z.enum(['immediate', 'queued']),
})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

type Scenario = 'immediate' | 'queued'

async function main(): Promise<void> {
  // Two buses: bus_a is the source, bus_b is the forward target.
  const bus_a = new EventBus('QueueJumpDemoA', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const bus_b = new EventBus('QueueJumpDemoB', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })

  // Simple step counter so ordering is easy to read in stdout.
  let step = 0
  const log = (message: string): void => {
    step += 1
    console.log(`${String(step).padStart(2, '0')}. ${message}`)
  }

  // Forwarding setup: both sibling/child events emitted on bus_a are forwarded to bus_b.
  bus_a.on(ChildEvent, (event) => {
    log(`[forward] ${event.event_type}(${event.scenario}) bus_a -> bus_b`)
    bus_b.emit(event)
  })
  bus_a.on(SiblingEvent, (event) => {
    log(`[forward] ${event.event_type}(${event.scenario}) bus_a -> bus_b`)
    bus_b.emit(event)
  })

  // Local handlers on bus_a.
  bus_a.on(ChildEvent, async (event) => {
    log(`[bus_a] child start (${event.scenario})`)
    await delay(8)
    log(`[bus_a] child end (${event.scenario})`)
  })
  bus_a.on(SiblingEvent, async (event) => {
    log(`[bus_a] sibling start (${event.scenario})`)
    await delay(14)
    log(`[bus_a] sibling end (${event.scenario})`)
  })

  // Forwarded handlers on bus_b.
  bus_b.on(ChildEvent, async (event) => {
    log(`[bus_b] child start (${event.scenario})`)
    await delay(4)
    log(`[bus_b] child end (${event.scenario})`)
  })
  bus_b.on(SiblingEvent, async (event) => {
    log(`[bus_b] sibling start (${event.scenario})`)
    await delay(6)
    log(`[bus_b] sibling end (${event.scenario})`)
  })

  // Parent handler queues sibling first, then child, then compares await behavior.
  bus_a.on(ParentEvent, async (event) => {
    log(`[parent:${event.mode}] start`)

    // Queue a sibling first so normal queue order has sibling ahead of child.
    event.bus?.emit(SiblingEvent({ scenario: event.mode }))
    log(`[parent:${event.mode}] sibling queued`)

    // Queue child second; this is the event we await in two different ways.
    const child = event.bus?.emit(ChildEvent({ scenario: event.mode }))!
    log(`[parent:${event.mode}] child queued`)

    if (event.mode === 'immediate') {
      // Queue-jump: child processes immediately while still inside parent handler.
      log(`[parent:${event.mode}] await child.done()`)
      await child.done()
      log(`[parent:${event.mode}] child.done() resolved`)
    } else {
      // Normal queue wait: child waits its turn behind already-queued sibling work.
      log(`[parent:${event.mode}] await child.eventCompleted()`)
      await child.eventCompleted()
      log(`[parent:${event.mode}] child.eventCompleted() resolved`)
    }

    log(`[parent:${event.mode}] end`)
  })

  const runScenario = async (mode: Scenario): Promise<void> => {
    log(`----- scenario=${mode} -----`)

    // Parent event uses parallel concurrency so eventCompleted() in handler
    // can wait safely while other queued events continue to run.
    const parent = bus_a.emit(
      ParentEvent({
        mode,
        event_concurrency: 'parallel',
      })
    )

    await parent.eventCompleted()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
    log(`----- done scenario=${mode} -----`)
  }

  await runScenario('immediate')
  await runScenario('queued')

  console.log('\nExpected behavior:')
  console.log('- immediate: child runs before sibling (queue-jump) and parent resumes right after child.')
  console.log('- queued: sibling runs first, child waits in normal queue order, parent resumes later.')
  console.log('\n=== bus_a.logTree() ===')
  console.log(bus_a.logTree())
  console.log('\n=== bus_b.logTree() ===')
  console.log(bus_b.logTree())
}

await main()
