#!/usr/bin/env -S node --import tsx
// Run: node --import tsx examples/forwarding_between_busses.ts

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ForwardedEvent = BaseEvent.extend('ForwardedEvent', {
  message: z.string(),
})

async function main(): Promise<void> {
  const busA = new EventBus('BusA')
  const busB = new EventBus('BusB')
  const busC = new EventBus('BusC')

  const handleCounts = {
    BusA: 0,
    BusB: 0,
    BusC: 0,
  }

  const seenEventIds = {
    BusA: new Set<string>(),
    BusB: new Set<string>(),
    BusC: new Set<string>(),
  }

  // Each bus handles the typed event locally.
  // In a forwarding cycle, loop prevention should keep each bus to one handle.
  busA.on(ForwardedEvent, (event) => {
    handleCounts.BusA += 1
    seenEventIds.BusA.add(event.event_id)
    console.log(`[BusA] handled ${event.event_id} (count=${handleCounts.BusA})`)
  })

  busB.on(ForwardedEvent, (event) => {
    handleCounts.BusB += 1
    seenEventIds.BusB.add(event.event_id)
    console.log(`[BusB] handled ${event.event_id} (count=${handleCounts.BusB})`)
  })

  busC.on(ForwardedEvent, (event) => {
    handleCounts.BusC += 1
    seenEventIds.BusC.add(event.event_id)
    console.log(`[BusC] handled ${event.event_id} (count=${handleCounts.BusC})`)
  })

  // Forward all events in a ring:
  // A -> B -> C -> A
  // Expected for one dispatch from A: event path becomes [A, B, C] and stops.
  // The C -> A edge is skipped because A is already in event_path.
  busA.on('*', busB.emit)
  busB.on('*', busC.emit)
  busC.on('*', busA.emit)

  console.log('Dispatching ForwardedEvent on BusA with cyclic forwarding A -> B -> C -> A')

  const event = busA.emit(
    ForwardedEvent({
      message: 'hello across 3 buses',
    })
  )

  // done() waits for handlers on all forwarded buses, not just the origin bus.
  await event.done()
  await Promise.all([busA.waitUntilIdle(), busB.waitUntilIdle(), busC.waitUntilIdle()])

  const path = event.event_path
  const totalHandles = handleCounts.BusA + handleCounts.BusB + handleCounts.BusC

  console.log('\nFinal propagation summary:')
  console.log(`- event_id: ${event.event_id}`)
  console.log(`- event_path: ${path.join(' -> ')}`)
  console.log(`- handle counts: ${JSON.stringify(handleCounts)}`)
  console.log(`- unique ids seen per bus: A=${seenEventIds.BusA.size}, B=${seenEventIds.BusB.size}, C=${seenEventIds.BusC.size}`)
  console.log(`- total handles: ${totalHandles}`)

  const handledOncePerBus = handleCounts.BusA === 1 && handleCounts.BusB === 1 && handleCounts.BusC === 1
  const visitedThreeBuses = path.length === 3

  if (handledOncePerBus && visitedThreeBuses) {
    console.log('\nLoop prevention confirmed: each bus handled the event at most once.')
  } else {
    console.log('\nUnexpected forwarding result. Check handlers/forwarding setup.')
  }

  console.log('\n=== BusA logTree() ===')
  console.log(busA.logTree())
  console.log('\n=== BusB logTree() ===')
  console.log(busB.logTree())
  console.log('\n=== BusC logTree() ===')
  console.log(busC.logTree())
}

await main()
