#!/usr/bin/env -S node --import tsx
// Run: node --import tsx examples/parent_child_tracking.ts

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

// Step 1: Define a tiny parent -> child -> grandchild event model.
const ParentEvent = BaseEvent.extend('ParentEvent', {
  workflow: z.string(),
})

const ChildEvent = BaseEvent.extend('ChildEvent', {
  stage: z.string(),
})

const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {
  note: z.string(),
})

const shortId = (id?: string | null): string => (id ? id.slice(-8) : 'none')

async function main(): Promise<void> {
  // Step 2: Create one bus so parent/child linkage is easy to inspect in one history.
  const bus = new EventBus('ParentChildTrackingBus')

  // Step 3: Child handler dispatches a grandchild through event.bus.
  // Because this runs inside ChildEvent handling, grandchild gets linked automatically.
  bus.on(ChildEvent, async (event: InstanceType<typeof ChildEvent>): Promise<string> => {
    console.log(`child handler start: ${event.event_type}#${shortId(event.event_id)}`)

    const grandchild = event.bus?.emit(
      GrandchildEvent({
        note: `spawned by ${event.stage}`,
      })
    )

    if (grandchild) {
      console.log(
        `  child dispatched grandchild: ${grandchild.event_type}#${shortId(grandchild.event_id)} parent_id=${shortId(grandchild.event_parent_id)}`
      )

      // Step 4: Await a nested event so ordering and linkage are explicit in output.
      await grandchild.done()
      console.log(`  child resumed after grandchild.done(): ${shortId(grandchild.event_id)}`)
    }

    return `child_completed:${event.stage}`
  })

  // Step 5: Grandchild handler is simple; it just marks completion with a string result.
  bus.on(GrandchildEvent, async (event: InstanceType<typeof GrandchildEvent>): Promise<string> => {
    console.log(`grandchild handler: ${event.event_type}#${shortId(event.event_id)} note="${event.note}"`)
    return `grandchild_completed:${event.note}`
  })

  // Step 6: Parent handler emits/dispatches child events via event.bus.
  // One child is awaited with .done() to clearly show queue-jump + linkage behavior.
  bus.on(ParentEvent, async (event: InstanceType<typeof ParentEvent>): Promise<string> => {
    console.log(`parent handler start: ${event.event_type}#${shortId(event.event_id)} workflow="${event.workflow}"`)

    const awaitedChild = event.bus?.emit(ChildEvent({ stage: 'awaited-child' }))
    if (awaitedChild) {
      console.log(
        `  parent emitted child: ${awaitedChild.event_type}#${shortId(awaitedChild.event_id)} parent_id=${shortId(awaitedChild.event_parent_id)}`
      )

      // Required by this example: await at least one child so parent/child linkage is obvious.
      await awaitedChild.done()
      console.log(`  parent resumed after awaited child.done(): ${shortId(awaitedChild.event_id)}`)
    }

    const backgroundChild = event.bus?.emit(ChildEvent({ stage: 'background-child' }))
    if (backgroundChild) {
      console.log(
        `  parent dispatched second child: ${backgroundChild.event_type}#${shortId(backgroundChild.event_id)} parent_id=${shortId(backgroundChild.event_parent_id)}`
      )
    }

    // Parent also dispatches a GrandchildEvent type directly via event.bus.
    // This is still automatically linked to the parent event.
    const directGrandchild = event.bus?.emit(GrandchildEvent({ note: 'directly from parent' }))
    if (directGrandchild) {
      console.log(
        `  parent dispatched grandchild type directly: ${directGrandchild.event_type}#${shortId(directGrandchild.event_id)} parent_id=${shortId(directGrandchild.event_parent_id)}`
      )
      await directGrandchild.done()
    }

    return 'parent_completed'
  })

  // Step 7: Dispatch parent and wait for full bus idle so history is complete.
  const parent = bus.emit(ParentEvent({ workflow: 'demo-parent-child-tracking' }))
  await parent.done()
  await bus.waitUntilIdle()

  // Step 8: Print IDs + relationship checks from event history.
  console.log('\n=== Event History Relationships ===')
  const history = Array.from(bus.event_history.values()).sort((a, b) => {
    if (a.event_created_at === b.event_created_at) {
      return 0
    }
    return a.event_created_at < b.event_created_at ? -1 : 1
  })

  for (const item of history) {
    const parentEvent = item.event_parent
    console.log(
      [
        `${item.event_type}#${shortId(item.event_id)}`,
        `parent=${parentEvent ? `${parentEvent.event_type}#${shortId(parentEvent.event_id)}` : 'none'}`,
        `isChildOfRoot=${bus.eventIsChildOf(item, parent)}`,
        `rootIsParentOf=${bus.eventIsParentOf(parent, item)}`,
      ].join(' | ')
    )
  }

  const firstChild = history.find((event) => event.event_type === 'ChildEvent')
  const nestedGrandchild = history.find(
    (event) => event.event_type === 'GrandchildEvent' && firstChild && event.event_parent_id === firstChild.event_id
  )
  if (firstChild && nestedGrandchild) {
    console.log(
      `grandchild->child relationship check: ${nestedGrandchild.event_type}#${shortId(nestedGrandchild.event_id)} is child of ${firstChild.event_type}#${shortId(firstChild.event_id)} = ${bus.eventIsChildOf(nestedGrandchild, firstChild)}`
    )
  }

  // Step 9: Print the built-in tree view from event history.
  console.log('\n=== bus.logTree() ===')
  const tree = bus.logTree()
  console.log(tree)
}

await main()
