import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, type EventResult } from '../src/index.js'

const PingEvent = BaseEvent.extend('PingEvent', { value: z.number() })
const OrderEvent = BaseEvent.extend('OrderEvent', { order: z.number() })
const ForwardedDefaultsTriggerEvent = BaseEvent.extend('ForwardedDefaultsTriggerEvent', {})
const ForwardedDefaultsChildEvent = BaseEvent.extend('ForwardedDefaultsChildEvent', {
  mode: z.enum(['inherited', 'override']),
})
const ForwardedFirstDefaultsEvent = BaseEvent.extend('ForwardedFirstDefaultsEvent', { event_result_type: z.string() })
const ProxyDispatchRootEvent = BaseEvent.extend('ProxyDispatchRootEvent', {})
const ProxyDispatchChildEvent = BaseEvent.extend('ProxyDispatchChildEvent', {})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

const indexOf = (values: string[], value: string): number => {
  const index = values.indexOf(value)
  assert.notEqual(index, -1, `missing required log entry ${value}; log=${values.join(',')}`)
  return index
}

test('test_events_forward_between_buses_without_duplication', async () => {
  const bus_a = new EventBus('BusA')
  const bus_b = new EventBus('BusB')
  const bus_c = new EventBus('BusC')
  const seen_a: string[] = []
  const seen_b: string[] = []
  const seen_c: string[] = []

  try {
    bus_a.on(PingEvent, (event) => {
      seen_a.push(event.event_id)
    })
    bus_b.on(PingEvent, (event) => {
      seen_b.push(event.event_id)
    })
    bus_c.on(PingEvent, (event) => {
      seen_c.push(event.event_id)
    })
    bus_a.on('*', bus_b.emit)
    bus_b.on('*', bus_c.emit)

    const event = bus_a.emit(PingEvent({ value: 1 }))
    await event.now()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle(), bus_c.waitUntilIdle()])

    assert.deepEqual(seen_a, [event.event_id])
    assert.deepEqual(seen_b, [event.event_id])
    assert.deepEqual(seen_c, [event.event_id])
    assert.deepEqual(event.event_path, [bus_a.label, bus_b.label, bus_c.label])
    assert.equal(event.event_pending_bus_count, 0)
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy(), bus_c.destroy()])
  }
})

test('test_tree_level_hierarchy_bubbling', async () => {
  const parent_bus = new EventBus('ParentBus')
  const child_bus = new EventBus('ChildBus')
  const subchild_bus = new EventBus('SubchildBus')
  const seen_parent: string[] = []
  const seen_child: string[] = []
  const seen_subchild: string[] = []

  try {
    parent_bus.on(PingEvent, (event) => {
      seen_parent.push(event.event_id)
    })
    child_bus.on(PingEvent, (event) => {
      seen_child.push(event.event_id)
    })
    subchild_bus.on(PingEvent, (event) => {
      seen_subchild.push(event.event_id)
    })
    child_bus.on('*', parent_bus.emit)
    subchild_bus.on('*', child_bus.emit)

    const bottom = subchild_bus.emit(PingEvent({ value: 1 }))
    await bottom.now()
    await Promise.all([subchild_bus.waitUntilIdle(), child_bus.waitUntilIdle(), parent_bus.waitUntilIdle()])

    assert.deepEqual(seen_subchild, [bottom.event_id])
    assert.deepEqual(seen_child, [bottom.event_id])
    assert.deepEqual(seen_parent, [bottom.event_id])
    assert.deepEqual(bottom.event_path, [subchild_bus.label, child_bus.label, parent_bus.label])

    seen_parent.length = 0
    seen_child.length = 0
    seen_subchild.length = 0

    const middle = child_bus.emit(PingEvent({ value: 2 }))
    await middle.now()
    await Promise.all([child_bus.waitUntilIdle(), parent_bus.waitUntilIdle()])

    assert.deepEqual(seen_subchild, [])
    assert.deepEqual(seen_child, [middle.event_id])
    assert.deepEqual(seen_parent, [middle.event_id])
    assert.deepEqual(middle.event_path, [child_bus.label, parent_bus.label])
  } finally {
    await Promise.all([parent_bus.destroy(), child_bus.destroy(), subchild_bus.destroy()])
  }
})

test('test_forwarding_disambiguates_buses_that_share_the_same_name', async () => {
  const bus_a = new EventBus('SharedName')
  const bus_b = new EventBus('SharedName')
  const seen_a: string[] = []
  const seen_b: string[] = []

  try {
    bus_a.on(PingEvent, (event) => {
      seen_a.push(event.event_id)
    })
    bus_b.on(PingEvent, (event) => {
      seen_b.push(event.event_id)
    })
    bus_a.on('*', bus_b.emit)

    const event = bus_a.emit(PingEvent({ value: 99 }))
    await event.now()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

    assert.deepEqual(seen_a, [event.event_id])
    assert.deepEqual(seen_b, [event.event_id])
    assert.notEqual(bus_a.label, bus_b.label)
    assert.deepEqual(event.event_path, [bus_a.label, bus_b.label])
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy()])
  }
})

test('test_await_event_now_waits_for_handlers_on_forwarded_buses', async () => {
  const bus_a = new EventBus('ForwardWaitA')
  const bus_b = new EventBus('ForwardWaitB')
  const bus_c = new EventBus('ForwardWaitC')
  const completion_log: string[] = []

  try {
    bus_a.on(PingEvent, async () => {
      await delay(10)
      completion_log.push('A')
    })
    bus_b.on(PingEvent, async () => {
      await delay(30)
      completion_log.push('B')
    })
    bus_c.on(PingEvent, async () => {
      await delay(50)
      completion_log.push('C')
    })
    bus_a.on('*', bus_b.emit)
    bus_b.on('*', bus_c.emit)

    const event = bus_a.emit(PingEvent({ value: 2 }))
    await event.now()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle(), bus_c.waitUntilIdle()])

    assert.deepEqual(completion_log.sort(), ['A', 'B', 'C'])
    assert.equal(event.event_pending_bus_count, 0)
    assert.deepEqual(event.event_path, [bus_a.label, bus_b.label, bus_c.label])
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy(), bus_c.destroy()])
  }
})

test('test_circular_forwarding_from_first_peer_does_not_loop', async () => {
  const peer1 = new EventBus('Peer1')
  const peer2 = new EventBus('Peer2')
  const peer3 = new EventBus('Peer3')
  const seen1: string[] = []
  const seen2: string[] = []
  const seen3: string[] = []

  try {
    peer1.on(PingEvent, (event) => {
      seen1.push(event.event_id)
    })
    peer2.on(PingEvent, (event) => {
      seen2.push(event.event_id)
    })
    peer3.on(PingEvent, (event) => {
      seen3.push(event.event_id)
    })
    peer1.on('*', peer2.emit)
    peer2.on('*', peer3.emit)
    peer3.on('*', peer1.emit)

    const event = peer1.emit(PingEvent({ value: 42 }))
    await event.now()
    await Promise.all([peer1.waitUntilIdle(), peer2.waitUntilIdle(), peer3.waitUntilIdle()])

    assert.deepEqual(seen1, [event.event_id])
    assert.deepEqual(seen2, [event.event_id])
    assert.deepEqual(seen3, [event.event_id])
    assert.deepEqual(event.event_path, [peer1.label, peer2.label, peer3.label])
  } finally {
    await Promise.all([peer1.destroy(), peer2.destroy(), peer3.destroy()])
  }
})

test('test_circular_forwarding_from_middle_peer_does_not_loop', async () => {
  const peer1 = new EventBus('RacePeer1')
  const peer2 = new EventBus('RacePeer2')
  const peer3 = new EventBus('RacePeer3')
  const seen1: string[] = []
  const seen2: string[] = []
  const seen3: string[] = []

  try {
    peer1.on(PingEvent, (event) => {
      seen1.push(event.event_id)
    })
    peer2.on(PingEvent, (event) => {
      seen2.push(event.event_id)
    })
    peer3.on(PingEvent, (event) => {
      seen3.push(event.event_id)
    })
    peer1.on('*', peer2.emit)
    peer2.on('*', peer3.emit)
    peer3.on('*', peer1.emit)

    const warmup = peer1.emit(PingEvent({ value: 42 }))
    await warmup.now()
    await Promise.all([peer1.waitUntilIdle(), peer2.waitUntilIdle(), peer3.waitUntilIdle()])
    seen1.length = 0
    seen2.length = 0
    seen3.length = 0

    const event = peer2.emit(PingEvent({ value: 99 }))
    await event.now()
    await Promise.all([peer1.waitUntilIdle(), peer2.waitUntilIdle(), peer3.waitUntilIdle()])

    assert.deepEqual(seen1, [event.event_id])
    assert.deepEqual(seen2, [event.event_id])
    assert.deepEqual(seen3, [event.event_id])
    assert.deepEqual(event.event_path, [peer2.label, peer3.label, peer1.label])
    assert.equal(event.event_status, 'completed')
  } finally {
    await Promise.all([peer1.destroy(), peer2.destroy(), peer3.destroy()])
  }
})

test('test_await_event_now_waits_when_forwarding_handler_is_async_delayed', async () => {
  const bus_a = new EventBus('BusADelayedForward')
  const bus_b = new EventBus('BusBDelayedForward')
  let bus_a_done = false
  let bus_b_done = false

  try {
    bus_a.on(PingEvent, async () => {
      await delay(20)
      bus_a_done = true
    })
    bus_b.on(PingEvent, async () => {
      await delay(10)
      bus_b_done = true
    })
    bus_a.on('*', async (event) => {
      await delay(30)
      bus_b.emit(event)
    })

    const event = bus_a.emit(PingEvent({ value: 3 }))
    await event.now()

    assert.equal(bus_a_done, true)
    assert.equal(bus_b_done, true)
    assert.equal(event.event_pending_bus_count, 0)
    assert.deepEqual(event.event_path, [bus_a.label, bus_b.label])
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy()])
  }
})

test('test_forwarding_same_event_does_not_set_self_parent_id', async () => {
  const origin = new EventBus('SelfParentOrigin')
  const target = new EventBus('SelfParentTarget')

  try {
    origin.on(PingEvent, () => 'origin-ok')
    target.on(PingEvent, () => 'target-ok')
    origin.on('*', target.emit)

    const event = origin.emit(PingEvent({ value: 9 }))
    await event.now()
    await Promise.all([origin.waitUntilIdle(), target.waitUntilIdle()])

    assert.equal(event.event_parent_id, null)
    assert.deepEqual(event.event_path, [origin.label, target.label])
  } finally {
    await Promise.all([origin.destroy(), target.destroy()])
  }
})

test('test_forwarded_event_uses_processing_bus_defaults', async () => {
  const bus_a = new EventBus('ForwardDefaultsA', { event_handler_concurrency: 'serial', event_timeout: 1.5 })
  const bus_b = new EventBus('ForwardDefaultsB', { event_handler_concurrency: 'parallel', event_timeout: 2.5 })
  const log: string[] = []
  let inherited_ref: ReturnType<typeof ForwardedDefaultsChildEvent> | null = null

  try {
    bus_b.on(ForwardedDefaultsChildEvent, async (event) => {
      assert.equal(event.event_timeout, null)
      assert.equal(event.event_handler_concurrency ?? null, null)
      assert.equal(event.event_handler_completion ?? null, null)
      log.push(`${event.mode}:b1_start`)
      await delay(15)
      log.push(`${event.mode}:b1_end`)
      return 'b1'
    })
    bus_b.on(ForwardedDefaultsChildEvent, async (event) => {
      assert.equal(event.event_timeout, null)
      assert.equal(event.event_handler_concurrency ?? null, null)
      assert.equal(event.event_handler_completion ?? null, null)
      log.push(`${event.mode}:b2_start`)
      await delay(5)
      log.push(`${event.mode}:b2_end`)
      return 'b2'
    })
    bus_a.on(ForwardedDefaultsTriggerEvent, async (event) => {
      const inherited = event.emit(ForwardedDefaultsChildEvent({ mode: 'inherited' }))!
      inherited_ref = inherited
      bus_b.emit(inherited)
      await inherited.now()
    })

    const top = bus_a.emit(ForwardedDefaultsTriggerEvent({}))
    await top.now()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

    assert.ok(indexOf(log, 'inherited:b2_start') < indexOf(log, 'inherited:b1_end'), `inherited mode should use bus_b parallel; log=${log}`)
    assert.ok(inherited_ref)
    const inherited_event = inherited_ref as ReturnType<typeof ForwardedDefaultsChildEvent>
    assert.equal(inherited_event.event_timeout, null)
    assert.equal(inherited_event.event_handler_concurrency ?? null, null)
    assert.equal(inherited_event.event_handler_completion ?? null, null)
    const bus_b_results = (Array.from(inherited_event.event_results.values()) as EventResult[]).filter(
      (result) => result.eventbus_id === bus_b.id
    )
    assert.ok(bus_b_results.length > 0)
    assert.deepEqual(
      bus_b_results.map((result) => result.handler_timeout),
      bus_b_results.map(() => bus_b.event_timeout)
    )
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy()])
  }
})

test('test_forwarded_event_preserves_explicit_handler_concurrency_override', async () => {
  const bus_a = new EventBus('ForwardOverrideA', { event_handler_concurrency: 'parallel' })
  const bus_b = new EventBus('ForwardOverrideB', { event_handler_concurrency: 'parallel' })
  const log: string[] = []

  try {
    bus_b.on(ForwardedDefaultsChildEvent, async (event) => {
      log.push(`${event.mode}:b1_start`)
      await delay(15)
      log.push(`${event.mode}:b1_end`)
      return 'b1'
    })
    bus_b.on(ForwardedDefaultsChildEvent, async (event) => {
      log.push(`${event.mode}:b2_start`)
      await delay(5)
      log.push(`${event.mode}:b2_end`)
      return 'b2'
    })
    bus_a.on(ForwardedDefaultsTriggerEvent, async (event) => {
      const override = event.emit(
        ForwardedDefaultsChildEvent({
          mode: 'override',
          event_timeout: 0,
          event_handler_concurrency: 'serial',
        })
      )!
      bus_b.emit(override)
      await override.now()
    })

    const top = bus_a.emit(ForwardedDefaultsTriggerEvent({}))
    await top.now()
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

    assert.ok(indexOf(log, 'override:b1_end') < indexOf(log, 'override:b2_start'), `explicit override should force serial; log=${log}`)
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy()])
  }
})

test('test_forwarded_first_mode_uses_processing_bus_handler_concurrency_defaults', async () => {
  const bus_a = new EventBus('ForwardedFirstDefaultsA', {
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const bus_b = new EventBus('ForwardedFirstDefaultsB', {
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
  })
  const log: string[] = []

  try {
    bus_a.on('*', bus_b.emit)
    bus_b.on(ForwardedFirstDefaultsEvent, async () => {
      log.push('slow_start')
      await delay(20)
      log.push('slow_end')
      return 'slow'
    })
    bus_b.on(ForwardedFirstDefaultsEvent, async () => {
      log.push('fast_start')
      await delay(1)
      log.push('fast_end')
      return 'fast'
    })

    const result = await bus_a
      .emit(ForwardedFirstDefaultsEvent({ event_timeout: 0 }))
      .now({ first_result: true })
      .eventResult({ raise_if_any: false })
    await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

    assert.equal(result, 'fast', `first-mode on processing bus should pick fast handler, got ${String(result)} log=${log}`)
    assert.equal(log.includes('slow_start'), true, `slow handler should start under parallel first-mode, log=${log}`)
    assert.equal(log.includes('fast_start'), true, `fast handler should start under parallel first-mode, log=${log}`)
  } finally {
    await Promise.all([bus_a.destroy(), bus_b.destroy()])
  }
})

test('test_proxy_dispatch_auto_links_child_events_like_emit', async () => {
  const bus = new EventBus('ProxyDispatchAutoLinkBus')

  try {
    bus.on(ProxyDispatchRootEvent, (event) => {
      event.emit(ProxyDispatchChildEvent({}))
      return 'root'
    })
    bus.on(ProxyDispatchChildEvent, () => 'child')

    const root = bus.emit(ProxyDispatchRootEvent({}))
    await Promise.all([root.now(), bus.waitUntilIdle()])

    const child = root.event_children[0]
    assert.ok(child)
    assert.equal(child.event_parent_id, root.event_id)
    assert.equal(root.event_children.length, 1)
    assert.equal(root.event_children[0]?.event_id, child.event_id)
  } finally {
    await bus.destroy()
  }
})

test('test_proxy_dispatch_of_same_event_does_not_self_parent_or_self_link_child', async () => {
  const bus = new EventBus('ProxyDispatchSameEventBus')

  try {
    bus.on(ProxyDispatchRootEvent, (event) => {
      event.emit(event)
      return 'root'
    })

    const root = bus.emit(ProxyDispatchRootEvent({}))
    await Promise.all([root.now(), bus.waitUntilIdle()])

    assert.equal(root.event_parent_id, null)
    assert.equal(root.event_children.length, 0)
  } finally {
    await bus.destroy()
  }
})

test('test_events_are_processed_in_fifo_order', async () => {
  const bus = new EventBus('FifoBus')
  const processed_orders: number[] = []
  const handler_start_times: number[] = []

  try {
    bus.on(OrderEvent, async (event) => {
      handler_start_times.push(Date.now())
      if (event.order % 2 === 0) {
        await delay(30)
      } else {
        await delay(5)
      }
      processed_orders.push(event.order)
    })

    for (let order = 0; order < 10; order += 1) {
      bus.emit(OrderEvent({ order }))
    }

    await bus.waitUntilIdle()

    assert.deepEqual(
      processed_orders,
      Array.from({ length: 10 }, (_, i) => i)
    )
    for (let i = 1; i < handler_start_times.length; i += 1) {
      assert.ok(handler_start_times[i] >= handler_start_times[i - 1])
    }
  } finally {
    await bus.destroy()
  }
})
