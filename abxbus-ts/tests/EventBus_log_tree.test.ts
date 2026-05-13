import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const RootEvent = BaseEvent.extend('RootEvent', { data: z.string().optional() })
const ChildEvent = BaseEvent.extend('ChildEvent', { value: z.number().optional() })
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', { nested: z.record(z.string(), z.number()).optional() })
const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

class ValueError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValueError'
  }
}

const waitForStartedResult = async (event: BaseEvent, timeout_ms = 1000): Promise<void> => {
  const started_by = Date.now() + timeout_ms
  while (Date.now() < started_by) {
    if (Array.from(event.event_results.values()).some((result) => result.status === 'started')) {
      return
    }
    await delay(5)
  }
  throw new Error(`Timed out waiting for started handler result on ${event.event_type}#${event.event_id.slice(-4)}`)
}

test('logTree: single event', async () => {
  const bus = new EventBus('SingleBus')
  try {
    const event = bus.emit(RootEvent({ data: 'test' }))
    await event.now()
    const output = bus.logTree()
    assert.ok(output.includes('└── ✅ RootEvent#'))
    assert.ok(output.includes('[') && output.includes(']'))
  } finally {
    bus.destroy()
  }
})

test('logTree: with handler results', async () => {
  const bus = new EventBus('HandlerBus')
  try {
    async function test_handler(_event: InstanceType<typeof RootEvent>): Promise<string> {
      return 'status: success'
    }

    bus.on(RootEvent, test_handler)
    const event = bus.emit(RootEvent({ data: 'test' }))
    await event.now()
    const output = bus.logTree()
    assert.ok(output.includes('└── ✅ RootEvent#'))
    assert.ok(output.includes(`${bus.label}.test_handler#`))
    assert.ok(output.includes('"status: success"'))
  } finally {
    bus.destroy()
  }
})

test('logTree: with handler errors', async () => {
  const bus = new EventBus('ErrorBus')
  try {
    async function error_handler(_event: InstanceType<typeof RootEvent>): Promise<string> {
      throw new ValueError('Test error message')
    }

    bus.on(RootEvent, error_handler)
    const event = bus.emit(RootEvent({ data: 'test' }))
    await event.now()
    assert.equal(event.event_errors.length, 1)
    assert.equal((event.event_errors[0] as Error).message, 'Test error message')
    const output = bus.logTree()
    assert.ok(output.includes(`${bus.label}.error_handler#`))
    assert.ok(output.includes('ValueError: Test error message'))
  } finally {
    bus.destroy()
  }
})

test('logTree: first-mode control cancellations use cancelled icon', async () => {
  const bus = new EventBus('CancelledLogBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('CancelledLogEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })
  try {
    async function fast_handler(_event: InstanceType<typeof TestEvent>): Promise<string> {
      await delay(5)
      return 'fast result'
    }

    async function slow_handler(_event: InstanceType<typeof TestEvent>): Promise<string> {
      await delay(100)
      return 'slow result'
    }

    bus.on(TestEvent, fast_handler)
    await delay(2)
    bus.on(TestEvent, slow_handler)

    const event = bus.emit(TestEvent({}))
    await event.now({ first_result: true }).eventResult({ raise_if_any: false })
    const output = bus.logTree()

    assert.ok(output.includes(`🚫 ${bus.label}.slow_handler#`))
    assert.ok(!output.includes(`❌ ${bus.label}.slow_handler#`))
    assert.ok(output.includes("Aborted: event_handler_completion='first' resolved"))
  } finally {
    bus.destroy()
  }
})

test('logTree: complex nested', async () => {
  const bus = new EventBus('ComplexBus')
  try {
    async function root_handler(_event: InstanceType<typeof RootEvent>): Promise<string> {
      const child = bus.emit(ChildEvent({ value: 100 }))
      await child.now()
      return 'Root processed'
    }

    async function child_handler(_event: InstanceType<typeof ChildEvent>): Promise<number[]> {
      const grandchild = bus.emit(GrandchildEvent({}))
      await grandchild.now()
      return [1, 2, 3]
    }

    async function grandchild_handler(_event: InstanceType<typeof GrandchildEvent>): Promise<null> {
      return null
    }

    bus.on(RootEvent, root_handler)
    bus.on(ChildEvent, child_handler)
    bus.on(GrandchildEvent, grandchild_handler)

    const root = bus.emit(RootEvent({ data: 'root_data' }))
    await root.now()
    const output = bus.logTree()
    assert.ok(output.includes('✅ RootEvent#'))
    assert.ok(output.includes(`✅ ${bus.label}.root_handler#`))
    assert.ok(output.includes('✅ ChildEvent#'))
    assert.ok(output.includes(`✅ ${bus.label}.child_handler#`))
    assert.ok(output.includes('✅ GrandchildEvent#'))
    assert.ok(output.includes(`✅ ${bus.label}.grandchild_handler#`))
    assert.ok(output.includes('"Root processed"'))
    assert.ok(output.includes('list(3 items)'))
    assert.ok(output.includes('None'))
  } finally {
    bus.destroy()
  }
})

test('logTree: multiple roots', async () => {
  const bus = new EventBus('MultiBus')
  try {
    const root1 = bus.emit(RootEvent({ data: 'first' }))
    const root2 = bus.emit(RootEvent({ data: 'second' }))
    await Promise.all([root1.now(), root2.now()])
    const output = bus.logTree()
    assert.equal(output.split('├── ✅ RootEvent#').length - 1, 1)
    assert.equal(output.split('└── ✅ RootEvent#').length - 1, 1)
  } finally {
    bus.destroy()
  }
})

test('logTree: timing info', async () => {
  const bus = new EventBus('TimingBus')
  try {
    async function timed_handler(_event: InstanceType<typeof RootEvent>): Promise<string> {
      await delay(5)
      return 'done'
    }

    bus.on(RootEvent, timed_handler)
    const event = bus.emit(RootEvent({}))
    await event.now()
    const output = bus.logTree()
    assert.ok(output.includes('('))
    assert.ok(output.includes('s)'))
  } finally {
    bus.destroy()
  }
})

test('logTree: running handler', async () => {
  const bus = new EventBus('RunningBus')
  let release_handler!: () => void
  const block_handler = new Promise<void>((resolve) => {
    release_handler = resolve
  })
  try {
    async function running_handler(_event: InstanceType<typeof RootEvent>): Promise<string> {
      await block_handler
      return 'done'
    }

    bus.on(RootEvent, running_handler)
    const event = bus.emit(RootEvent({}))
    await waitForStartedResult(event)
    const output = bus.logTree()
    assert.ok(output.includes(`${bus.label}.running_handler#`))
    assert.ok(output.includes('🏃 RootEvent#'))
    release_handler()
    await event.now()
  } finally {
    release_handler()
    bus.destroy()
  }
})
