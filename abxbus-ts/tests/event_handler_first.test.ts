import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus, retry, clearSemaphoreRegistry } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

// ─── event_handler_completion='first' with parallel handlers ────────────────

test("event_handler_completion='first': returns the first non-null result from parallel handlers", async () => {
  const bus = new EventBus('FirstParallelBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstParallelEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })

  bus.on(TestEvent, async () => {
    await delay(100)
    return 'slow handler'
  })

  await delay(2)

  bus.on(TestEvent, async () => {
    await delay(10)
    return 'fast handler'
  })

  const result = await bus.emit(TestEvent({})).now({ first_result: true }).eventResult({ raise_if_any: false })

  assert.equal(result, 'fast handler')
})

test("event_handler_completion='first': cancels remaining parallel handlers after first result", async () => {
  const bus = new EventBus('FirstCancelBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstCancelEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })
  let slow_handler_completed = false

  bus.on(TestEvent, async () => {
    await delay(10)
    return 'fast result'
  })
  bus.on(TestEvent, async () => {
    await delay(500)
    slow_handler_completed = true
    return 'slow result'
  })

  const event = await bus.emit(TestEvent({})).now({ first_result: true })
  const result = await event.eventResult({ raise_if_any: false })

  assert.equal(result, 'fast result')
  assert.equal(slow_handler_completed, false)
  assert.equal(
    Array.from(event.event_results.values()).some((entry) => entry.status === 'error'),
    true
  )
})

// ─── event_handler_completion='first' with serial handlers ──────────────────

test("event_handler_completion='first': returns the first non-null result from serial handlers", async () => {
  const bus = new EventBus('FirstSerialBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const TestEvent = BaseEvent.extend('FirstSerialEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })
  let second_handler_called = false

  bus.on(TestEvent, async () => 'first handler result')
  bus.on(TestEvent, async () => {
    second_handler_called = true
    return 'second handler result'
  })

  const event = await bus.emit(TestEvent({})).now()

  assert.equal(await event.eventResult({ raise_if_any: false }), 'first handler result')
  assert.equal(second_handler_called, false)
})

test("event_handler_completion='first': serial mode skips null result and uses next handler", async () => {
  const bus = new EventBus('FirstSerialSkipBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const TestEvent = BaseEvent.extend('FirstSerialSkipEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })
  let third_handler_called = false

  bus.on(TestEvent, async () => null)
  bus.on(TestEvent, async () => 'winner')
  bus.on(TestEvent, async () => {
    third_handler_called = true
    return 'third'
  })

  const event = await bus.emit(TestEvent({})).now()

  assert.equal(await event.eventResult({ raise_if_any: false }), 'winner')
  assert.equal(third_handler_called, false)
})

// ─── now()/wait() first_result with all-handler completion ──────────────────

test('now: first_result returns first valid result without changing handler completion mode', async () => {
  const bus = new EventBus('NowFirstResultAllBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('NowFirstResultAllEvent', { event_result_type: z.string() })
  let slow_handler_completed = false

  bus.on(TestEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(TestEvent, async () => {
    await delay(100)
    slow_handler_completed = true
    return 'slow'
  })

  const event = await bus.emit(TestEvent({})).now({ first_result: true })

  assert.equal(event.event_handler_completion, undefined)
  assert.equal(await event.eventResult(), 'fast')
  assert.equal(slow_handler_completed, false)
  assert.notEqual(event.event_status, 'completed')
  await event.wait({ timeout: 1 })
  assert.equal(slow_handler_completed, true)
})

test('wait: first_result returns first valid result without forcing execution', async () => {
  const bus = new EventBus('WaitFirstResultAllBus', { event_timeout: 0, event_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('WaitFirstResultAllEvent', {
    event_concurrency: 'parallel',
    event_result_type: z.string(),
  })
  let slow_handler_completed = false

  bus.on(TestEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(TestEvent, async () => {
    await delay(100)
    slow_handler_completed = true
    return 'slow'
  })

  const event = await bus.emit(TestEvent({})).wait({ first_result: true, timeout: 1 })

  assert.equal(await event.eventResult(), 'fast')
  assert.equal(slow_handler_completed, false)
  await event.wait({ timeout: 1 })
  assert.equal(slow_handler_completed, true)
})

// ─── eventResult() edge cases ───────────────────────────────────────────────

test('eventResult: returns undefined when all handlers return nullish values', async () => {
  const bus = new EventBus('EventResultUndefinedBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('EventResultUndefinedEvent', {})

  bus.on(TestEvent, async () => undefined)
  bus.on(TestEvent, async () => null)

  assert.equal(await bus.emit(TestEvent({})).now().eventResult(), undefined)
})

test('eventResult: raises by default when every handler fails', async () => {
  const bus = new EventBus('EventResultErrorBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('EventResultErrorEvent', { event_result_type: z.string() })

  bus.on(TestEvent, async () => {
    throw new Error('handler 1 error')
  })
  bus.on(TestEvent, async () => {
    throw new Error('handler 2 error')
  })

  await assert.rejects(() => bus.emit(TestEvent({})).now().eventResult(), AggregateError)
})

test('eventResult: can return a valid result when handler errors are suppressed', async () => {
  const bus = new EventBus('EventResultMixBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('EventResultMixEvent', { event_result_type: z.string() })

  bus.on(TestEvent, async () => {
    throw new Error('fast but fails')
  })
  bus.on(TestEvent, async () => {
    await delay(20)
    return 'slow but succeeds'
  })

  const event = await bus.emit(TestEvent({})).now()

  assert.equal(await event.eventResult({ raise_if_any: false }), 'slow but succeeds')
  await assert.rejects(() => event.eventResult({ raise_if_any: true }), AggregateError)
})

test('eventResult: raises with raise_if_none when no real result is found', async () => {
  const bus = new EventBus('EventResultRaiseNoneBus', { event_timeout: 0 })
  const TestEvent = BaseEvent.extend('EventResultRaiseNoneEvent', {})

  bus.on(TestEvent, async () => null)

  await assert.rejects(() => bus.emit(TestEvent({})).now().eventResult({ raise_if_none: true }), /Expected at least one handler/)
})

test('eventResult: include callback receives result and event result', async () => {
  const bus = new EventBus('EventResultIncludeBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const TestEvent = BaseEvent.extend('EventResultIncludeEvent', { event_result_type: z.string() })
  const seen_handler_names: Array<string | null> = []
  const seen_results: Array<string | null | undefined> = []

  async function none_handler() {
    return null
  }
  async function second_handler() {
    return 'second'
  }

  bus.on(TestEvent, none_handler)
  bus.on(TestEvent, second_handler)

  const result = await bus
    .emit(TestEvent({}))
    .now()
    .eventResult({
      raise_if_any: false,
      include: (result, event_result) => {
        assert.equal(result, event_result.result)
        seen_results.push(result)
        seen_handler_names.push(event_result.handler_name)
        return event_result.status === 'completed' && result === 'second'
      },
    })

  assert.equal(result, 'second')
  assert.deepEqual(seen_results, [undefined, 'second'])
  assert.deepEqual(seen_handler_names, ['none_handler', 'second_handler'])
})

test('eventResult: preserves non-null falsy values', async () => {
  const bus = new EventBus('EventResultFalsyBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const NumberEvent = BaseEvent.extend('EventResultFalsyNumberEvent', { event_result_type: z.number() })
  const BooleanEvent = BaseEvent.extend('EventResultFalsyBooleanEvent', { event_result_type: z.boolean() })
  const StringEvent = BaseEvent.extend('EventResultFalsyStringEvent', { event_result_type: z.string() })

  bus.on(NumberEvent, async () => 0)
  bus.on(BooleanEvent, async () => false)
  bus.on(StringEvent, async () => '')

  assert.equal(await bus.emit(NumberEvent({})).now().eventResult(), 0)
  assert.equal(await bus.emit(BooleanEvent({})).now().eventResult(), false)
  assert.equal(await bus.emit(StringEvent({})).now().eventResult(), '')
})

test('eventResult: rejects when event has no bus attached', async () => {
  const TestEvent = BaseEvent.extend('EventResultNoBusEvent', {})
  const event = TestEvent({})

  await assert.rejects(() => event.eventResult(), { message: 'event has no bus attached' })
})

test('now: rejects when event has no bus attached', async () => {
  const TestEvent = BaseEvent.extend('NowNoBusEvent', {})
  const event = TestEvent({})

  await assert.rejects(() => event.now(), { message: 'event has no bus attached' })
})

test('wait/now/eventResult: completed events do not need a bus attached', async () => {
  const TestEvent = BaseEvent.extend('CompletedNoBusEvent', {})

  const waitEvent = TestEvent({})
  waitEvent._markCompleted(false)
  assert.equal(await waitEvent.wait(), waitEvent)

  const nowEvent = TestEvent({})
  nowEvent._markCompleted(false)
  assert.equal(await nowEvent.now(), nowEvent)

  const resultEvent = TestEvent({})
  resultEvent._markCompleted(false)
  assert.equal(await resultEvent.eventResult(), undefined)
})

// ─── event_handler_completion='first' with @retry() decorated handlers ──────

test("event_handler_completion='first': @retry decorated handler retries before result resolves", async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('FirstRetryBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstRetryEvent', {
    event_handler_completion: 'first',
    event_result_type: z.string(),
  })
  let fast_attempts = 0

  class Service {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(TestEvent, this.on_fast.bind(this))
    }

    @retry({ max_attempts: 3 })
    async on_fast(): Promise<string | undefined> {
      fast_attempts++
      if (fast_attempts < 3) throw new Error(`attempt ${fast_attempts} failed`)
      return 'succeeded after retries'
    }
  }

  new Service(bus)

  assert.equal(await bus.emit(TestEvent({})).now({ first_result: true }).eventResult(), 'succeeded after retries')
  assert.equal(fast_attempts, 3)
})
