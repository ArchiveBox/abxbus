import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus, retry, clearSemaphoreRegistry } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

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

// ─── Integration with EventBus ───────────────────────────────────────────────

test('retry: works as event bus handler wrapper (inline HOF)', async () => {
  const bus = new EventBus('RetryBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3 })(async (_event) => {
      calls++
      if (calls < 3) throw new Error(`handler fail ${calls}`)
      return 'handler ok'
    })
  )

  const event = bus.emit(TestEvent({}))
  await event.done()

  assert.equal(calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'handler ok')
})

test('retry: bus handler with retry_on_errors only retries matching errors (inline HOF)', async () => {
  const bus = new EventBus('RetryFilterBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async (_event) => {
      calls++
      throw new ValidationError()
    })
  )

  const event = bus.emit(TestEvent({}))
  await assert.rejects(event.done(), ValidationError)

  assert.equal(calls, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof ValidationError)
})

test('retry: @retry() decorated method works with bus.on via bind', async () => {
  const bus = new EventBus('DecoratorBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  class Handler {
    calls = 0

    @retry({ max_attempts: 3 })
    async onTest(_event: InstanceType<typeof TestEvent>): Promise<string> {
      this.calls++
      if (this.calls < 3) throw new Error('handler fail')
      return 'handler ok'
    }
  }

  const handler = new Handler()
  bus.on(TestEvent, handler.onTest.bind(handler))

  const event = bus.emit(TestEvent({}))
  await event.done()
  assert.equal(handler.calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.result, 'handler ok')
})

// ─── @retry() decorator + bus.on via .bind(this) — all three scopes ─────────

test('retry: @retry(scope=class) + bus.on via .bind — serializes across instances', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeClassBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeClassEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'class', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.done()
  assert.equal(max_active, 1, 'class scope should serialize across instances')
})

test('retry: @retry(scope=instance) + bus.on via .bind — isolates per instance', async () => {
  const bus = new EventBus('ScopeInstanceBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeInstanceEvent', {})

  let active = 0
  let max_active = 0
  let total_calls = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: 'on_SomeEvent_inst' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      total_calls++
      await delay(200)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.done()

  assert.equal(total_calls, 2, 'both handlers should have run')
  assert.equal(
    max_active,
    2,
    `instance scope should allow different instances to run in parallel (got max_active=${max_active}, total_calls=${total_calls})`
  )
})

test('retry: @retry(scope=global) + bus.on via .bind — all calls share one semaphore', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeGlobalBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeGlobalEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'global', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.emit(SomeEvent({}))
  await event.done()
  assert.equal(max_active, 1, 'global scope should serialize all calls')
})

// ─── HOF pattern: retry({...})(fn).bind(instance) — alternative to decorator ─

test('retry: HOF retry()(fn).bind(instance) — instance scope works when bind is after wrap', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('HOFBindBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('HOFBindEvent', {})

  let active = 0
  let max_active = 0

  const some_instance_a = { name: 'a' }
  const some_instance_b = { name: 'b' }

  const handler = retry({
    max_attempts: 1,
    semaphore_scope: 'instance',
    semaphore_limit: 1,
    semaphore_name: 'handler',
  })(async function (this: any, _event: InstanceType<typeof SomeEvent>): Promise<string> {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  bus.on(SomeEvent, handler.bind(some_instance_a))
  bus.on(SomeEvent, handler.bind(some_instance_b))

  const event = bus.emit(SomeEvent({}))
  await event.done()
  assert.equal(max_active, 2, 'bind-after-wrap: different instances should run in parallel')
})

// ─── retry wrapping emit→done (TECHNICALLY SUPPORTED, NOT RECOMMENDED) ──────

test('retry: retry wrapping emit→done retries the full dispatch cycle (discouraged pattern)', async () => {
  const bus = new EventBus('RetryEmitBus', { event_timeout: null, event_handler_concurrency: 'parallel' })

  const TabsEvent = BaseEvent.extend('TabsEvent', {})
  const DOMEvent = BaseEvent.extend('DOMEvent', {})
  const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', {})

  let tabs_attempts = 0
  let dom_calls = 0
  let screenshot_calls = 0

  bus.on(TabsEvent, async (_event) => {
    tabs_attempts++
    if (tabs_attempts < 3) throw new Error(`tabs fail attempt ${tabs_attempts}`)
    return 'tabs ok'
  })

  bus.on(DOMEvent, async (_event) => {
    dom_calls++
    return 'dom ok'
  })

  bus.on(ScreenshotEvent, async (_event) => {
    screenshot_calls++
    return 'screenshot ok'
  })

  const [tabs_event, dom_event, screenshot_event] = await Promise.all([
    retry({ max_attempts: 4 })(async () => {
      const event = bus.emit(TabsEvent({}))
      await event.done()
      if (event.event_errors.length) throw event.event_errors[0]
      return event
    })(),
    bus.emit(DOMEvent({})).done(),
    bus.emit(ScreenshotEvent({})).done(),
  ])

  assert.equal(tabs_attempts, 3)
  assert.equal(tabs_event.event_status, 'completed')
  assert.equal(dom_calls, 1)
  assert.equal(screenshot_calls, 1)
  assert.equal(dom_event.event_status, 'completed')
  assert.equal(screenshot_event.event_status, 'completed')
})
