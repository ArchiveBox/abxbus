import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const TestEvent = BaseEvent.extend('TestEvent', {})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('handler error is captured and does not prevent other handlers from running', async () => {
  const bus = new EventBus('ErrorIsolationBus')
  const results: string[] = []

  const failing_handler = (): string => {
    throw new Error('Expected to fail - testing error handling')
  }

  const working_handler = (): string => {
    results.push('success')
    return 'worked'
  }

  bus.on(TestEvent, failing_handler)
  bus.on(TestEvent, working_handler)

  const event = bus.emit(TestEvent({}))
  await event.done({ raise_if_any: false })

  // Both handlers should have run and produced results
  assert.equal(event.event_results.size, 2)

  const failing_result = Array.from(event.event_results.values()).find((r) => r.handler_name === 'failing_handler')
  assert.ok(failing_result, 'failing_handler result should exist')
  assert.equal(failing_result.status, 'error')
  assert.ok(failing_result.error instanceof Error)
  assert.ok((failing_result.error as Error).message.includes('Expected to fail'), 'error message should contain the thrown message')

  const working_result = Array.from(event.event_results.values()).find((r) => r.handler_name === 'working_handler')
  assert.ok(working_result, 'working_handler result should exist')
  assert.equal(working_result.status, 'completed')
  assert.equal(working_result.result, 'worked')

  // The working handler actually ran
  assert.deepEqual(results, ['success'])
})

test('event.event_errors collects handler errors', async () => {
  const bus = new EventBus('ErrorCollectionBus')

  const handler_a = (): void => {
    throw new Error('error_a')
  }

  const handler_b = (): void => {
    throw new TypeError('error_b')
  }

  const handler_c = (): string => {
    return 'ok'
  }

  bus.on(TestEvent, handler_a)
  bus.on(TestEvent, handler_b)
  bus.on(TestEvent, handler_c)

  const event = bus.emit(TestEvent({}))
  await event.done({ raise_if_any: false })

  // Two errors should be collected
  assert.equal(event.event_errors.length, 2)
  const error_messages = event.event_errors.map((e) => (e as Error).message)
  assert.ok(error_messages.includes('error_a'))
  assert.ok(error_messages.includes('error_b'))
})

test('handler error does not prevent event completion', async () => {
  const bus = new EventBus('ErrorCompletionBus')

  bus.on(TestEvent, () => {
    throw new Error('handler failed')
  })

  const event = bus.emit(TestEvent({}))
  await event.done({ raise_if_any: false })

  // Event should still complete even though handler errored
  assert.equal(event.event_status, 'completed')
  assert.ok(event.event_completed_at, 'event_completed_at should be set')
  assert.equal(event.event_errors.length, 1)
})

test('error in one event does not affect subsequent queued events', async () => {
  const bus = new EventBus('ErrorQueueBus')
  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})

  bus.on(Event1, () => {
    throw new Error('event1 handler failed')
  })

  bus.on(Event2, () => {
    return 'event2 ok'
  })

  const event_1 = bus.emit(Event1({}))
  const event_2 = bus.emit(Event2({}))

  await bus.waitUntilIdle()

  // Event1 completed with error
  assert.equal(event_1.event_status, 'completed')
  assert.equal(event_1.event_errors.length, 1)

  // Event2 completed successfully and was not affected by Event1's error
  assert.equal(event_2.event_status, 'completed')
  assert.equal(event_2.event_errors.length, 0)
  const result = Array.from(event_2.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'event2 ok')
})

test('async handler rejection is captured as error', async () => {
  const bus = new EventBus('AsyncErrorBus')

  const async_failing_handler = async (): Promise<string> => {
    await delay(1)
    throw new Error('async rejection')
  }

  bus.on(TestEvent, async_failing_handler)

  const event = bus.emit(TestEvent({}))
  await event.done({ raise_if_any: false })

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_errors.length, 1)
  assert.ok((event.event_errors[0] as Error).message.includes('async rejection'))

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
})

test('error in forwarded event handler does not block source bus', async () => {
  const bus_a = new EventBus('ErrorForwardA')
  const bus_b = new EventBus('ErrorForwardB')

  const ForwardEvent = BaseEvent.extend('ForwardEvent', {})

  // Forward from A to B
  bus_a.on('*', bus_b.emit)

  // Handler on bus_b throws
  bus_b.on(ForwardEvent, () => {
    throw new Error('bus_b handler failed')
  })

  // Handler on bus_a succeeds
  bus_a.on(ForwardEvent, () => {
    return 'bus_a ok'
  })

  const event = bus_a.emit(ForwardEvent({}))
  await event.done({ raise_if_any: false })

  assert.equal(event.event_status, 'completed')

  // bus_a's handler succeeded
  const bus_a_result = Array.from(event.event_results.values()).find((r) => r.eventbus_id === bus_a.id && r.handler_name !== 'emit')
  assert.ok(bus_a_result)
  assert.equal(bus_a_result.status, 'completed')
  assert.equal(bus_a_result.result, 'bus_a ok')

  // bus_b's handler errored
  const bus_b_result = Array.from(event.event_results.values()).find((r) => r.eventbus_id === bus_b.id && r.handler_name !== 'emit')
  assert.ok(bus_b_result)
  assert.equal(bus_b_result.status, 'error')

  // Both errors tracked
  assert.ok(event.event_errors.length >= 1)
})

test('event with no handlers completes without errors', async () => {
  const bus = new EventBus('NoHandlerBus')
  const OrphanEvent = BaseEvent.extend('OrphanEvent', {})

  const event = bus.emit(OrphanEvent({}))
  await event.done()

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_results.size, 0)
  assert.equal(event.event_errors.length, 0)
})

test('error handler result fields are populated correctly', async () => {
  const bus = new EventBus('ErrorFieldsBus')

  const my_handler = (): void => {
    throw new RangeError('out of range')
  }

  bus.on(TestEvent, my_handler)

  const event = bus.emit(TestEvent({}))
  await event.done({ raise_if_any: false })

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.equal(result.handler_name, 'my_handler')
  assert.equal(result.eventbus_name, 'ErrorFieldsBus')
  assert.ok(result.error instanceof RangeError)
  assert.equal((result.error as RangeError).message, 'out of range')
  assert.ok(result.started_at, 'started_at should be set')
  assert.ok(result.completed_at, 'completed_at should be set even on error')
})
