import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const PropagationEvent = BaseEvent.extend('PropagationEvent', {})
const ConcurrencyOverrideEvent = BaseEvent.extend('ConcurrencyOverrideEvent', {
  event_concurrency: 'global-serial',
})
const HandlerOverrideEvent = BaseEvent.extend('HandlerOverrideEvent', {
  event_handler_concurrency: 'serial',
  event_handler_completion: 'all',
})

test('event_concurrency remains unset on dispatch and resolves during processing', async () => {
  const bus = new EventBus('EventConcurrencyDefaultBus', { event_concurrency: 'parallel' })
  bus.on(PropagationEvent, async () => 'ok')

  const implicit = bus.emit(PropagationEvent({}))
  const explicit_null = bus.emit(PropagationEvent({ event_concurrency: null }))

  assert.equal(implicit.event_concurrency ?? null, null)
  assert.equal(explicit_null.event_concurrency ?? null, null)

  await implicit.done()
  await explicit_null.done()
})

test('event_concurrency class override beats bus default', async () => {
  const bus = new EventBus('EventConcurrencyOverrideBus', { event_concurrency: 'parallel' })
  bus.on(ConcurrencyOverrideEvent, async () => 'ok')

  const event = bus.emit(ConcurrencyOverrideEvent({}))
  assert.equal(event.event_concurrency, 'global-serial')
  await event.done()
})

test('handler defaults remain unset on dispatch and resolve during processing', async () => {
  const bus = new EventBus('HandlerDefaultsBus', {
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
  })
  bus.on(PropagationEvent, async () => 'ok')

  const implicit = bus.emit(PropagationEvent({}))
  const explicit_null = bus.emit(
    PropagationEvent({
      event_handler_concurrency: null,
      event_handler_completion: null,
    })
  )

  assert.equal(implicit.event_handler_concurrency ?? null, null)
  assert.equal(implicit.event_handler_completion ?? null, null)
  assert.equal(explicit_null.event_handler_concurrency ?? null, null)
  assert.equal(explicit_null.event_handler_completion ?? null, null)

  await implicit.done()
  await explicit_null.done()
})

test('handler class override beats bus defaults', async () => {
  const bus = new EventBus('HandlerDefaultsOverrideBus', {
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
  })
  bus.on(HandlerOverrideEvent, async () => 'ok')

  const event = bus.emit(HandlerOverrideEvent({}))
  assert.equal(event.event_handler_concurrency, 'serial')
  assert.equal(event.event_handler_completion, 'all')
  await event.done()
})
