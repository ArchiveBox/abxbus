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
const ConfiguredEvent = BaseEvent.extend('ConfiguredEvent', {
  event_version: '2.0.0',
  event_timeout: 12,
  event_slow_timeout: 30,
  event_handler_timeout: 3,
  event_handler_slow_timeout: 4,
  event_blocks_parent_completion: true,
})

test('event_concurrency remains unset on dispatch and resolves during processing', async () => {
  const bus = new EventBus('EventConcurrencyDefaultBus', { event_concurrency: 'parallel' })
  try {
    bus.on(PropagationEvent, async () => 'ok')

    const implicit = bus.emit(PropagationEvent({}))
    const explicit_null = bus.emit(PropagationEvent({ event_concurrency: null }))

    assert.equal(implicit.event_concurrency ?? null, null)
    assert.equal(explicit_null.event_concurrency ?? null, null)

    await implicit.now()
    await explicit_null.now()

    assert.equal(implicit.event_concurrency ?? null, null)
    assert.equal(explicit_null.event_concurrency ?? null, null)
  } finally {
    await bus.destroy()
  }
})

test('event_concurrency class override beats bus default', async () => {
  const bus = new EventBus('EventConcurrencyOverrideBus', { event_concurrency: 'parallel' })
  try {
    bus.on(ConcurrencyOverrideEvent, async () => 'ok')

    const event = bus.emit(ConcurrencyOverrideEvent({}))
    assert.equal(event.event_concurrency, 'global-serial')
    await event.now()
  } finally {
    await bus.destroy()
  }
})

test('event instance override beats event class defaults', async () => {
  const bus = new EventBus('EventInstanceOverrideBus')
  try {
    const class_default = bus.emit(ConcurrencyOverrideEvent({}))
    assert.equal(class_default.event_concurrency, 'global-serial')

    const instance_override = bus.emit(ConcurrencyOverrideEvent({ event_concurrency: 'parallel' }))
    assert.equal(instance_override.event_concurrency, 'parallel')
  } finally {
    await bus.destroy()
  }
})

test('handler defaults remain unset on dispatch and resolve during processing', async () => {
  const bus = new EventBus('HandlerDefaultsBus', {
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
  })
  try {
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

    await implicit.now()
    await explicit_null.now()

    assert.equal(implicit.event_handler_concurrency ?? null, null)
    assert.equal(implicit.event_handler_completion ?? null, null)
    assert.equal(explicit_null.event_handler_concurrency ?? null, null)
    assert.equal(explicit_null.event_handler_completion ?? null, null)
  } finally {
    await bus.destroy()
  }
})

test('handler class override beats bus defaults', async () => {
  const bus = new EventBus('HandlerDefaultsOverrideBus', {
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
  })
  try {
    bus.on(HandlerOverrideEvent, async () => 'ok')

    const event = bus.emit(HandlerOverrideEvent({}))
    assert.equal(event.event_handler_concurrency, 'serial')
    assert.equal(event.event_handler_completion, 'all')
    await event.now()
  } finally {
    await bus.destroy()
  }
})

test('handler instance override beats event class defaults', async () => {
  const bus = new EventBus('HandlerInstanceOverrideBus')
  try {
    const class_default = bus.emit(HandlerOverrideEvent({}))
    assert.equal(class_default.event_handler_concurrency, 'serial')
    assert.equal(class_default.event_handler_completion, 'all')

    const instance_override = bus.emit(
      HandlerOverrideEvent({
        event_handler_concurrency: 'parallel',
        event_handler_completion: 'first',
      })
    )
    assert.equal(instance_override.event_handler_concurrency, 'parallel')
    assert.equal(instance_override.event_handler_completion, 'first')
  } finally {
    await bus.destroy()
  }
})

test('typed event config defaults populate base event fields', async () => {
  const bus = new EventBus('ConfiguredEventDefaultsBus')
  try {
    const event = bus.emit(ConfiguredEvent({}))
    assert.equal(event.event_version, '2.0.0')
    assert.equal(event.event_timeout, 12)
    assert.equal(event.event_slow_timeout, 30)
    assert.equal(event.event_handler_timeout, 3)
    assert.equal(event.event_handler_slow_timeout, 4)
    assert.equal(event.event_blocks_parent_completion, true)
  } finally {
    await bus.destroy()
  }
})
