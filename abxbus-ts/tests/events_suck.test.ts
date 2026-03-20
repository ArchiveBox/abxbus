import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus, events_suck } from '../src/index.js'

test('events_suck.wrap builds imperative methods for emitting events', async () => {
  const bus = new EventBus('EventsSuckBus')
  const CreateEvent = BaseEvent.extend('EventsSuckCreateEvent', {
    name: z.string(),
    age: z.number(),
    nickname: z.string().nullable().optional(),
    event_result_type: z.string(),
  })
  const UpdateEvent = BaseEvent.extend('EventsSuckUpdateEvent', {
    id: z.string(),
    age: z.number().nullable().optional(),
    source: z.string().nullable().optional(),
    event_result_type: z.boolean(),
  })

  bus.on(CreateEvent, async (event) => {
    assert.equal(event.nickname, 'bobby')
    return `user-${event.age}`
  })

  bus.on(UpdateEvent, async (event) => {
    assert.equal(event.source, 'sync')
    return event.age === 46
  })

  const SDKClient = events_suck.wrap('SDKClient', {
    create: CreateEvent,
    update: UpdateEvent,
  })
  const client = new SDKClient(bus)

  const user_id = await client.create({ name: 'bob', age: 45 }, { nickname: 'bobby' })
  const updated = await client.update({ id: user_id ?? 'fallback-id', age: 46 }, { source: 'sync' })

  assert.equal(user_id, 'user-45')
  assert.equal(updated, true)
})

test('events_suck.make_events works with inline handlers', async () => {
  class LegacyService {
    calls: Array<[string, Record<string, unknown>]> = []

    create(id: string | null, name: string, age: number): string {
      this.calls.push(['create', { id, name, age }])
      return `${name}-${age}`
    }

    update(id: string, name?: string | null, age?: number | null, extra?: Record<string, unknown>): boolean {
      this.calls.push(['update', { id, name, age, ...(extra ?? {}) }])
      return true
    }
  }

  const ping_user = (user_id: string): string => `pong:${user_id}`
  const service = new LegacyService()

  const create_from_payload = (payload: { id: string | null; name: string; age: number }): string => {
    return service.create(payload.id, payload.name, payload.age)
  }

  const update_from_payload = (payload: { id: string; name?: string | null; age?: number | null } & Record<string, unknown>): boolean => {
    const { id, name, age, ...extra } = payload
    return service.update(id, name, age, extra)
  }

  const ping_from_payload = (payload: { user_id: string }): string => ping_user(payload.user_id)

  const events = events_suck.make_events({
    FooBarAPICreateEvent: create_from_payload,
    FooBarAPIUpdateEvent: update_from_payload,
    FooBarAPIPingEvent: ping_from_payload,
  })

  const bus = new EventBus('LegacyBus')
  bus.on(events.FooBarAPICreateEvent, (event) => create_from_payload(event))
  bus.on(events.FooBarAPIUpdateEvent, (event) => update_from_payload(event))
  bus.on(events.FooBarAPIPingEvent, (event) => ping_from_payload(event))

  const created = await bus.emit(events.FooBarAPICreateEvent({ id: null, name: 'bob', age: 45 })).first()
  assert.ok(created !== undefined)
  const updated = await bus.emit(events.FooBarAPIUpdateEvent({ id: created, age: 46, source: 'sync' })).first()
  const user_id = 'e692b6cb-ae63-773b-8557-3218f7ce5ced'
  const pong = await bus.emit(events.FooBarAPIPingEvent({ user_id })).first()

  assert.equal(created, 'bob-45')
  assert.equal(updated, true)
  assert.equal(pong, `pong:${user_id}`)
  assert.deepEqual(service.calls[0], ['create', { id: null, name: 'bob', age: 45 }])
  assert.equal(service.calls[1]?.[0], 'update')
  assert.equal(service.calls[1]?.[1].id, 'bob-45')
  assert.equal(service.calls[1]?.[1].age, 46)
  assert.equal(service.calls[1]?.[1].source, 'sync')
})
