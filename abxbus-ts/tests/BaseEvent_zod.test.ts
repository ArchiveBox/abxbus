import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent } from '../src/index.js'

test('BaseEvent.extend accepts full Zod object schemas as event_schema metadata only', () => {
  const FullZodEvent = BaseEvent.extend(
    'BaseEventFullZodSchemaEvent',
    z
      .object({
        url: z.string().url(),
        retries: z.number().int().min(0).max(2).default(0),
        schema: z.string(),
        event_timeout: z.number().positive().nullable().default(25),
      })
      .strict()
      .refine((event) => event.retries <= 1)
  )

  const event = FullZodEvent({
    url: 'https://example.com/job/123',
    schema: 'payload-schema',
  })

  assert.equal(event.schema, 'payload-schema')
  assert.equal(event.retries, 0)
  assert.equal(event.event_timeout, 25)
  assert.equal(typeof FullZodEvent.event_schema.safeParse, 'function')
  assert.equal(Reflect.get(FullZodEvent, 'schema'), undefined)
  assert.ok(FullZodEvent.class)
  assert.equal(Reflect.get(FullZodEvent.class, 'schema'), undefined)
  assert.equal(Object.prototype.propertyIsEnumerable.call(event, 'event_schema'), false)

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.schema, 'payload-schema')
  assert.equal(json.event_timeout, 25)
  assert.equal('event_schema' in json, false)

  assert.throws(
    () =>
      FullZodEvent({
        url: 'not-a-url',
        schema: 'payload-schema',
      } as never),
    z.ZodError
  )
  assert.throws(
    () =>
      FullZodEvent({
        url: 'https://example.com/job/123',
        schema: 'payload-schema',
        retries: 2,
      }),
    z.ZodError
  )
})

test('BaseEvent.fromJSON uses registered event_schema for known Zod event types', () => {
  const CodecEvent = BaseEvent.extend(
    'BaseEventKnownZodCodecEvent',
    z
      .object({
        scheduled_at: z.codec(z.string().datetime(), z.date(), {
          decode: (value) => new Date(value),
          encode: (value) => value.toISOString(),
        }),
        slug: z.string().refine((value) => value.startsWith('job_')),
      })
      .strict()
  )
  const scheduled_at = '2026-05-12T12:00:00.000Z'
  const event = CodecEvent({ scheduled_at, slug: 'job_123' })
  assert.ok(event.scheduled_at instanceof Date)

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.scheduled_at, scheduled_at)
  assert.equal('event_schema' in json, false)

  const restored = BaseEvent.fromJSON(json) as typeof event
  assert.equal(restored.event_type, 'BaseEventKnownZodCodecEvent')
  assert.ok(restored.scheduled_at instanceof Date)
  assert.equal(restored.scheduled_at.toISOString(), scheduled_at)

  assert.throws(() => BaseEvent.fromJSON({ ...json, slug: 'bad' }), z.ZodError)
})

test('BaseEvent.extend preserves non-Zod shortcut defaults through generated event_schema', () => {
  const TimeoutEvent = BaseEvent.extend('BaseEventShortcutDefaultSchemaEvent', {
    event_timeout: 25,
  })

  const event = TimeoutEvent({})
  assert.equal(event.event_timeout, 25)
  assert.equal((event.toJSON() as Record<string, unknown>).event_timeout, 25)
})
