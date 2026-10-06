import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent } from '../src/index.js'

test('BaseEvent.extend accepts full Zod object schemas and exposes model_fields as event_schema.shape', () => {
  const url_schema = z.string().url()
  const retries_schema = z.number().int().min(0).max(2).default(0)
  const payload_schema_field = z.string()
  const event_timeout_schema = z.number().positive().nullable().default(25)
  const FullZodEvent = BaseEvent.extend(
    'BaseEventFullZodSchemaEvent',
    z
      .object({
        url: url_schema,
        retries: retries_schema,
        schema: payload_schema_field,
        event_timeout: event_timeout_schema,
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
  assert.equal(FullZodEvent.model_fields, FullZodEvent.event_schema.shape)
  assert.equal(FullZodEvent.model_fields.url, url_schema)
  assert.equal(FullZodEvent.model_fields.retries, retries_schema)
  assert.equal(FullZodEvent.model_fields.schema, payload_schema_field)
  assert.equal(FullZodEvent.model_fields.event_timeout, event_timeout_schema)
  assert.equal(FullZodEvent.retries, 0)
  assert.equal(FullZodEvent.event_timeout, 25)
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

test('BaseEvent.extend treats compound non-Zod shortcut defaults as fixed literals', () => {
  const ShortcutLiteralEvent = BaseEvent.extend('BaseEventShortcutCompoundLiteralSchemaEvent', {
    exact_object: { mode: 'exact', nested: { count: 2 }, flags: [true, null] },
    exact_tuple: ['ready', { retries: 1 }],
  })

  const default_event = ShortcutLiteralEvent({})
  assert.deepEqual(default_event.exact_object, { mode: 'exact', nested: { count: 2 }, flags: [true, null] })
  assert.deepEqual(default_event.exact_tuple, ['ready', { retries: 1 }])

  const roundtripped_event = ShortcutLiteralEvent({
    exact_object: { mode: 'exact', nested: { count: 2 }, flags: [true, null] },
    exact_tuple: ['ready', { retries: 1 }],
  })
  assert.deepEqual(roundtripped_event.exact_object, default_event.exact_object)
  assert.deepEqual(roundtripped_event.exact_tuple, default_event.exact_tuple)

  assert.throws(
    () =>
      ShortcutLiteralEvent({
        exact_object: { mode: 'exact', nested: { count: 3 }, flags: [true, null] },
        exact_tuple: ['ready', { retries: 1 }],
      }),
    z.ZodError
  )
  assert.throws(
    () =>
      ShortcutLiteralEvent({
        exact_object: { mode: 'exact', nested: { count: 2 }, flags: [true, null], extra: true },
        exact_tuple: ['ready', { retries: 1 }],
      }),
    z.ZodError
  )
  assert.throws(
    () =>
      ShortcutLiteralEvent({
        exact_object: { mode: 'exact', nested: { count: 2 }, flags: [true, null] },
        exact_tuple: ['ready'],
      }),
    z.ZodError
  )
})

test('BaseEvent.extend validates non-Zod shortcut defaults for builtin metadata fields', () => {
  assert.throws(
    () =>
      BaseEvent.extend('BaseEventInvalidShortcutTimeoutEvent', {
        event_timeout: -2,
      }),
    z.ZodError
  )
  assert.throws(
    () =>
      BaseEvent.extend('BaseEventInvalidShortcutTTLProbeEvent', {
        event_ttl: -2,
      }),
    z.ZodError
  )
  assert.throws(
    () =>
      BaseEvent.extend('BaseEventInvalidShortcutResultTTLProbeEvent', {
        event_result_ttl: -2,
      }),
    z.ZodError
  )
  assert.throws(
    () =>
      BaseEvent.extend('BaseEventInvalidShortcutVersionEvent', {
        event_version: 123,
      }),
    z.ZodError
  )
})
