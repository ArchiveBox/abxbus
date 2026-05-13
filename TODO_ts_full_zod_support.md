# TODO: TS Full Zod Support for `BaseEvent.extend()`

## Goal

Make TypeScript `BaseEvent.extend()` use Zod schemas as the canonical event definition instead of unwrapping Zod fields out of a plain object and rebuilding a partial schema.

The target user shape is:

```ts
const MyEvent = BaseEvent.extend(
  'MyEvent',
  z
    .object({
      url: z.string().url(),
      retries: z.number().int().min(0).max(5).default(0),
    })
    .strict()
    .refine((event) => event.retries <= 3)
)
```

Users should also still be able to use the shorthand:

```ts
const MyEvent = BaseEvent.extend('MyEvent', {
  url: z.string().url(),
  retries: z.number().int().min(0).max(5).default(0),
  event_result_type: z.string(),
})
```

The shorthand is only a convenience. Internally it should be converted into a real Zod object schema and then handled through the same schema path.

## Desired Semantics

1. `BaseEvent.extend(event_type, schema)` accepts a normal Zod object schema.
2. `BaseEvent.extend(event_type, shape)` accepts `{ key: z.whatever() }` as a shortcut.
3. The full event schema is a new schema object. Do not mutate the user's passed schema.
4. Builtin event fields and metadata are added only when missing.
5. The user's schema wins if it defines builtin fields such as `event_type`, `event_timeout`, `event_version`, `event_result_type`, etc.
6. Runtime constructor defaults are still hydrated before schema decode/parse when missing.
7. User construction data wins over runtime defaults, including builtin metadata fields.
8. Zod decides validity. If the user overrides builtin fields in a way that breaks bus behavior, allow the normal hard failure or broken behavior rather than adding special guards.
9. Preserve Zod object intent where possible: `.strict()`, `.passthrough()`, `.catchall()`, `.refine()`, `.superRefine()`, `.default()`, field transforms, field codecs, and field constraints.
10. JSON roundtrips must not carry the full event payload schema. The event class `event_schema` is TS-only metadata.
11. JSON roundtrips should continue carrying `event_result_type` as best-effort portable handler-result schema metadata.
12. JSON does not need to preserve non-portable Zod logic across languages, but known TS rehydration must recover it by using the known event class schema.
13. `event_result_type` keeps its existing special behavior. It is handler result schema metadata, not a normal event payload field, and it should accept any Zod schema/type or `null`.
14. `event_schema` is TS-only metadata and must always point at the generated canonical Zod event schema. Do not add a `.schema` compatibility alias; `schema` must remain available as a normal payload field name.
15. The existing non-Zod shortcut behavior remains exactly as it works today.
16. `toJSON()` should use Zod encode and throw if the user's schema cannot be encoded. That failure is user-authored schema behavior, not something to silently recover from.
17. Python/Go/Rust should not receive or depend on TS event payload schema metadata. They should continue preserving and enforcing `event_result_type` JSON Schema where supported, plus their own runtime-local typed payload mechanisms.

## Current Code To Replace

Relevant TS implementation points:

- `BaseEventSchema` is currently a loose base object at `abxbus-ts/src/BaseEvent.ts:55`.
- `EventSchema`, `EventInit`, `EventFactory`, and payload inference are shape-based at `abxbus-ts/src/BaseEvent.ts:111`.
- The constructor merges side-channel defaults, normalizes result schema, and calls `schema.parse(base_data)` at `abxbus-ts/src/BaseEvent.ts:212`.
- `BaseEvent.extend()` currently accepts a record, checks reserved/event-prefixed/model-prefixed fields, extracts Zod-looking fields, and rebuilds with `BaseEventSchema.extend(zod_shape)` at `abxbus-ts/src/BaseEvent.ts:290`.
- `EVENT_CLASS_DEFAULTS` stores non-Zod values outside the schema at `abxbus-ts/src/BaseEvent.ts:156` and `abxbus-ts/src/BaseEvent.ts:342`.
- `BaseEvent.fromJSON()` reconstructs `event_result_type` from JSON Schema but does not route unknown base calls through a registered known event type at `abxbus-ts/src/BaseEvent.ts:347`.
- `BaseEvent.toJSON()` manually materializes fields and serializes `event_result_type` via `toJsonSchema()` at `abxbus-ts/src/BaseEvent.ts:374`.
- `extractZodShape()` is the current duck-typed unwrapping helper at `abxbus-ts/src/types.ts:101`.
- `toJsonSchema()` / `fromJsonSchema()` already provide a best-effort JSON Schema bridge at `abxbus-ts/src/types.ts:110`.
- `EventResult.update()` currently validates handler results with `safeParse()` at `abxbus-ts/src/EventResult.ts:263`.
- `EventBus.fromJSON()` currently rehydrates history through `BaseEvent.fromJSON()` at `abxbus-ts/src/EventBus.ts:560`.

## Proposed Internal Shape

### Schema Types

Move the public event factory types away from `TShape extends z.ZodRawShape` and toward schema input/output types.

Sketch:

```ts
type AnyEventSchema = z.ZodTypeAny

type EventPayloadFromSchema<TSchema extends AnyEventSchema> =
  z.output<TSchema> extends Record<string, unknown>
    ? z.output<TSchema>
    : never

type EventInputFromSchema<TSchema extends AnyEventSchema> =
  z.input<TSchema> extends Record<string, unknown>
    ? z.input<TSchema>
    : never

type EventFactory<TSchema extends AnyEventSchema, TResult = unknown> = {
  (data: EventInputFromSchema<TSchema>): BaseEvent & EventPayloadFromSchema<TSchema> & { __event_result_type__?: TResult }
  new (data: EventInputFromSchema<TSchema>): BaseEvent & EventPayloadFromSchema<TSchema> & { __event_result_type__?: TResult }
  event_schema: TSchema
  class?: new (data: EventInputFromSchema<TSchema>) => BaseEvent & EventPayloadFromSchema<TSchema>
  event_type?: string
  event_version?: string
  event_result_type?: z.ZodTypeAny
  fromJSON?: (data: unknown) => BaseEvent & EventPayloadFromSchema<TSchema> & { __event_result_type__?: TResult }
}
```

This will need refinement to keep builtin fields optional at construction when BaseEvent hydrates them. A helper like `EventInitFromSchema<TSchema>` may still need to make known builtin fields partial for ergonomic construction, but it should be derived from `z.input<TSchema>`, not from a raw shape.

### Internal Event Schema Metadata

`event_schema` is a TS-only internal metadata property. It should be populated from the generated full Zod schema. The field is covered by the existing `event_*` guard; do not add a second bespoke guard for it.

Implementation rules:

- `BaseEvent.event_schema` and `EventFactory.event_schema` should point at the generated canonical full schema for the event type.
- Do not expose generated metadata through `.schema`. `schema` is not reserved and can be used as an ordinary payload field.
- Instances may expose `event_schema` for local introspection, but it must be non-enumerable or explicitly excluded from materialized wire objects.
- `event_schema` must not appear in `baseEventDefaultShape()`, encoded event data, `toJSON()` output, `BaseEventJSON`, or cross-runtime payloads.
- Do not serialize or synthesize `event_schema` into payloads. User-supplied `event_schema` remains undefined behavior beyond the existing `event_*` guard.

### Base Event Extension Shape

Define a reusable base extension shape that can be added to user object schemas when fields are missing.

Sketch:

```ts
function baseEventDefaultShape(event_type: string): z.ZodRawShape {
  return {
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_type: z.literal(event_type).default(event_type),
    event_version: z.string().default('0.0.1'),
    event_timeout: z.number().positive().nullable().default(null),
    event_slow_timeout: z.number().positive().nullable().optional(),
    event_handler_timeout: z.number().positive().nullable().optional(),
    event_handler_slow_timeout: z.number().positive().nullable().optional(),
    event_blocks_parent_completion: z.boolean().default(false),
    event_parent_id: z.string().uuid().nullable().optional(),
    event_path: z.array(z.string()).optional(),
    event_result_type: z.unknown().optional(),
    event_emitted_by_handler_id: z.string().uuid().nullable().optional(),
    event_pending_bus_count: z.number().nonnegative().optional(),
    event_status: z.enum(['pending', 'started', 'completed']).optional(),
    event_started_at: z.string().datetime().nullable().optional(),
    event_completed_at: z.string().datetime().nullable().optional(),
    event_results: z.record(z.string(), z.unknown()).optional(),
    event_concurrency: z.enum(EVENT_CONCURRENCY_MODES).nullable().optional(),
    event_handler_concurrency: z.enum(EVENT_HANDLER_CONCURRENCY_MODES).nullable().optional(),
    event_handler_completion: z.enum(EVENT_HANDLER_COMPLETION_MODES).nullable().optional(),
  }
}
```

Important: when extending a user object schema, filter this shape to only keys that the user did not define.

```ts
function missingBaseFields(event_type: string, user_shape: z.ZodRawShape): z.ZodRawShape {
  return Object.fromEntries(
    Object.entries(baseEventDefaultShape(event_type)).filter(([key]) => !(key in user_shape))
  ) as z.ZodRawShape
}
```

### Build Full Event Schema

Add one normalization function for all `extend()` inputs.

Sketch:

```ts
function buildFullEventSchema(event_type: string, spec: unknown): {
  event_schema: z.ZodTypeAny
  event_result_type?: z.ZodTypeAny
} {
  if (isZodObject(spec)) {
    const event_schema = spec.safeExtend(missingBaseFields(event_type, spec.shape))
    return {
      event_schema,
      event_result_type: extractEventResultTypeFromObjectSchema(event_schema),
    }
  }

  if (isRawZodShape(spec)) {
    const user_shape = normalizeShortcutShape(spec)
    const user_schema = z.object(user_shape)
    const event_schema = user_schema.safeExtend(missingBaseFields(event_type, user_shape))
    return {
      event_schema,
      event_result_type: extractEventResultTypeFromShortcut(spec),
    }
  }

  if (spec === undefined) {
    const event_schema = z.object({}).safeExtend(baseEventDefaultShape(event_type))
    return { event_schema }
  }

  throw new Error('BaseEvent.extend() requires a Zod object schema or a raw Zod field shape')
}
```

This should replace the current `extractZodShape()` + `EVENT_CLASS_DEFAULTS` path.

### Zod Object Detection

Do not rely only on `value instanceof z.ZodType` because package duplication can break `instanceof`. The current `isZodSchema()` uses `safeParse` duck typing at `abxbus-ts/src/types.ts:80`. That is probably still useful.

Need a robust `isZodObject()` for Zod 4. Options:

- Prefer public API/shape checks if available.
- Fall back to checking `isZodSchema(value)`, `typeof value.safeExtend === 'function'`, and object-like `shape`.
- Avoid reaching into `_def` unless no public alternative is reliable.

### Raw Shape Detection

`BaseEvent.extend('X', { key: z.string() })` should remain supported.

For the shortcut path:

- Treat properties whose values are Zod schemas as event fields.
- Preserve the special old `event_result_type: z.string()` shorthand for handler result validation.
- Preserve constructor shorthand values (`String`, `Number`, etc.) for `event_result_type`.
- Preserve arbitrary non-Zod shortcut defaults exactly as they work today.

Existing users may rely on `BaseEvent.extend('X', { event_timeout: 25 })`. Keep that behavior. The new preferred Zod-native spelling can still be `event_timeout: z.number().positive().nullable().default(25)`, but it should not be a forced migration.

## Constructor Changes

The constructor should hydrate runtime defaults, then decode/parse through the exact class schema.

Current constructor path is at `abxbus-ts/src/BaseEvent.ts:212`.

Target behavior:

```ts
const input = {
  event_id: uuidv7(),
  event_created_at: monotonicDatetime(),
  event_type: ctor.event_type ?? ctor.name,
  event_version: ctor.event_version ?? '0.0.1',
  event_timeout: null,
  event_blocks_parent_completion: false,
  ...data,
}

const decoded = decodeEventSchema(ctor.event_schema, input)
assertFlatEventObject(decoded)
Object.assign(this, decoded)
```

Key rules:

- Hydrated defaults come first.
- User data comes last.
- Construction-time overrides for builtin metadata fields must be accepted and validated by the final schema.
- Example: `SerializableAdvancedEvent({ event_handler_concurrency: 'parallel', ... })` should succeed or fail according to the event schema's `event_handler_concurrency` field.
- User schema field defaults/transforms/refinements decide final values.
- If decoded output is not a plain object, throw a normal validation/runtime error.
- If decoded output omits fields the bus needs, let downstream code fail normally unless the default field schema should have caught it.
- Continue normalizing `event_path`, `event_results`, status fields, timestamps, parent ids, and emitted handler ids after decode.

### Decode vs Parse

To support Zod codecs, use `z.decode(schema, input)` when constructing and rehydrating known TS events.

If the user's schema fails during decode, fail normally. This includes invalid input, custom decode failures, failed refinements, and codec output that does not match the schema's decoded-side type.

Open question: async schemas/codecs. The current constructor is sync. If async Zod features are in scope, they require a new async factory path. Otherwise document that event construction supports sync Zod schemas only.

## `fromJSON()` and Known-Type Rehydration

Known TS event rehydration must use the known class schema. The full event schema is TS-only and is not serialized into event JSON.

Add an event type registry populated by `extend()`:

```ts
const EVENT_TYPE_REGISTRY = new Map<string, typeof BaseEvent>()
```

`BaseEvent.extend()` registers:

```ts
EVENT_TYPE_REGISTRY.set(event_type, ExtendedEvent)
```

`BaseEvent.fromJSON()` should do:

```ts
if (this === BaseEvent && isRecord(data)) {
  const event_type = data.event_type
  if (typeof event_type === 'string') {
    const KnownEvent = EVENT_TYPE_REGISTRY.get(event_type)
    if (KnownEvent) return KnownEvent.fromJSON(data)
  }
}
return new this(normalizeWireSchemas(data))
```

This matters for event history rehydration in `EventBus.fromJSON()` at `abxbus-ts/src/EventBus.ts:560`, since it currently calls `BaseEvent.fromJSON()`.

Wire JSON schema behavior:

- Wire JSON must not include generated event payload schema metadata.
- Unknown event JSON should use `BaseEvent` and reconstruct `event_result_type` from the portable schema if present.
- Known event JSON should prefer the known class's canonical schema and canonical result schema.
- If JSON includes a portable `event_result_type`, known event rehydration can ignore it when the class already has a canonical result schema, or use it as input only when the known schema allows/overrides it.

## `toJSON()` and Wire Projection

Current `toJSON()` manually enumerates instance fields and projects `event_result_type` via `toJsonSchema()` at `abxbus-ts/src/BaseEvent.ts:374`.

Target additions:

1. Use `z.encode(ctor.event_schema, materializeEventObject(this))` to honor codecs and reversible schema behavior.
2. Throw if schema encoding fails.
3. Keep the full event schema TS-only. Do not add generated `event_schema` metadata to JSON output.
4. `event_result_type` remains best-effort `z.toJSONSchema(this.event_result_type)` because handler-result validation metadata is intentionally portable.

Sketch:

```ts
const encoded = tryEncodeEventSchema(ctor.event_schema, materializeEventObject(this))

return {
  ...encoded,
  event_result_type: this.event_result_type ? toJsonSchemaBestEffort(this.event_result_type) : this.event_result_type,
  event_results: serializedEventResultsIfAny,
}
```

Complexities:

- `z.encode()` can fail for one-way transforms. Throw.
- `z.encode()` can fail when a codec's encode function throws. Throw.
- `z.encode()` can fail when a codec's encode function returns a value that does not satisfy the encoded-side schema. Throw.
- `z.encode()` can fail if the runtime event was mutated into a value that no longer satisfies the schema. Throw.
- `event_results` is currently stored as `Map` in memory and serialized manually. Encoding must not accidentally expose the raw Map.
- Runtime-only fields (`bus`, `event_bus`, `event_schema`, `_event_*`, functions) must still be excluded.
- Do not synthesize `event_schema` in `toJSON()`. `event_schema` is local generated Zod metadata and must never be serialized.

Specific encode failure cases to test:

```ts
z.object({
  value: z.string().transform((value) => value.length),
})
```

This decodes but cannot encode because Zod treats `.transform()` as unidirectional.

```ts
z.object({
  value: z.codec(z.string(), z.number(), {
    decode: Number,
    encode: () => {
      throw new Error('cannot encode')
    },
  }),
})
```

This fails because the user-provided codec encode function throws.

```ts
z.object({
  value: z.codec(z.string(), z.number(), {
    decode: Number,
    encode: () => 123,
  }),
})
```

This fails because the codec returns a number where the encoded-side schema requires a string.

## `event_result_type`

Current behavior:

```ts
BaseEvent.extend('TypedResultEvent', {
  event_result_type: z.string(),
})
```

means "handler result schema is string", not "the event payload has a field named `event_result_type` whose value is a string".

The new shape must preserve this existing shorthand behavior.

Required behavior:

- Runtime `event.event_result_type` remains a Zod schema used by `EventResult.update()`.
- JSON `event_result_type` remains a portable JSON Schema projection.
- Known TS rehydration should restore the canonical Zod result schema from the known event class when available.
- Unknown rehydration should reconstruct a Zod schema from JSON Schema via `fromJsonSchema()`.
- In both shorthand and full Zod object inputs, `event_result_type: z.string()` means handler result schema metadata.
- It should accept any Zod object/schema/type or `null`.
- The full-schema path needs to inspect the original object schema shape and treat `event_result_type` specially while still preserving the rest of the user's object schema behavior.

This is intentionally special-cased even though it conflicts with pure "every field is a normal data field" Zod semantics.

## Validation Guardrails

Existing code rejects unknown `event_*` and `model_*` fields in both shapes and construction data at `abxbus-ts/src/BaseEvent.ts:300` and `abxbus-ts/tests/base_event.test.ts:450`.

New desired behavior from the current design discussion:

- Users can override builtin event and metadata fields.
- If they break the bus, let it hard fail normally.
- Do not add special guards beyond the basic Zod constraints on default fields.
- Keep method/runtime collisions reserved: `bus`, `done`, `emit`, `first`, `now`, `toJSON`, `fromJSON`, etc.
- Keep `event_schema` as TS-only generated metadata. Rely on the existing `event_*` guard instead of adding bespoke `event_schema` rejection logic.
- Keep `schema` unreserved so users can use it as a normal payload field.

Test updates will need to delete or rewrite tests expecting unknown `event_*`/`model_*` rejection for schema fields and possibly construction data.

## Docs Updates

Add the following full example to `docs/features/typed-events.mdx` in the TypeScript snippet tab. The goal is to show which advanced Zod features affect TS construction/known rehydration, which can be encoded to wire values, and which break JSON serialization because Zod cannot encode them.

Do not document the full event schema as portable wire metadata. Other runtimes define/enforce their own event payload schemas locally; only `event_result_type` remains portable handler-result schema metadata.

```ts
import { z } from "zod"
import { BaseEvent } from "abxbus"

const AdvancedEvent = BaseEvent.extend(
  "AdvancedEvent",
  z
    .object({
      // TS validation: required string field with URL format.
      url: z.string().url(),

      // TS validation/defaulting: integer + min/max constraints.
      retries: z.number().int().min(0).max(5).default(0),

      // TS validation: strict nested object shape.
      metadata: z
        .object({
          source: z.enum(["api", "worker"]),
          priority: z.number().int().min(1).max(10),
        })
        .strict(),

      // TS-local on known rehydrate: custom refine function is not in wire JSON.
      slug: z.string().refine((value) => value.startsWith("job_")),

      // Works for TS JSON serialization: codec can decode from JSON string to Date
      // and encode Date back to JSON string.
      scheduled_at: z.codec(z.string().datetime(), z.date(), {
        decode: (value) => new Date(value),
        encode: (value) => value.toISOString(),
      }),

      // Breaks toJSON(): transform is one-way in Zod.
      // Construction/fromJSON can decode string -> number, but z.encode() cannot
      // recover the original string representation.
      derived_length: z.string().transform((value) => value.length),

      // Breaks toJSON(): codec encode throws.
      // Construction/fromJSON can decode JSON string -> number, but serialization
      // fails because the user-provided encode function fails.
      broken_codec: z.codec(z.string(), z.number(), {
        decode: Number,
        encode: () => {
          throw new Error("cannot serialize broken_codec")
        },
      }),

      // Breaks toJSON(): codec encode returns the wrong encoded-side type.
      // The encoded-side schema is z.string(), but encode returns a number.
      invalid_codec_output: z.codec(z.string(), z.number(), {
        decode: Number,
        encode: (value) => value,
      }),

      // Existing special behavior: this is handler result schema metadata.
      // Portable subset is projected into JSON Schema in event_result_type.
      event_result_type: z.object({
        ok: z.boolean(),
        count: z.number().int().min(0),
      }),
    })
    .strict()
    // TS-local on known rehydrate: this cross-field predicate is not portable.
    .refine((event) => event.retries <= event.metadata.priority)
)

const event = AdvancedEvent({
  url: "https://example.com/job/123",
  metadata: { source: "api", priority: 3 },
  slug: "job_123",
  scheduled_at: "2026-05-12T12:00:00.000Z",
  derived_length: "hello",
  broken_codec: "123",
  invalid_codec_output: "456",
})

// Works at construction time:
// - scheduled_at is now a Date
// - derived_length is now a number
// - broken_codec is now a number
// - invalid_codec_output is now a number
// - strict/refine/defaults all applied

event.toJSON()
// Throws before producing JSON because:
// - derived_length uses a one-way transform
// - broken_codec encode throws
// - invalid_codec_output encode returns number instead of string
```

Also include a serializable version immediately after the failing example:

```ts
const SerializableAdvancedEvent = BaseEvent.extend(
  "SerializableAdvancedEvent",
  z
    .object({
      // Validated by TS before serialization.
      url: z.string().url(),

      // Defaulted/validated by TS before serialization.
      retries: z.number().int().min(0).max(5).default(0),

      // Strict nested object validation is TS-local.
      metadata: z
        .object({
          source: z.enum(["api", "worker"]),
          priority: z.number().int().min(1).max(10),
        })
        .strict(),

      // Does not survive as wire metadata, but known TS rehydrate
      // gets it back because SerializableAdvancedEvent.event_schema is reused.
      slug: z.string().refine((value) => value.startsWith("job_")),

      // Serializes cleanly: JSON string <-> runtime Date.
      scheduled_at: z.codec(z.string().datetime(), z.date(), {
        decode: (value) => new Date(value),
        encode: (value) => value.toISOString(),
      }),

      event_result_type: z.object({
        ok: z.boolean(),
        count: z.number().int().min(0),
      }),
    })
    .strict()
    // TS-only predicate, restored for known TS event types.
    .refine((event) => event.retries <= event.metadata.priority)
)

const event = SerializableAdvancedEvent({
  url: "https://example.com/job/123",
  metadata: { source: "api", priority: 3 },
  slug: "job_123",
  scheduled_at: "2026-05-12T12:00:00.000Z",
})

const json = event.toJSON()
// Works:
// - scheduled_at encodes back to an ISO string.
// - no generated event payload schema is included in JSON.
// - event_result_type contains best-effort JSON Schema for handler results.
// - custom refine functions are not encoded into JSON.

const restored = SerializableAdvancedEvent.fromJSON(json)
// Known TS rehydrate:
// - scheduled_at decodes back to Date.
// - slug refine is enforced again.
// - object-level refine is enforced again.
// - full Zod behavior comes from SerializableAdvancedEvent.event_schema, not from JSON.

const unknown = BaseEvent.fromJSON(json)
// Unknown/cross-language style rehydrate:
// - can use event_result_type portable JSON Schema subset for handler results.
// - cannot recover payload schema, custom refine functions, or codec functions
//   unless the event type is registered/known in that runtime.
```

## Test Plan

### Existing Tests To Update

- `abxbus-ts/tests/BaseEvent.test.ts`
  - Runtime field serialization/fromJSON coverage at lines 480-508 should assert generated `event_schema` metadata is not present.
  - Reserved `event_*` and `model_*` rejection tests at lines 450-478 need to change for the new override policy.
  - Add direct construction tests for schema defaults and user override of builtin fields.

- `abxbus-ts/tests/BaseEvent_zod.test.ts`
  - Keep Zod-specific BaseEvent coverage here: full Zod object input, strict/refine/default behavior, codecs, known-type rehydration through `event_schema`, and payload field name `schema`.

- `abxbus-ts/tests/EventResult_typed_results.test.ts`
  - Existing result schema setup at lines 13-52 uses the raw shape shorthand.
  - Existing fromJSON/result schema tests at lines 177-253 should be expanded so known event rehydration prefers canonical result schema over lossy JSON Schema.

- `abxbus-ts/tests/cross_runtime_roundtrip.test.ts`
  - Existing schema roundtrip checks rely on `event_result_type`.
  - Assert generated `event_schema` metadata is not emitted in cross-runtime payloads.
  - Do not require TS payload schema, custom Zod logic, or codecs to survive through Python/Go/Rust.

- `abxbus-ts/src/type_inference.test.ts`
  - Update factory inference from raw-shape generics to schema input/output generics.
  - Add inference tests for `BaseEvent.extend('X', z.object(...))`.
  - Add inference tests for field transforms/codecs if the installed Zod types expose input/output precisely.

### New Tests To Add

1. **Zod object input works**
   - `BaseEvent.extend('X', z.object({ url: z.string().url() }))`
   - Construction succeeds for valid URL.
   - Construction fails for invalid URL.

2. **Strict is preserved**
   - User schema `.strict()`.
   - BaseEvent adds missing builtin fields.
   - Unknown extra payload field fails.

3. **Refine/superRefine is preserved**
   - User schema `.refine((event) => event.retries <= 3)`.
   - Invalid refined value fails in constructor/fromJSON.

4. **User builtin field override wins**
   - User schema defines `event_timeout` with a different default/constraint.
   - Constructing without `event_timeout` uses user default.
   - Constructing with invalid `event_timeout` fails.

5. **User construction data wins**
   - Runtime default provides `event_timeout: null`.
   - User construction data overrides it.
   - Builtin metadata fields can be overridden at construction time.
   - Example: `SerializableAdvancedEvent({ event_handler_concurrency: 'parallel', ... })` validates through the final schema.
   - Invalid metadata override values fail through Zod.

6. **Known rehydration restores custom Zod logic**
   - Register event with custom refine or codec.
   - Serialize to JSON.
   - Rehydrate through `BaseEvent.fromJSON(json)`.
   - Assert returned instance is the known event class/factory type and still enforces/refines/decodes through canonical schema.

7. **Unknown rehydration uses portable result schemas only**
   - Create JSON with `event_result_type`.
   - Rehydrate without registered type.
   - Assert portable result schema is reconstructed and usable for handler result validation.

8. **Full event schema stays TS-only**
   - `toJSON()` does not include generated `event_schema`.
   - `BaseEvent.fromJSON(json)` cannot enforce payload constraints for unknown event types.
   - Known TS rehydrate still gets the original custom logic via registry/class schema.
   - Local `event_schema` metadata points at the generated canonical Zod event schema.
   - There is no `.schema` metadata alias; `schema` is allowed as a normal payload field.

9. **Non-portable Zod logic is not required in JSON**
   - Custom refine/codec does not need to survive in wire JSON.
   - Known TS rehydrate still gets the original custom logic via registry/class schema.

10. **Codec behavior**
    - Payload field codec decodes JSON/input into runtime value.
    - `toJSON()` encodes runtime value back when Zod can encode it.
    - One-way transform cannot encode and should make `toJSON()` throw.
    - Codec encode function throwing should make `toJSON()` throw.
    - Codec encode returning the wrong encoded-side type should make `toJSON()` throw.

11. **Result schema still works**
    - Raw shape shorthand `event_result_type: z.string()` still validates handler results.
    - Full object schema path with `event_result_type: z.string()` keeps the same handler result schema behavior.
    - Known rehydration prefers canonical result schema.

12. **EventBus.fromJSON known type path**
    - Event history serialized by an `EventBus`.
    - Rehydrate the bus.
    - Confirm event history entries use registered known event classes when available.

13. **Cross-language schema subset**
    - Python/Go/Rust should not receive TS event payload schema metadata.
    - Python/Go/Rust should continue preserving and enforcing the `event_result_type` JSON Schema subset they already support.
    - Add/keep cases for result-schema required keys, min/max numeric constraints, integer constraints, enum/literal constraints, and object additional-properties behavior where supported.

## Migration / Compatibility Notes

Potential breaking areas:

1. Non-Zod side-channel defaults in `extend()` may stop working if removed.
   - Example: `BaseEvent.extend('X', { event_timeout: 25 })`.
   - Preferred replacement: `event_timeout: z.number().positive().nullable().default(25)`.
   - Current decision: keep existing non-Zod shortcut behavior exactly as it works today.

2. Unknown `event_*` / `model_*` rejection behavior will loosen.
   - This is intentional under the new "user schema wins" rule.

3. Type inference will change from shape-based to schema input/output-based.
   - Should be better for transforms/codecs, but type tests need to catch regressions.

4. Known-type registry can change `BaseEvent.fromJSON(json)` return type/behavior for event types already registered.
   - This is desired for canonical schema restoration.
   - Need to ensure no infinite recursion when `BaseEvent.fromJSON()` delegates to `KnownEvent.fromJSON()`.

5. Full event schemas are TS-local.
   - This avoids adding a new wire field and avoids cross-language metadata churn.
   - Unknown event rehydration cannot recover TS payload schema constraints without a registered known event type.
   - `event_schema` is local TS metadata and is omitted from wire data.
   - `.schema` is intentionally not used for metadata, so payloads can contain a normal `schema` field.

## Implementation Order

1. Add schema normalization helpers in TS.
2. Keep full event schema metadata TS-only; add generated `event_schema` as local class/factory/instance metadata without adding it to the wire/base field list.
3. Refactor `EventFactory` and related type helpers to schema input/output generics.
4. Refactor `BaseEvent.extend()` to call `buildFullEventSchema()`.
5. Remove or deprecate `extractZodShape()` and `EVENT_CLASS_DEFAULTS` usage.
6. Refactor constructor to hydrate defaults and decode/parse through the exact class schema.
7. Add event type registry and known-type `fromJSON()` routing.
8. Refactor `toJSON()` to schema encode without emitting generated `event_schema`, throwing when encode fails.
9. Update `EventResult.update()` if result schema validation should use `safeParse`, `decode`, or another Zod 4 API for codecs.
10. Update TS tests and type inference tests.
11. Run the TS test suite with `pnpm`.
12. Run cross-runtime roundtrip tests to confirm no generated `event_schema` leaks onto the wire.

## Open Questions To Resolve Before Coding

1. Are async Zod transforms/codecs explicitly out of scope because event construction is sync?
2. How should duplicate `event_type` registrations behave: overwrite, throw, or last registration wins?
3. Should `EventResult.update()` keep result validation as `safeParse()` or use a Zod 4 decode API for `event_result_type` codecs?
