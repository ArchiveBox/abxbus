import { z } from 'zod'
import { v7 as uuidv7 } from 'uuid'

import { EventBus } from './EventBus.js'
import { EventResult } from './EventResult.js'
import { EventHandler, EventHandlerAbortedError, EventHandlerCancelledError, EventHandlerTimeoutError } from './EventHandler.js'
import type { EventConcurrencyMode, EventHandlerConcurrencyMode, EventHandlerCompletionMode, Deferred } from './LockManager.js'
import {
  AsyncLock,
  EVENT_CONCURRENCY_MODES,
  EVENT_HANDLER_CONCURRENCY_MODES,
  EVENT_HANDLER_COMPLETION_MODES,
  withResolvers,
} from './LockManager.js'
import { _runWithTimeout } from './timing.js'
import { toJsonSchema, type JsonSchema } from './jsonschema.js'
import { isZodSchema, normalizeEventResultType } from './types.js'
import type { EventHandlerCallable, EventResultType } from './types.js'
import { monotonicDatetime } from './helpers.js'

const RESERVED_USER_EVENT_FIELDS = new Set([
  'bus',
  'emit',
  'wait',
  'now',
  'eventResult',
  'eventResultsList',
  'toString',
  'toJSON',
  'fromJSON',
])

const EVENT_TYPE_REGISTRY = new Map<string, typeof BaseEvent>()

function assertNoReservedUserEventFields(data: Record<string, unknown>, context: string): void {
  for (const field_name of RESERVED_USER_EVENT_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(data, field_name)) {
      throw new Error(`${context} field "${field_name}" is reserved for EventBus runtime context and cannot be set in event payload`)
    }
  }
}

function assertNoUnknownEventPrefixedFields(data: Record<string, unknown>, context: string): void {
  for (const field_name of Object.keys(data)) {
    if (field_name === 'event_extra_payload') {
      throw new Error(`${context} JSON must be flat; event_extra_payload is runtime-only and must not appear on the wire`)
    }
    if (field_name.startsWith('event_') && !KNOWN_BASE_EVENT_FIELDS.has(field_name)) {
      throw new Error(`${context} field "${field_name}" starts with "event_" but is not a recognized BaseEvent field`)
    }
  }
}

function assertNoModelPrefixedFields(data: Record<string, unknown>, context: string): void {
  for (const field_name of Object.keys(data)) {
    if (field_name.startsWith('model_')) {
      throw new Error(`${context} field "${field_name}" starts with "model_" and is reserved for model internals`)
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
}

function isZodObjectSchema(value: unknown): value is z.ZodObject<z.ZodRawShape> {
  return (
    isZodSchema(value) &&
    typeof (value as { safeExtend?: unknown }).safeExtend === 'function' &&
    isRecord((value as { shape?: unknown }).shape)
  )
}

function compareIsoDatetime(left: string | null | undefined, right: string | null | undefined): number {
  const left_value = left ?? ''
  const right_value = right ?? ''
  if (left_value === right_value) {
    return 0
  }
  return left_value < right_value ? -1 : 1
}

export const BaseEventSchema = z
  .object({
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_type: z.string(),
    event_version: z.string().default('0.0.1'),
    event_timeout: z.number().gte(-1).nullable(),
    event_slow_timeout: z.number().gte(-1).nullable().optional(),
    event_handler_timeout: z.number().gte(-1).nullable().optional(),
    event_handler_slow_timeout: z.number().gte(-1).nullable().optional(),
    event_ttl: z.number().gte(-1).nullable().optional(),
    event_result_ttl: z.number().gte(-1).nullable().optional(),
    event_blocks_parent_completion: z.boolean().optional(),
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
  })
  .loose()

const KNOWN_BASE_EVENT_FIELDS = new Set(Object.keys(BaseEventSchema.shape))
const EVENT_FACTORY_METADATA_FIELDS = new Set([
  'class',
  'fromJSON',
  'prototype',
  'event_schema',
  'model_fields',
  'event_type',
  'event_version',
  'event_result_type',
])
type AnyEventSchema = z.ZodObject<z.ZodRawShape>

export type BaseEventData = z.infer<typeof BaseEventSchema>
export type BaseEventJSON = BaseEventData & Record<string, unknown>
type BaseEventFieldName =
  | 'event_id'
  | 'event_created_at'
  | 'event_type'
  | 'event_version'
  | 'event_timeout'
  | 'event_slow_timeout'
  | 'event_handler_timeout'
  | 'event_handler_slow_timeout'
  | 'event_ttl'
  | 'event_result_ttl'
  | 'event_blocks_parent_completion'
  | 'event_parent_id'
  | 'event_path'
  | 'event_result_type'
  | 'event_emitted_by_handler_id'
  | 'event_pending_bus_count'
  | 'event_status'
  | 'event_started_at'
  | 'event_completed_at'
  | 'event_results'
  | 'event_concurrency'
  | 'event_handler_concurrency'
  | 'event_handler_completion'
type BaseEventFields = { [K in BaseEventFieldName]: BaseEventData[K] }

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields & Partial<BaseEventFields>

type BaseEventSchemaShape = typeof BaseEventSchema.shape
export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<BaseEventSchemaShape & TShape>
type EventPayloadShape<TShape extends z.ZodRawShape> = {
  [K in keyof TShape as K extends BaseEventFieldName ? never : K]: TShape[K]
}
type EventPayload<TShape extends z.ZodRawShape> =
  EventPayloadShape<TShape> extends Record<string, never> ? {} : z.infer<z.ZodObject<EventPayloadShape<TShape>>>
type EventPayloadRuntimeFieldName =
  | 'bus'
  | 'emit'
  | 'now'
  | 'wait'
  | 'toString'
  | 'toJSON'
  | 'fromJSON'
  | 'eventResult'
  | 'eventResultsList'
type AnyFunction = (...args: any[]) => unknown
type KnownEventPayloadFields<TEvent> = {
  [K in keyof TEvent as K extends string
    ? K extends `event_${string}`
      ? never
      : K extends `_${string}`
        ? never
        : K extends EventPayloadRuntimeFieldName
          ? never
          : TEvent[K] extends AnyFunction
            ? never
            : K
    : never]: TEvent[K]
}
export type EventPayloadFields<TEvent> = keyof KnownEventPayloadFields<TEvent> extends never
  ? Record<string, unknown>
  : KnownEventPayloadFields<TEvent>
type EventClassMetadataFieldName =
  | 'fromJSON'
  | 'prototype'
  | 'event_schema'
  | 'model_fields'
  | 'event_type'
  | 'event_version'
  | 'event_result_type'
type StaticDefaultSchema = z.ZodDefault<z.ZodTypeAny> | z.ZodPrefault<z.ZodTypeAny> | z.ZodCatch<z.ZodTypeAny>
type EventModelFields<TShape extends z.ZodRawShape> = {
  readonly [K in keyof TShape]: TShape[K]
}
type StaticEventDefaultValues<TShape extends z.ZodRawShape> = {
  readonly [K in keyof TShape as K extends EventClassMetadataFieldName
    ? never
    : TShape[K] extends StaticDefaultSchema
      ? K
      : never]: z.output<TShape[K]>
}
type StaticEventDefaultValuesFromSchema<TSchema extends AnyEventSchema> =
  TSchema extends z.ZodObject<infer TShape> ? StaticEventDefaultValues<TShape> : {}
type EventModelFieldsFromSchema<TSchema extends AnyEventSchema> =
  TSchema extends z.ZodObject<infer TShape> ? TSchema['shape'] & EventModelFields<TShape> : {}
type OptionalFactoryArgs<TData> = {} extends TData ? [data?: TData] : [data: TData]

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> & Partial<BaseEventFields>
type EventPayloadFromSchema<TSchema extends AnyEventSchema> =
  z.output<TSchema> extends Record<string, unknown> ? Omit<z.output<TSchema>, keyof BaseEventFields> : {}
type EventInputFromSchema<TSchema extends AnyEventSchema> = z.input<TSchema> extends Record<string, unknown> ? z.input<TSchema> : never
export type EventInitFromSchema<TSchema extends AnyEventSchema> = Omit<EventInputFromSchema<TSchema>, keyof BaseEventFields> &
  Partial<BaseEventFields>

type EventWithResultSchema<TResult> = BaseEvent & { __event_result_type__?: TResult }
type NormalizedEventResultSchema<TInput> = TInput extends z.ZodTypeAny
  ? TInput
  : TInput extends z.core.$ZodType
    ? z.ZodType<z.output<TInput>>
    : TInput extends StringConstructor
      ? z.ZodString
      : TInput extends NumberConstructor
        ? z.ZodNumber
        : TInput extends BooleanConstructor
          ? z.ZodBoolean
          : TInput extends ArrayConstructor
            ? z.ZodArray<z.ZodUnknown>
            : TInput extends ObjectConstructor
              ? z.ZodRecord<z.ZodString, z.ZodUnknown>
              : TInput extends JsonSchema
                ? z.ZodTypeAny
                : z.ZodTypeAny
type ResultTypeSchemaFromShape<TShape> = TShape extends { event_result_type: infer S }
  ? NormalizedEventResultSchema<S>
  : z.ZodTypeAny | undefined
type ResultTypeSchemaFromEventSchema<TSchema> =
  TSchema extends z.ZodObject<infer TShape> ? ResultTypeSchemaFromShape<TShape> : z.ZodTypeAny | undefined

type ResultTypeFromEventResultTypeInput<TInput> = TInput extends z.ZodTypeAny
  ? z.infer<TInput>
  : TInput extends z.core.$ZodType
    ? z.output<TInput>
    : TInput extends StringConstructor
      ? string
      : TInput extends NumberConstructor
        ? number
        : TInput extends BooleanConstructor
          ? boolean
          : TInput extends ArrayConstructor
            ? unknown[]
            : TInput extends ObjectConstructor
              ? Record<string, unknown>
              : TInput extends JsonSchema
                ? unknown
                : unknown

type ResultSchemaFromShape<TShape> = TShape extends { event_result_type: infer S } ? ResultTypeFromEventResultTypeInput<S> : unknown
type ResultSchemaFromEventSchema<TSchema> = TSchema extends z.ZodObject<infer TShape> ? ResultSchemaFromShape<TShape> : unknown
type ZodLiteralValue = string | number | bigint | boolean | null | undefined
type SeenShortcutLiteralPairs = WeakMap<object, WeakSet<object>>
type ShortcutDefaultModelField<K, TValue> = K extends keyof BaseEventSchemaShape
  ? z.ZodDefault<BaseEventSchemaShape[K]>
  : z.ZodDefault<TValue extends ZodLiteralValue ? z.ZodLiteral<TValue> : z.ZodType<TValue>>
type ShortcutModelFields<TShape> = {
  [K in keyof TShape as K extends 'event_result_type' ? never : K]: TShape[K] extends z.ZodTypeAny
    ? TShape[K]
    : ShortcutDefaultModelField<K, TShape[K]>
} & (TShape extends { event_result_type: infer TResultType } ? { event_result_type: NormalizedEventResultSchema<TResultType> } : {})
type ShortcutZodModelFields<TShape> = {
  [K in keyof ShortcutModelFields<TShape>]: ShortcutModelFields<TShape>[K] extends z.ZodTypeAny ? ShortcutModelFields<TShape>[K] : never
}
type ShortcutStaticDefaultValues<TShape, TModelFields extends z.ZodRawShape> = StaticEventDefaultValues<TModelFields> & {
  readonly [K in keyof TShape as K extends EventClassMetadataFieldName ? never : TShape[K] extends z.ZodTypeAny ? never : K]: TShape[K]
}
export type EventResultInclude<TEvent extends BaseEvent> = (
  result: EventResult<TEvent>['result'],
  event_result: EventResult<TEvent>
) => boolean
export type EventResultOptions<TEvent extends BaseEvent> = {
  include?: EventResultInclude<TEvent>
  raise_if_any?: boolean
  raise_if_none?: boolean
}
export type EventWaitOptions = {
  timeout?: number | null
  first_result?: boolean
}
export type EventResetOptions = {
  ids?: boolean
  status?: boolean
  timestamps?: boolean
  results?: boolean
}
export type EventWaitPromise<TEvent extends BaseEvent> = Promise<TEvent> & {
  eventResult(options?: EventResultOptions<TEvent>): Promise<EventResultType<TEvent> | undefined>
  eventResultsList(options?: EventResultOptions<TEvent>): Promise<Array<EventResultType<TEvent> | undefined>>
}
type EventResultUpdateOptions<TEvent extends BaseEvent> = {
  eventbus?: EventBus
  status?: 'pending' | 'started' | 'completed' | 'error'
  result?: EventResultType<TEvent> | BaseEvent | undefined
  error?: unknown
}

const ROOT_EVENTBUS_ID = '00000000-0000-0000-0000-000000000000'

export type EventClass<
  TEvent extends BaseEvent = BaseEvent,
  TInit = never,
  TSchema extends z.ZodTypeAny = AnyEventSchema,
  TModelFields extends z.ZodRawShape = z.ZodRawShape,
  TResultSchema extends z.ZodTypeAny | undefined = z.ZodTypeAny | undefined,
  TStaticFields = {},
> = TStaticFields & {
  (...args: OptionalFactoryArgs<TInit>): TEvent
  new (...args: OptionalFactoryArgs<TInit>): TEvent
  event_schema: TSchema
  model_fields: TModelFields
  event_type: string
  event_version: string
  event_result_type: TResultSchema
  fromJSON: (data: unknown) => TEvent
}

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends 'event_result_type' ? never : TShape[K] extends z.ZodTypeAny ? K : never]: Extract<
    TShape[K],
    z.ZodTypeAny
  >
}

function baseEventDefaultShape(event_type: string): z.ZodRawShape {
  return {
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_type: z.string().default(event_type),
    event_version: z.string().default('0.0.1'),
    event_timeout: z.number().gte(-1).nullable().default(null),
    event_slow_timeout: z.number().gte(-1).nullable().optional(),
    event_handler_timeout: z.number().gte(-1).nullable().optional(),
    event_handler_slow_timeout: z.number().gte(-1).nullable().optional(),
    event_ttl: z.number().gte(-1).nullable().optional(),
    event_result_ttl: z.number().gte(-1).nullable().optional(),
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

function missingBaseFields(event_type: string, user_shape: z.ZodRawShape): z.ZodRawShape {
  return Object.fromEntries(Object.entries(baseEventDefaultShape(event_type)).filter(([key]) => !(key in user_shape))) as z.ZodRawShape
}

function isZodLiteralValue(value: unknown): value is ZodLiteralValue {
  return value === null || value === undefined || ['string', 'number', 'bigint', 'boolean'].includes(typeof value)
}

function isPlainShortcutLiteralObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false
  }
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function alreadyComparedShortcutLiteralPair(left: object, right: object, seen: SeenShortcutLiteralPairs): boolean {
  let right_values = seen.get(left)
  if (right_values?.has(right)) {
    return true
  }
  if (!right_values) {
    right_values = new WeakSet<object>()
    seen.set(left, right_values)
  }
  right_values.add(right)
  return false
}

function shortcutLiteralValuesEqual(left: unknown, right: unknown, seen: SeenShortcutLiteralPairs = new WeakMap()): boolean {
  if (Object.is(left, right)) {
    return true
  }
  if (typeof left !== 'object' || left === null || typeof right !== 'object' || right === null) {
    return false
  }
  if (alreadyComparedShortcutLiteralPair(left, right, seen)) {
    return true
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
      return false
    }
    return left.every((item, index) => shortcutLiteralValuesEqual(item, right[index], seen))
  }
  if (!isPlainShortcutLiteralObject(left) || !isPlainShortcutLiteralObject(right)) {
    return false
  }
  const left_keys = Object.keys(left)
  const right_keys = Object.keys(right)
  if (left_keys.length !== right_keys.length) {
    return false
  }
  return left_keys.every((key) =>
    Object.prototype.hasOwnProperty.call(right, key) ? shortcutLiteralValuesEqual(left[key], right[key], seen) : false
  )
}

function shortcutLiteralSchema<TValue>(value: TValue): z.ZodType<TValue> {
  if (isZodLiteralValue(value)) {
    return z.literal(value) as z.ZodType<TValue>
  }
  return z.custom<TValue>((candidate) => shortcutLiteralValuesEqual(candidate, value), 'Invalid literal value')
}

function shortcutDefaultSchema(base_field_schema: z.ZodTypeAny | undefined, value: unknown): z.ZodTypeAny {
  if (!base_field_schema) {
    return shortcutLiteralSchema(value).default(value)
  }
  return base_field_schema.default(base_field_schema.parse(value))
}

function schemaDefaultsForShortcut(event_type: string, raw_shape: Record<string, unknown>): z.ZodRawShape {
  const defaults: Record<string, z.ZodTypeAny> = {}
  const base_shape = baseEventDefaultShape(event_type)
  for (const [key, value] of Object.entries(raw_shape)) {
    if (key === 'event_result_type') continue
    if (!isZodSchema(value)) {
      defaults[key] = shortcutDefaultSchema(base_shape[key] as z.ZodTypeAny | undefined, value)
    }
  }
  return defaults
}

function zodFieldsForShortcut(raw_shape: Record<string, unknown>): z.ZodRawShape {
  const fields: Record<string, z.ZodTypeAny> = {}
  for (const [key, value] of Object.entries(raw_shape)) {
    if (key === 'event_result_type') continue
    if (isZodSchema(value)) {
      fields[key] = value
    }
  }
  return fields
}

function modelFieldsForShortcut(raw_shape: Record<string, unknown>, shortcut_shape: z.ZodRawShape): z.ZodRawShape {
  const event_result_type = normalizeEventResultType(raw_shape.event_result_type)
  return event_result_type ? { ...shortcut_shape, event_result_type } : shortcut_shape
}

function staticEventDefaultsFromModelFields(model_fields: z.ZodRawShape): Record<string, unknown> {
  const fields: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(model_fields)) {
    if (EVENT_FACTORY_METADATA_FIELDS.has(key)) {
      continue
    }
    const parsed = (value as z.ZodTypeAny).safeParse(undefined)
    if (parsed.success && parsed.data !== undefined) {
      fields[key] = parsed.data
    }
  }
  return fields
}

function defineStaticEventFields(target: object, fields: Record<string, unknown>): void {
  for (const [key, value] of Object.entries(fields)) {
    Object.defineProperty(target, key, {
      value,
      writable: false,
      enumerable: true,
      configurable: true,
    })
  }
}

function eventResultTypeFromObjectSchema(schema: z.ZodObject<z.ZodRawShape>): z.ZodTypeAny | undefined {
  const raw_event_result_type = schema.shape.event_result_type
  return raw_event_result_type === undefined ? undefined : normalizeEventResultType(raw_event_result_type)
}

function eventParseSchemaFromEventSchema(schema: z.ZodObject<z.ZodRawShape>): z.ZodObject<z.ZodRawShape> {
  return schema.safeExtend({
    event_result_type: z.unknown().optional(),
  })
}

function buildFullEventSchema(
  event_type: string,
  spec: unknown
): {
  event_schema: z.ZodObject<z.ZodRawShape>
  event_parse_schema: z.ZodObject<z.ZodRawShape>
  static_field_defaults: Record<string, unknown>
  event_result_type?: z.ZodTypeAny
  event_version?: string
} {
  if (isZodObjectSchema(spec)) {
    const user_shape = spec.shape
    assertNoReservedUserEventFields(user_shape, `BaseEvent.extend(${event_type})`)
    assertNoUnknownEventPrefixedFields(user_shape, `BaseEvent.extend(${event_type})`)
    assertNoModelPrefixedFields(user_shape, `BaseEvent.extend(${event_type})`)
    const full_schema = spec.safeExtend({
      ...missingBaseFields(event_type, user_shape),
    })
    return {
      event_schema: full_schema,
      event_parse_schema: eventParseSchemaFromEventSchema(full_schema),
      static_field_defaults: staticEventDefaultsFromModelFields(full_schema.shape),
      event_result_type: eventResultTypeFromObjectSchema(spec),
    }
  }

  const raw_shape = (isRecord(spec) ? spec : {}) as Record<string, unknown>
  assertNoReservedUserEventFields(raw_shape, `BaseEvent.extend(${event_type})`)
  assertNoUnknownEventPrefixedFields(raw_shape, `BaseEvent.extend(${event_type})`)
  assertNoModelPrefixedFields(raw_shape, `BaseEvent.extend(${event_type})`)
  const shortcut_shape = {
    ...schemaDefaultsForShortcut(event_type, raw_shape),
    ...zodFieldsForShortcut(raw_shape),
  }
  const model_fields = modelFieldsForShortcut(raw_shape, shortcut_shape)
  const full_schema = z.object(model_fields).safeExtend(missingBaseFields(event_type, model_fields)).loose()
  return {
    event_schema: full_schema,
    event_parse_schema: eventParseSchemaFromEventSchema(full_schema),
    static_field_defaults: staticEventDefaultsFromModelFields(full_schema.shape),
    event_result_type: normalizeEventResultType(raw_shape.event_result_type),
    event_version: typeof raw_shape.event_version === 'string' ? raw_shape.event_version : undefined,
  }
}

function decodeEventSchema(schema: AnyEventSchema, input: unknown): Record<string, unknown> {
  const decoded = (z as unknown as { decode: (schema: AnyEventSchema, input: unknown) => unknown }).decode(schema, input)
  if (!isRecord(decoded)) {
    throw new Error('BaseEvent schema must decode to an object')
  }
  return decoded
}

function encodeEventSchema(schema: AnyEventSchema, input: Record<string, unknown>): Record<string, unknown> {
  const encoded = (z as unknown as { encode: (schema: AnyEventSchema, input: unknown) => unknown }).encode(schema, input)
  if (!isRecord(encoded)) {
    throw new Error('BaseEvent schema must encode to an object')
  }
  return encoded
}

function encodeEventSchemaForJSON(schema: AnyEventSchema, input: Record<string, unknown>): Record<string, unknown> {
  try {
    return encodeEventSchema(schema, input)
  } catch {
    return { ...input }
  }
}

function eventResultTypeToJSON(schema: z.ZodTypeAny | undefined): JsonSchema | undefined {
  if (!schema) {
    return undefined
  }
  return toJsonSchema(schema)
}

export class BaseEvent {
  // event metadata fields
  event_id!: string // unique uuidv7 identifier for the event
  event_created_at!: string
  event_type!: string // should match the class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  event_version!: string // event schema/version tag managed by callers for migration-friendly payload handling
  event_timeout!: number | null // maximum time in seconds that the event is allowed to run before it is aborted
  event_slow_timeout?: number | null // optional per-event slow warning threshold in seconds
  event_handler_timeout?: number | null // optional per-event handler timeout override in seconds
  event_handler_slow_timeout?: number | null // optional per-event slow handler warning threshold in seconds
  event_ttl?: number | null // optional seconds to keep this event in each bus history after completion
  event_result_ttl?: number | null // optional seconds to keep this event's handler results after completion
  event_blocks_parent_completion!: boolean // true only for children explicitly awaited via now()
  event_parent_id!: string | null // id of the parent event that triggered this event, if this event was emitted during handling of another event, else null
  event_path!: string[] // list of bus labels (name#id) that the event has been dispatched to, including the current bus
  event_result_type?: z.ZodTypeAny // optional zod schema to enforce the shape of return values from handlers
  event_results!: Map<string, EventResult<this>> // map of handler ids to EventResult objects for the event
  event_emitted_by_handler_id!: string | null // if event was emitted inside a handler while it was running, this is set to the enclosing handler's handler id, else null
  event_pending_bus_count!: number // number of buses that have accepted this event and not yet finished processing or removed it from their queues (for queue-jump processing)
  event_status!: 'pending' | 'started' | 'completed' // processing status of the event as a whole, no separate 'error' state because events can not error, only individual handlers can
  event_started_at!: string | null
  event_completed_at!: string | null
  event_concurrency?: EventConcurrencyMode | null // concurrency mode for the event as a whole in relation to other events
  event_handler_concurrency?: EventHandlerConcurrencyMode | null // concurrency mode for the handlers within the event
  event_handler_completion?: EventHandlerCompletionMode | null // completion strategy: 'all' (default) waits for every handler, 'first' returns earliest non-undefined result and cancels the rest
  event_schema?: z.ZodTypeAny
  _event_parse_schema?: z.ZodTypeAny

  static event_type?: string // class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  static event_version = '0.0.1'
  static event_result_type?: z.ZodTypeAny
  static event_schema: AnyEventSchema = BaseEventSchema // generated Zod schema for local TS event data validation; never sent over the wire
  static model_fields: z.ZodRawShape = BaseEventSchema.shape
  static _event_parse_schema: AnyEventSchema = BaseEventSchema

  // internal runtime state
  event_bus?: EventBus // bus that dispatched this event, also used by event.emit(child)
  _event_original?: BaseEvent // underlying event object that was dispatched, if this is a bus-scoped proxy wrapping it
  _event_dispatch_context?: unknown | null // captured AsyncLocalStorage context at dispatch site, used to restore that context when running handlers
  _event_expires_at_by_bus: Map<string, number>

  _event_completed_signal: Deferred<this> | null
  _lock_for_event_handler: AsyncLock | null
  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    assertNoReservedUserEventFields(data as Record<string, unknown>, 'BaseEvent')
    assertNoUnknownEventPrefixedFields(data as Record<string, unknown>, 'BaseEvent')
    assertNoModelPrefixedFields(data as Record<string, unknown>, 'BaseEvent')
    const ctor = this.constructor as typeof BaseEvent & {
      event_version?: string
      event_result_type?: z.ZodTypeAny
      event_schema?: AnyEventSchema
      _event_parse_schema?: AnyEventSchema
    }
    const merged_data = { ...data } as BaseEventInit<Record<string, unknown>>
    const event_type = merged_data.event_type ?? ctor.event_type ?? ctor.name
    const event_version = merged_data.event_version ?? ctor.event_version ?? '0.0.1'
    const raw_event_result_type = merged_data.event_result_type ?? ctor.event_result_type
    const event_result_type = normalizeEventResultType(raw_event_result_type)

    const event_schema = ctor.event_schema ?? BaseEventSchema
    const event_parse_schema = ctor._event_parse_schema ?? event_schema
    const base_data: Record<string, unknown> = {
      ...merged_data,
      event_id: merged_data.event_id ?? uuidv7(),
      event_created_at: merged_data.event_created_at ?? monotonicDatetime(),
      event_type,
      event_version,
      event_result_type,
    }
    if (event_parse_schema === BaseEventSchema) {
      base_data.event_timeout ??= null
      base_data.event_blocks_parent_completion ??= false
    }

    const parsed = decodeEventSchema(event_parse_schema, base_data) as BaseEventData & Record<string, unknown>

    Object.assign(this, parsed)
    Object.defineProperty(this, 'event_schema', {
      value: event_schema,
      writable: true,
      enumerable: false,
      configurable: true,
    })
    Object.defineProperty(this, '_event_parse_schema', {
      value: event_parse_schema,
      writable: true,
      enumerable: false,
      configurable: true,
    })
    const parsed_path = (parsed as { event_path?: string[] }).event_path
    this.event_path = Array.isArray(parsed_path) ? [...parsed_path] : []
    this.event_created_at = monotonicDatetime(parsed.event_created_at)
    this._event_expires_at_by_bus = new Map()

    // load event results from potentially raw objects from JSON to proper EventResult objects
    this.event_results = hydrateEventResults(this, (parsed as { event_results?: unknown }).event_results)
    this.event_pending_bus_count =
      typeof (parsed as { event_pending_bus_count?: unknown }).event_pending_bus_count === 'number'
        ? Math.max(0, Number((parsed as { event_pending_bus_count?: number }).event_pending_bus_count))
        : 0
    const parsed_status = (parsed as { event_status?: unknown }).event_status
    this.event_status =
      parsed_status === 'pending' || parsed_status === 'started' || parsed_status === 'completed' ? parsed_status : 'pending'

    this.event_started_at =
      parsed.event_started_at === null || parsed.event_started_at === undefined ? null : monotonicDatetime(parsed.event_started_at)
    this.event_completed_at =
      parsed.event_completed_at === null || parsed.event_completed_at === undefined ? null : monotonicDatetime(parsed.event_completed_at)
    this.event_parent_id =
      typeof (parsed as { event_parent_id?: unknown }).event_parent_id === 'string'
        ? (parsed as { event_parent_id: string }).event_parent_id
        : null
    this.event_emitted_by_handler_id =
      typeof (parsed as { event_emitted_by_handler_id?: unknown }).event_emitted_by_handler_id === 'string'
        ? (parsed as { event_emitted_by_handler_id: string }).event_emitted_by_handler_id
        : null

    if (
      (this.event_ttl === null || this.event_ttl === undefined) &&
      typeof (ctor as typeof BaseEvent & { event_ttl?: unknown }).event_ttl === 'number'
    ) {
      this.event_ttl = (ctor as typeof BaseEvent & { event_ttl: number }).event_ttl
    }
    if (
      (this.event_result_ttl === null || this.event_result_ttl === undefined) &&
      typeof (ctor as typeof BaseEvent & { event_result_ttl?: unknown }).event_result_ttl === 'number'
    ) {
      this.event_result_ttl = (ctor as typeof BaseEvent & { event_result_ttl: number }).event_result_ttl
    }

    this.event_result_type = normalizeEventResultType(parsed.event_result_type ?? event_result_type)

    this._event_completed_signal = null
    this._lock_for_event_handler = null
    this._event_dispatch_context = undefined
  }

  // "MyEvent#a48f"
  toString(): string {
    return `${this.event_type}#${this.event_id.slice(-4)}`
  }

  // main entry point for users to define their own event types
  // BaseEvent.extend("MyEvent", { some_custom_field: z.string(), event_result_type: z.string(), event_timeout: 25, ... }) -> MyEvent
  static extend<TSchema extends z.ZodObject<z.ZodRawShape>>(
    event_type: string,
    event_schema: TSchema
  ): EventClass<
    EventWithResultSchema<ResultSchemaFromEventSchema<TSchema>> & EventPayloadFromSchema<TSchema>,
    EventInitFromSchema<TSchema>,
    TSchema,
    EventModelFieldsFromSchema<TSchema>,
    ResultTypeSchemaFromEventSchema<TSchema>,
    StaticEventDefaultValuesFromSchema<TSchema>
  >
  static extend<const TShape extends Record<string, unknown>>(
    event_type: string,
    shape?: TShape
  ): EventClass<
    EventWithResultSchema<ResultSchemaFromShape<ShortcutZodModelFields<TShape>>> & EventPayload<ShortcutZodModelFields<TShape>>,
    EventInit<ShortcutZodModelFields<TShape>>,
    EventSchema<ShortcutZodModelFields<TShape>>,
    EventModelFields<ShortcutZodModelFields<TShape>>,
    ResultTypeSchemaFromShape<ShortcutZodModelFields<TShape>>,
    ShortcutStaticDefaultValues<TShape, ShortcutZodModelFields<TShape>>
  >
  static extend<TShape extends z.ZodRawShape>(
    event_type: string,
    shape?: TShape
  ): EventClass<
    EventWithResultSchema<ResultSchemaFromShape<TShape>> & EventPayload<TShape>,
    EventInit<TShape>,
    EventSchema<TShape>,
    EventModelFields<TShape>,
    ResultTypeSchemaFromShape<TShape>,
    StaticEventDefaultValues<TShape>
  >
  static extend<const TShape extends Record<string, unknown>>(
    event_type: string,
    shape?: TShape
  ):
    | EventClass<
        EventWithResultSchema<ResultSchemaFromShape<ZodShapeFrom<TShape>>> & EventPayload<ZodShapeFrom<TShape>>,
        EventInit<ZodShapeFrom<TShape>>,
        EventSchema<ZodShapeFrom<TShape>>,
        EventModelFields<ZodShapeFrom<TShape>>,
        ResultTypeSchemaFromShape<ZodShapeFrom<TShape>>,
        StaticEventDefaultValues<ZodShapeFrom<TShape>>
      >
    | EventClass<
        EventWithResultSchema<ResultSchemaFromEventSchema<AnyEventSchema>> & EventPayloadFromSchema<AnyEventSchema>,
        EventInitFromSchema<AnyEventSchema>,
        AnyEventSchema,
        EventModelFieldsFromSchema<AnyEventSchema>,
        ResultTypeSchemaFromEventSchema<AnyEventSchema>,
        StaticEventDefaultValuesFromSchema<AnyEventSchema>
      > {
    const built = buildFullEventSchema(event_type, shape ?? {})
    const full_schema = built.event_schema
    const event_parse_schema = built.event_parse_schema
    const static_field_defaults = built.static_field_defaults
    const event_result_type = built.event_result_type
    const event_version = built.event_version

    const EventClass = class extends BaseEvent {
      static override event_schema = full_schema as EventSchema<ZodShapeFrom<TShape>>
      static override model_fields = full_schema.shape as EventModelFields<ZodShapeFrom<TShape>>
      static override _event_parse_schema = event_parse_schema
      static override event_type = event_type
      static override event_version = event_version ?? BaseEvent.event_version
      static override event_result_type = event_result_type

      constructor(data?: EventInit<ZodShapeFrom<TShape>>) {
        super(data)
      }
    }

    Object.defineProperty(EventClass, 'name', { value: event_type, configurable: true })
    defineStaticEventFields(EventClass, static_field_defaults)

    let CallableEventClass: typeof EventClass
    CallableEventClass = new Proxy(EventClass, {
      apply(target, _this_arg, args) {
        return Reflect.construct(target, args, target)
      },
      construct(target, args, new_target) {
        return Reflect.construct(target, args, new_target === CallableEventClass ? target : new_target)
      },
    })

    Object.defineProperty(EventClass.prototype, 'constructor', {
      value: CallableEventClass,
      writable: true,
      configurable: true,
    })
    EVENT_TYPE_REGISTRY.set(event_type, CallableEventClass as unknown as typeof BaseEvent)

    return CallableEventClass as unknown as EventClass<
      EventWithResultSchema<ResultSchemaFromShape<ZodShapeFrom<TShape>>> & EventPayload<ZodShapeFrom<TShape>>,
      EventInit<ZodShapeFrom<TShape>>,
      EventSchema<ZodShapeFrom<TShape>>,
      EventModelFields<ZodShapeFrom<TShape>>,
      ResultTypeSchemaFromShape<ZodShapeFrom<TShape>>,
      StaticEventDefaultValues<ZodShapeFrom<TShape>>
    >
  }

  static fromJSON<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    if (!data || typeof data !== 'object') {
      const event_parse_schema = this._event_parse_schema ?? this.event_schema ?? BaseEventSchema
      const parsed = decodeEventSchema(event_parse_schema, data)
      return new this(parsed) as InstanceType<T>
    }
    const record = { ...(data as Record<string, unknown>) }
    if (this === BaseEvent) {
      const event_type = record.event_type
      if (typeof event_type === 'string') {
        const KnownEvent = EVENT_TYPE_REGISTRY.get(event_type)
        if (KnownEvent) {
          return KnownEvent.fromJSON(record) as InstanceType<T>
        }
      }
    }
    const ctor = this as typeof BaseEvent
    if (this !== BaseEvent && ctor.event_result_type && record.event_result_type !== undefined) {
      delete record.event_result_type
    }
    return new this(record as BaseEventInit<Record<string, unknown>>) as InstanceType<T>
  }

  static toJSONArray(events: Iterable<BaseEvent>): BaseEventJSON[] {
    return Array.from(events, (event) => {
      const original = event._event_original ?? event
      return original.toJSON()
    })
  }

  static fromJSONArray(data: unknown): BaseEvent[] {
    if (!Array.isArray(data)) {
      return []
    }
    return data.map((item) => BaseEvent.fromJSON(item))
  }

  toJSON(): BaseEventJSON {
    const record: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(this as unknown as Record<string, unknown>)) {
      if (key.startsWith('_') || key === 'bus' || key === 'event_bus' || key === 'event_schema' || key === 'event_results') continue
      if (value === undefined || typeof value === 'function') continue
      record[key] = value
    }
    const event_results = Object.fromEntries(
      Array.from(this.event_results.entries()).map(([handler_id, result]) => [handler_id, result.toJSON()])
    )

    const event_parse_schema = ((this.constructor as typeof BaseEvent)._event_parse_schema ??
      this._event_parse_schema ??
      (this.constructor as typeof BaseEvent).event_schema ??
      this.event_schema ??
      BaseEventSchema) as AnyEventSchema
    const encoded = encodeEventSchemaForJSON(event_parse_schema, {
      ...record,
      event_id: this.event_id,
      event_type: this.event_type,
      event_version: this.event_version,
      event_result_type: this.event_result_type,

      // static configuration options
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_timeout: this.event_handler_timeout,
      event_ttl: this.event_ttl,
      event_result_ttl: this.event_result_ttl,
      event_blocks_parent_completion: this.event_blocks_parent_completion,

      // mutable parent/child/bus tracking runtime state
      event_parent_id: this.event_parent_id,
      event_path: this.event_path,
      event_emitted_by_handler_id: this.event_emitted_by_handler_id,
      event_pending_bus_count: this.event_pending_bus_count,

      // mutable runtime status and timestamps
      event_status: this.event_status,
      event_created_at: this.event_created_at,
      event_started_at: this.event_started_at ?? null,
      event_completed_at: this.event_completed_at ?? null,

      ...(Object.keys(event_results).length > 0 ? { event_results } : {}),
    })
    delete encoded.event_schema

    return {
      ...encoded,
      event_id: this.event_id,
      event_type: this.event_type,
      event_version: this.event_version,
      event_result_type: eventResultTypeToJSON(this.event_result_type),

      // static configuration options
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_timeout: this.event_handler_timeout,
      event_ttl: this.event_ttl,
      event_result_ttl: this.event_result_ttl,
      event_blocks_parent_completion: this.event_blocks_parent_completion,

      // mutable parent/child/bus tracking runtime state
      event_parent_id: this.event_parent_id,
      event_path: this.event_path,
      event_emitted_by_handler_id: this.event_emitted_by_handler_id,
      event_pending_bus_count: this.event_pending_bus_count,

      // mutable runtime status and timestamps
      event_status: this.event_status,
      event_created_at: this.event_created_at,
      event_started_at: this.event_started_at ?? null,
      event_completed_at: this.event_completed_at ?? null,

      // mutable result state
      ...(Object.keys(event_results).length > 0 ? { event_results } : {}),
    }
  }

  event_payload(): EventPayloadFields<this> {
    return Object.fromEntries(Object.entries(this.toJSON()).filter(([key]) => !key.startsWith('event_'))) as EventPayloadFields<this>
  }

  _createSlowEventWarningTimer(
    event_slow_timeout: number | null = this.event_slow_timeout ?? null,
    bus_name?: string
  ): ReturnType<typeof setTimeout> | null {
    const event_warn_ms = event_slow_timeout === null || event_slow_timeout <= 0 ? null : event_slow_timeout * 1000
    if (event_warn_ms === null) {
      return null
    }
    const name = bus_name ?? this.event_bus?.name ?? 'EventBus'
    return setTimeout(() => {
      if (this.event_status === 'completed') {
        return
      }
      const running_handler_count = [...this.event_results.values()].filter((result) => result.status === 'started').length
      const started_at = this.event_started_at ?? this.event_created_at
      const elapsed_ms = Math.max(0, Date.now() - Date.parse(started_at))
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(2)
      console.warn(
        `[abxbus] Slow event processing: ${name}.on(${this.event_type}#${this.event_id.slice(-4)}, ${running_handler_count} handlers) still running after ${elapsed_seconds}s`
      )
    }, event_warn_ms)
  }

  eventResultUpdate(handler: EventHandler | EventHandlerCallable<this>, options: EventResultUpdateOptions<this> = {}): EventResult<this> {
    const original_event = (this._event_original ?? this) as this
    let resolved_eventbus = options.eventbus
    let handler_entry: EventHandler

    if (handler instanceof EventHandler) {
      handler_entry = handler
      if (!resolved_eventbus && handler_entry.eventbus_id !== ROOT_EVENTBUS_ID && original_event.event_bus) {
        resolved_eventbus =
          original_event.event_bus.all_instances.findBusById(handler_entry.eventbus_id) ??
          (original_event.event_bus.id === handler_entry.eventbus_id ? original_event.event_bus : undefined)
      }
    } else {
      handler_entry = EventHandler.fromCallable({
        handler,
        event_pattern: original_event.event_type,
        eventbus_name: resolved_eventbus?.name ?? 'EventBus',
        eventbus_id: resolved_eventbus?.id ?? ROOT_EVENTBUS_ID,
      })
    }

    const scoped_event = resolved_eventbus ? resolved_eventbus._getEventProxyScopedToThisBus(original_event) : original_event
    const handler_id = handler_entry.id
    const existing = original_event.event_results.get(handler_id)
    const event_result: EventResult<this> =
      existing ?? (new EventResult({ event: scoped_event as this, handler: handler_entry }) as EventResult<this>)
    if (!existing) {
      original_event.event_results.set(handler_id, event_result)
    } else {
      if (existing.event !== scoped_event) {
        existing.event = scoped_event as this
      }
      if (existing.handler.id !== handler_entry.id) {
        existing.handler = handler_entry
      }
    }

    if (options.status !== undefined || options.result !== undefined || options.error !== undefined) {
      const update_params: Parameters<EventResult<this>['update']>[0] = {}
      if (options.status !== undefined) update_params.status = options.status
      if (options.result !== undefined) update_params.result = options.result
      if (options.error !== undefined) update_params.error = options.error
      event_result.update(update_params)
      if (event_result.status === 'started' && event_result.started_at !== null) {
        original_event._markStarted(event_result.started_at, false)
      }
      if (options.status === 'pending' || options.status === 'started') {
        original_event.event_completed_at = null
      }
    }

    return event_result
  }

  _createPendingHandlerResults(bus: EventBus): Array<{
    handler: EventHandler
    result: EventResult
  }> {
    const original_event = this._event_original ?? this
    const scoped_event = bus._getEventProxyScopedToThisBus(original_event)
    const handlers = bus._getHandlersForEvent(original_event)
    return handlers.map((entry) => {
      const handler_id = entry.id
      const existing = original_event.event_results.get(handler_id)
      const result = existing ?? new EventResult({ event: scoped_event, handler: entry })
      if (!existing) {
        original_event.event_results.set(handler_id, result)
      } else if (existing.event !== scoped_event) {
        existing.event = scoped_event
      }
      return { handler: entry, result }
    })
  }

  private _collectPendingResults(
    original: BaseEvent,
    pending_entries?: Array<{
      handler: EventHandler
      result: EventResult
    }>
  ): EventResult[] {
    if (pending_entries) {
      return pending_entries.map((entry) => entry.result)
    }
    if (!this.event_bus?.id) {
      return Array.from(original.event_results.values())
    }
    return Array.from(original.event_results.values()).filter((result) => result.eventbus_id === this.event_bus!.id)
  }

  private _isFirstModeWinningResult(entry: EventResult): boolean {
    return BaseEvent._defaultResultInclude(entry.result, entry)
  }

  private static _defaultResultInclude<TEvent extends BaseEvent>(
    result: EventResult<TEvent>['result'],
    event_result: EventResult<TEvent>
  ): boolean {
    return (
      event_result.status === 'completed' &&
      result !== undefined &&
      result !== null &&
      !(result instanceof Error) &&
      !(result instanceof BaseEvent) &&
      event_result.error === undefined
    )
  }

  private static _includeEventResult<TEvent extends BaseEvent>(
    include: EventResultInclude<TEvent>,
    event_result: EventResult<TEvent>
  ): boolean {
    return include(event_result.result, event_result)
  }

  private _markFirstModeWinnerIfNeeded(original: BaseEvent, entry: EventResult, first_state: { found: boolean }): void {
    if (first_state.found || !this._isFirstModeWinningResult(entry)) {
      return
    }
    first_state.found = true
    original._markRemainingFirstModeResultCancelled(entry)
  }

  private async _runHandlerWithLock(original: BaseEvent, entry: EventResult): Promise<void> {
    if (!this.event_bus) {
      throw new Error('event has no bus attached')
    }
    await this.event_bus.locks._runWithHandlerLock(
      original,
      original.event_handler_concurrency ?? this.event_bus.event_handler_concurrency,
      async (handler_lock) => {
        await entry.runHandler(handler_lock)
      }
    )
  }

  // Run all pending handler results for the current bus context.
  async _runHandlers(
    pending_entries?: Array<{
      handler: EventHandler
      result: EventResult
    }>
  ): Promise<void> {
    const original = this._event_original ?? this
    const pending_results = this._collectPendingResults(original, pending_entries)
    if (pending_results.length === 0) {
      return
    }
    const resolved_completion = original.event_handler_completion ?? this.event_bus?.event_handler_completion ?? 'all'
    if (resolved_completion === 'first') {
      if (original._getHandlerLock(original.event_handler_concurrency ?? this.event_bus?.event_handler_concurrency ?? 'serial') !== null) {
        for (const entry of pending_results) {
          await this._runHandlerWithLock(original, entry)
          if (!this._isFirstModeWinningResult(entry)) {
            continue
          }
          original._markRemainingFirstModeResultCancelled(entry)
          break
        }
        return
      }
      const first_state = { found: false }
      const handler_promises = pending_results.map((entry) => this._runHandlerWithLock(original, entry))
      const monitored = pending_results.map((entry, index) =>
        handler_promises[index].then(() => {
          this._markFirstModeWinnerIfNeeded(original, entry, first_state)
        })
      )
      await Promise.all(monitored)
      return
    } else {
      const handler_promises = pending_results.map((entry) => this._runHandlerWithLock(original, entry))
      await Promise.all(handler_promises)
    }
  }

  _getHandlerLock(default_concurrency?: EventHandlerConcurrencyMode): AsyncLock | null {
    const original = this._event_original ?? this
    const resolved = original.event_handler_concurrency ?? default_concurrency ?? 'serial'
    if (resolved === 'parallel') {
      return null
    }
    if (!original._lock_for_event_handler) {
      original._lock_for_event_handler = new AsyncLock(1)
    }
    return original._lock_for_event_handler
  }

  _setHandlerLock(lock: AsyncLock | null): void {
    const original = this._event_original ?? this
    original._lock_for_event_handler = lock
  }

  _getDispatchContext(): unknown | null | undefined {
    const original = this._event_original ?? this
    return original._event_dispatch_context
  }

  _setDispatchContext(dispatch_context: unknown | null | undefined): void {
    const original = this._event_original ?? this
    original._event_dispatch_context = dispatch_context
  }

  // Get parent event object from event_parent_id (checks across all buses)
  get event_parent(): BaseEvent | undefined {
    const original = this._event_original ?? this
    const parent_id = original.event_parent_id
    if (!parent_id) {
      return undefined
    }
    return original.event_bus?.findEventById(parent_id) ?? undefined
  }

  // get all direct children of this event
  get event_children(): BaseEvent[] {
    const children: BaseEvent[] = []
    const seen = new Set<string>()
    for (const result of this.event_results.values()) {
      for (const child of result.event_children) {
        if (!seen.has(child.event_id)) {
          seen.add(child.event_id)
          children.push(child)
        }
      }
    }
    return children
  }

  // get all children grandchildren etc. recursively
  get event_descendants(): BaseEvent[] {
    const descendants: BaseEvent[] = []
    const visited = new Set<string>()
    const root_id = this.event_id
    const stack = [...this.event_children]

    while (stack.length > 0) {
      const child = stack.pop()
      if (!child) {
        continue
      }
      const child_id = child.event_id
      if (child_id === root_id) {
        continue
      }
      if (visited.has(child_id)) {
        continue
      }
      visited.add(child_id)
      descendants.push(child)
      if (child.event_children.length > 0) {
        stack.push(...child.event_children)
      }
    }

    return descendants
  }

  emit<T extends BaseEvent>(event: T): T {
    const original_parent = this._event_original ?? this
    const original_child = event._event_original ?? event
    if (!original_child.event_parent_id && original_child.event_id !== original_parent.event_id) {
      original_child.event_parent_id = original_parent.event_id
    }
    if (!this.event_bus) {
      throw new Error('event has no bus attached')
    }
    return this.event_bus.emit(original_child as T)
  }

  // force-abort processing of all pending descendants of an event regardless of whether they have already started
  _cancelPendingChildProcessing(reason: unknown): void {
    const original = this._event_original ?? this
    const cancellation_cause =
      reason instanceof EventHandlerTimeoutError
        ? reason
        : reason instanceof EventHandlerCancelledError || reason instanceof EventHandlerAbortedError
          ? reason.cause instanceof Error
            ? reason.cause
            : reason
          : reason instanceof Error
            ? reason
            : new Error(String(reason))
    const visited = new Set<string>()
    const cancelChildEvent = (child: BaseEvent): void => {
      const original_child = child._event_original ?? child
      if (visited.has(original_child.event_id)) {
        return
      }
      visited.add(original_child.event_id)

      // Depth-first: cancel grandchildren before parent so
      // _areAllChildrenComplete() returns true when we get back up.
      for (const grandchild of original_child.event_children) {
        const original_grandchild = grandchild._event_original ?? grandchild
        if (!original_grandchild.event_blocks_parent_completion) {
          continue
        }
        cancelChildEvent(grandchild)
      }

      original_child._markCancelled(cancellation_cause)

      // Force-complete the child event. In JS we can't stop running async
      // handlers, but _markCompleted() resolves active waiters so callers
      // aren't blocked waiting for background work to finish. The background
      // handler's eventual _markCompleted/_markError is a no-op (terminal guard).
      if (original_child.event_status !== 'completed') {
        original_child._markCompleted()
      }
    }

    for (const child of original.event_children) {
      const original_child = child._event_original ?? child
      if (!original_child.event_blocks_parent_completion) {
        continue
      }
      cancelChildEvent(child)
    }
  }

  // Cancel all handler results for an event except the winner, used by event_handler_completion='first'.
  // Cancels pending handlers immediately, aborts started handlers via _signalAbort(),
  // and cancels any child events emitted by the losing handlers.
  _markRemainingFirstModeResultCancelled(winner: EventResult): void {
    const cause = new Error("event_handler_completion='first' resolved: another handler returned a result first")
    const bus_id = winner.eventbus_id

    for (const result of this.event_results.values()) {
      if (result === winner) continue
      if (result.eventbus_id !== bus_id) continue

      if (result.status === 'pending') {
        result._markError(
          new EventHandlerCancelledError(`Cancelled: event_handler_completion='first' resolved`, {
            event_result: result,
            cause,
          })
        )
      } else if (result.status === 'started') {
        // Cancel child events emitted by this handler before aborting it
        for (const child of result.event_children) {
          const original_child = child._event_original ?? child
          if (!original_child.event_blocks_parent_completion) {
            continue
          }
          original_child._cancelPendingChildProcessing(cause)
          original_child._markCancelled(cause)
        }

        // Abort the handler itself
        result._lock?.exitHandlerRun()
        const aborted_error = new EventHandlerAbortedError(`Aborted: event_handler_completion='first' resolved`, {
          event_result: result,
          cause,
        })
        result._markError(aborted_error)
        result._signalAbort(aborted_error)
      }
    }
  }

  // force-abort processing of this event regardless of whether it is pending or has already started
  _markCancelled(cause: Error): void {
    const original = this._event_original ?? this
    if (!this.event_bus) {
      if (original.event_status !== 'completed') {
        original._markCompleted()
      }
      return
    }
    const path = Array.isArray(original.event_path) ? original.event_path : []
    const buses_to_cancel = new Set<string>(path)
    for (const bus of this.event_bus.all_instances) {
      if (!buses_to_cancel.has(bus.label)) {
        continue
      }

      const handler_entries = original._createPendingHandlerResults(bus)
      let updated = false
      for (const entry of handler_entries) {
        if (entry.result.status === 'pending') {
          const cancelled_error = new EventHandlerCancelledError(`Cancelled pending handler due to parent error: ${cause.message}`, {
            event_result: entry.result,
            cause,
          })
          entry.result._markError(cancelled_error)
          updated = true
        } else if (entry.result.status === 'started') {
          entry.result._lock?.exitHandlerRun()
          const aborted_error = new EventHandlerAbortedError(`Aborted running handler due to parent error: ${cause.message}`, {
            event_result: entry.result,
            cause,
          })
          entry.result._markError(aborted_error)
          entry.result._signalAbort(aborted_error)
          updated = true
        }
      }

      const removed = bus.removeEventFromPendingQueue(original)

      if (removed > 0 && !bus.isEventInFlightOrQueued(original.event_id)) {
        original.event_pending_bus_count = Math.max(0, original.event_pending_bus_count - 1)
      }

      if (updated || removed > 0) {
        original._markCompleted(false)
      }
    }

    if (original.event_status !== 'completed') {
      original._markCompleted()
    }
  }

  _notifyEventParentsOfCompletion(): void {
    const original = this._event_original ?? this
    if (!this.event_bus) {
      return
    }
    const visited = new Set<string>()
    let parent_id = original.event_parent_id
    while (parent_id && !visited.has(parent_id)) {
      visited.add(parent_id)
      const parent = this.event_bus.findEventById(parent_id)
      if (!parent) {
        break
      }
      parent._markCompleted(false, false)
      if (parent.event_status !== 'completed') {
        break
      }
      parent_id = parent.event_parent_id
    }
  }

  private _withEventResultMethods(promise: Promise<this>): EventWaitPromise<this> {
    const chainable = promise as EventWaitPromise<this>
    chainable.eventResult = async (options?: EventResultOptions<this>) => {
      const event = await promise
      return event.eventResult(options)
    }
    chainable.eventResultsList = async (options?: EventResultOptions<this>) => {
      const event = await promise
      return event.eventResultsList(options)
    }
    return chainable
  }

  private _timeoutPromise<T>(timeout: number | null, message: () => string, fn: () => Promise<T>): Promise<T> {
    return timeout === null || timeout <= 0 ? fn() : _runWithTimeout(timeout, () => new Error(message()), fn)
  }

  private _orderedEventResults(): EventResult<this>[] {
    const original = this._event_original ?? this
    return (Array.from(original.event_results.values()) as EventResult<this>[]).sort((a, b) =>
      compareIsoDatetime(a.completed_at, b.completed_at)
    )
  }

  private _orderedEventResultsByRegistration(): EventResult<this>[] {
    const original = this._event_original ?? this
    return (Array.from(original.event_results.values()) as EventResult<this>[]).sort(
      (a, b) =>
        compareIsoDatetime(a.handler.handler_registered_at, b.handler.handler_registered_at) ||
        compareIsoDatetime(a.started_at, b.started_at) ||
        a.handler_id.localeCompare(b.handler_id)
    )
  }

  private _collectResultValues(
    options: EventResultOptions<this> = {},
    order: 'completion' | 'registration' = 'completion'
  ): Array<EventResultType<this> | undefined> {
    const include: EventResultInclude<this> = options.include ?? BaseEvent._defaultResultInclude
    const raise_if_any = options.raise_if_any ?? true
    const raise_if_none = options.raise_if_none ?? false
    const all_results = order === 'registration' ? this._orderedEventResultsByRegistration() : this._orderedEventResults()
    const error_results = all_results.filter((event_result) => event_result.error !== undefined || event_result.result instanceof Error)
    const included_results = all_results.filter((event_result) => BaseEvent._includeEventResult(include, event_result))

    if (error_results.length > 0 && raise_if_any) {
      const errors = error_results.map((event_result) => {
        if (event_result.error instanceof Error) {
          return event_result.error
        }
        if (event_result.result instanceof Error) {
          return event_result.result
        }
        return new Error(String(event_result.error ?? event_result.result))
      })
      if (errors.length === 1) {
        throw errors[0]
      }
      throw new AggregateError(errors, `Event ${this.event_type}#${this.event_id.slice(-4)} had ${errors.length} handler error(s)`)
    }

    if (raise_if_none && included_results.length === 0) {
      throw new Error(
        `Expected at least one handler to return a non-null result, but none did: ${this.event_type}#${this.event_id.slice(-4)}`
      )
    }

    return included_results.map((event_result) => event_result.result)
  }

  private _hasIncludedResult(options: EventResultOptions<this> = {}): boolean {
    const include: EventResultInclude<this> = options.include ?? BaseEvent._defaultResultInclude
    return this._orderedEventResults().some((event_result) => BaseEvent._includeEventResult(include, event_result))
  }

  private async _waitForFirstResultOrCompletion(options: EventWaitOptions & EventResultOptions<this> = {}): Promise<this> {
    const original = this._event_original ?? this
    if (options.timeout !== undefined && options.timeout !== null && options.timeout < 0) {
      throw new Error('timeout must be >= 0 or null')
    }
    if (!this.event_bus && original.event_status !== 'completed') {
      throw new Error('event has no bus attached')
    }
    if (original.event_status === 'completed' || this._hasIncludedResult(options)) {
      return this
    }

    const waitForResult = async (): Promise<this> => {
      for (;;) {
        if (original.event_status === 'completed' || this._hasIncludedResult(options)) {
          return this
        }
        await new Promise((resolve) => setTimeout(resolve, 1))
      }
    }

    const timeout = options.timeout ?? null
    return this._timeoutPromise(timeout, () => `Timed out waiting for ${original.event_type} result after ${timeout}s`, waitForResult)
  }

  // Active awaitable that triggers immediate (queue-jump) processing of the event on all buses where it is queued.
  now(options: EventWaitOptions = {}): EventWaitPromise<this> {
    const original = this._event_original ?? this
    if (options.timeout !== undefined && options.timeout !== null && options.timeout < 0) {
      return this._withEventResultMethods(Promise.reject(new Error('timeout must be >= 0 or null')))
    }
    if (!this.event_bus && original.event_status !== 'completed') {
      return this._withEventResultMethods(Promise.reject(new Error('event has no bus attached')))
    }
    original._markBlocksParentCompletionIfAwaitedFromEmittingHandler()
    const resolved_timeout_seconds = options.timeout ?? null
    const processing =
      original.event_status === 'completed'
        ? Promise.resolve(this)
        : this._timeoutPromise(
            resolved_timeout_seconds,
            () => `Timed out waiting for ${original.event_type} completion after ${resolved_timeout_seconds}s`,
            () => this.event_bus!._processEventImmediately(this)
          )

    if (options.first_result) {
      void processing.catch(() => undefined)
      return this._withEventResultMethods(this._waitForFirstResultOrCompletion(options))
    }

    return this._withEventResultMethods(processing)
  }

  // Passive awaitable that waits for normal queue-order processing without forcing execution.
  wait(options: EventWaitOptions = {}): EventWaitPromise<this> {
    const original = this._event_original ?? this
    if (options.timeout !== undefined && options.timeout !== null && options.timeout < 0) {
      return this._withEventResultMethods(Promise.reject(new Error('timeout must be >= 0 or null')))
    }
    if (!this.event_bus && original.event_status !== 'completed') {
      return this._withEventResultMethods(Promise.reject(new Error('event has no bus attached')))
    }
    if (options.first_result) {
      return this._withEventResultMethods(this._waitForFirstResultOrCompletion(options))
    }
    if (original.event_status === 'completed') {
      return this._withEventResultMethods(Promise.resolve(this))
    }
    this._notifyDoneListeners()
    const timeout = options.timeout ?? null
    return this._withEventResultMethods(
      this._timeoutPromise(
        timeout,
        () => `Timed out waiting for ${original.event_type} completion after ${timeout}s`,
        () => this._event_completed_signal!.promise.then(() => this)
      )
    )
  }

  async eventResult(options: EventResultOptions<this> = {}): Promise<EventResultType<this> | undefined> {
    const original = this._event_original ?? this
    if (original.event_status === 'pending' && original.event_results.size === 0) {
      await this.now({ first_result: true })
    }
    return this._collectResultValues(options, 'registration').at(0)
  }

  async eventResultsList(options: EventResultOptions<this> = {}): Promise<Array<EventResultType<this> | undefined>> {
    const original = this._event_original ?? this
    if (original.event_status === 'pending' && original.event_results.size === 0) {
      await this.now({ first_result: false })
    }
    return this._collectResultValues(options, 'registration')
  }

  _markBlocksParentCompletionIfAwaitedFromEmittingHandler(): void {
    const original = this._event_original ?? this
    if (original.event_blocks_parent_completion || !original.event_bus) {
      return
    }
    const active_result = original.event_bus.locks._getActiveHandlerResultForCurrentAsyncContext()
    if (!active_result || active_result.status !== 'started') {
      return
    }
    const active_parent = active_result.event._event_original ?? active_result.event
    const is_child_of_active_handler =
      original.event_parent_id === active_parent.event_id &&
      original.event_emitted_by_handler_id === active_result.handler_id &&
      active_result.event_children.some((child) => (child._event_original ?? child).event_id === original.event_id)
    if (is_child_of_active_handler) {
      original.event_blocks_parent_completion = true
    }
  }

  _resetForDispatch(options: EventResetOptions = {}): this {
    const original = this._event_original ?? this
    const ids = options.ids ?? true
    const status = options.status ?? true
    const timestamps = options.timestamps ?? true
    const results = options.results ?? true

    // Keep this helper as the single lifecycle reset path for bridges and eventReset redispatch.
    if (ids) {
      original.event_id = uuidv7()
      original.event_path = []
      original.event_parent_id = null
      original.event_emitted_by_handler_id = null
      original.event_blocks_parent_completion = false
    }
    if (status) {
      original.event_status = 'pending'
    }
    if (timestamps) {
      original.event_started_at = null
      original.event_completed_at = null
    }
    if (results) {
      original.event_results.clear()
    }
    original.event_pending_bus_count = 0
    original._setDispatchContext(undefined)
    original._event_completed_signal = null
    original._lock_for_event_handler = null
    original.event_bus = undefined
    original._event_expires_at_by_bus.clear()
    return this
  }

  eventReset(options: EventResetOptions = {}): this {
    const original = this._event_original ?? this
    const ctor = original.constructor as typeof BaseEvent
    const fresh_event = ctor.fromJSON(original.toJSON()) as this
    return fresh_event._resetForDispatch(options)
  }

  _markStarted(started_at: string | null = null, notify_hook: boolean = true): void {
    const original = this._event_original ?? this
    if (original.event_status !== 'pending' || original._shouldSkipHandlerExecution()) {
      return
    }
    original.event_status = 'started'
    original.event_started_at = started_at === null ? monotonicDatetime() : monotonicDatetime(started_at)
    if (notify_hook && original.event_bus) {
      const bus_for_hook = original.event_bus
      const event_for_bus = bus_for_hook._getEventProxyScopedToThisBus(original)
      void bus_for_hook.onEventChange(event_for_bus, 'started')
    }
  }

  _markCompleted(force: boolean = true, notify_parents: boolean = true): void {
    const original = this._event_original ?? this
    const was_completed = original.event_status === 'completed'
    const completion_marked = original._shouldSkipHandlerExecution()
    if (!force && !completion_marked) {
      if (original.event_pending_bus_count > 0) {
        return
      }
      if (!original._areAllChildrenComplete()) {
        return
      }
    }
    original.event_status = 'completed'
    original.event_completed_at = original.event_completed_at ?? monotonicDatetime()
    original.event_started_at = original.event_started_at ?? original.event_completed_at
    if (!was_completed && original.event_bus) {
      const bus_for_hook = original.event_bus
      const event_for_bus = bus_for_hook._getEventProxyScopedToThisBus(original)
      void bus_for_hook.onEventChange(event_for_bus, 'completed')
    }
    original._setDispatchContext(null)
    if (!was_completed) {
      original._notifyDoneListeners()
    }
    if (original._event_completed_signal) {
      original._event_completed_signal.resolve(original)
      original._event_completed_signal = null
    }
    if (!was_completed) {
      original.dropFromZeroHistoryBuses()
    }
    if (!was_completed && notify_parents && original.event_bus) {
      original._notifyEventParentsOfCompletion()
    }
  }

  _shouldSkipHandlerExecution(): boolean {
    const original = this._event_original ?? this
    return original.event_status === 'completed' || original.event_completed_at !== null
  }

  private dropFromZeroHistoryBuses(): void {
    if (!this.event_bus) {
      return
    }
    const original = this._event_original ?? this
    for (const bus of this.event_bus.all_instances) {
      if (bus.event_history.max_history_size !== 0) {
        continue
      }
      bus.removeEventFromHistory(original.event_id)
    }
  }

  get event_errors(): unknown[] {
    return (
      Array.from(this.event_results.values())
        // filter for events that have completed + have non-undefined error values
        .filter((event_result) => event_result.error !== undefined && event_result.completed_at !== null)
        // sort by completion time
        .sort((event_result_a, event_result_b) => compareIsoDatetime(event_result_a.completed_at, event_result_b.completed_at))
        // assemble array of flat error values
        .map((event_result) => event_result.error)
    )
  }

  _firstProcessingError(): unknown | undefined {
    return Array.from(this.event_results.values())
      .filter((event_result) => event_result.error !== undefined && event_result.completed_at !== null)
      .sort((event_result_a, event_result_b) => compareIsoDatetime(event_result_a.completed_at, event_result_b.completed_at))
      .map((event_result) => event_result.error)
      .at(0)
  }

  _areAllChildrenComplete(visited: Set<string> = new Set()): boolean {
    const original = this._event_original ?? this
    if (visited.has(original.event_id)) {
      return true
    }
    visited.add(original.event_id)

    for (const child of original.event_children) {
      const original_child = child._event_original ?? child
      if (!original_child.event_blocks_parent_completion) {
        continue
      }
      if (original_child.event_status !== 'completed') {
        return false
      }
      if (!original_child._areAllChildrenComplete(visited)) {
        return false
      }
    }
    return true
  }

  private _notifyDoneListeners(): void {
    if (this._event_completed_signal) {
      return
    }
    this._event_completed_signal = withResolvers<this>()
  }

  // Break internal reference chains so a completed event can be GC'd when
  // Evicted from event_history. Called by EventHistory.trimEventHistory().
  _gc(): void {
    this._event_completed_signal = null
    this._setDispatchContext(null)
    this.event_bus = undefined
    this._lock_for_event_handler = null
    for (const result of this.event_results.values()) {
      result.event_children = []
    }
    this.event_results.clear()
  }
}

const hydrateEventResults = <TEvent extends BaseEvent>(event: TEvent, raw_event_results: unknown): Map<string, EventResult<TEvent>> => {
  const event_results = new Map<string, EventResult<TEvent>>()
  if (raw_event_results == null) {
    return event_results
  }
  if (typeof raw_event_results !== 'object' || Array.isArray(raw_event_results)) {
    throw new Error('BaseEvent.event_results must be an object keyed by handler id')
  }
  for (const [handler_id, item] of Object.entries(raw_event_results)) {
    if (item == null || typeof item !== 'object' || Array.isArray(item)) continue
    const result = EventResult.fromJSON(event, {
      handler_id,
      ...(item as Record<string, unknown>),
    })
    event_results.set(handler_id, result)
  }
  return event_results
}
