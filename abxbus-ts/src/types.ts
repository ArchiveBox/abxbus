import { z } from 'zod'
import type { BaseEvent } from './base_event.js'

export type EventStatus = 'pending' | 'started' | 'completed'

export type EventClass<T extends BaseEvent = BaseEvent> = { event_type?: string } & (new (...args: any[]) => T)

export type EventPattern<T extends BaseEvent = BaseEvent> = string | EventClass<T>

export type EventWithResultSchema<TResult> = BaseEvent & { __event_result_type__?: TResult }

export type EventResultType<TEvent extends BaseEvent> = TEvent extends { __event_result_type__?: infer TResult } ? TResult : unknown

export type EventResultTypeConstructor = StringConstructor | NumberConstructor | BooleanConstructor | ArrayConstructor | ObjectConstructor

export type EventResultTypeInput = z.ZodTypeAny | EventResultTypeConstructor | unknown

export type EventHandlerReturn<T extends BaseEvent = BaseEvent> = EventResultType<T> | BaseEvent | null | void

export type EventHandlerCallable<T extends BaseEvent = BaseEvent> = (event: T) => EventHandlerReturn<T> | Promise<EventHandlerReturn<T>>

// For string and wildcard subscriptions we cannot reliably infer which event
// type will arrive, so return type checking intentionally degrades to unknown.
export type UntypedEventHandlerFunction<T extends BaseEvent = BaseEvent> = (
  event: T
) => EventHandlerReturn<T> | unknown | Promise<EventHandlerReturn<T> | unknown>

export type FindWindow = boolean | number

type FindReservedOptionKeys = 'past' | 'future' | 'child_of'

type EventFilterFields<T extends BaseEvent> = {
  [K in keyof T as string extends K
    ? never
    : number extends K
      ? never
      : symbol extends K
        ? never
        : K extends FindReservedOptionKeys
          ? never
          : T[K] extends (...args: any[]) => any
            ? never
            : K]?: T[K]
}

export type FindOptions<T extends BaseEvent = BaseEvent> = {
  past?: FindWindow
  future?: FindWindow
  child_of?: BaseEvent | null
} & EventFilterFields<T> &
  Record<string, unknown>

export const normalizeEventPattern = (event_pattern: EventPattern | '*'): string | '*' => {
  if (event_pattern === '*') {
    return '*'
  }
  if (typeof event_pattern === 'string') {
    return event_pattern
  }
  const event_type = (event_pattern as { event_type?: unknown }).event_type
  if (typeof event_type === 'string' && event_type.length > 0 && event_type !== 'BaseEvent') {
    return event_type
  }
  const class_name = (event_pattern as { name?: unknown }).name
  if (typeof class_name === 'string' && class_name.length > 0 && class_name !== 'BaseEvent') {
    return class_name
  }
  let preview: string
  try {
    const encoded = JSON.stringify(event_pattern)
    preview = typeof encoded === 'string' ? encoded.slice(0, 30) : String(event_pattern).slice(0, 30)
  } catch {
    preview = String(event_pattern).slice(0, 30)
  }
  throw new Error('bus.on(match_pattern, ...) must be a string event type, "*", or a BaseEvent class, got: ' + preview)
}

export const isZodSchema = (value: unknown): value is z.ZodTypeAny => !!value && typeof (value as z.ZodTypeAny).safeParse === 'function'

export const eventResultTypeFromConstructor = (value: unknown): z.ZodTypeAny | undefined => {
  if (value === String) {
    return z.string()
  }
  if (value === Number) {
    return z.number()
  }
  if (value === Boolean) {
    return z.boolean()
  }
  if (value === Array) {
    return z.array(z.unknown())
  }
  if (value === Object) {
    return z.record(z.string(), z.unknown())
  }
  return undefined
}

export const extractZodShape = (raw: Record<string, unknown>): z.ZodRawShape => {
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'event_result_type') continue
    if (isZodSchema(value)) shape[key] = value
  }
  return shape as z.ZodRawShape
}

export const toJsonSchema = (schema: unknown): unknown => {
  if (!schema || !isZodSchema(schema)) return schema
  const zod_any = z as unknown as { toJSONSchema: (input: z.ZodTypeAny) => unknown }
  // Cross-language roundtrips preserve core structural types; constraint keywords may not roundtrip exactly.
  return zod_any.toJSONSchema(schema)
}

export const fromJsonSchema = (schema: unknown): z.ZodTypeAny => {
  const zod_any = z as unknown as { fromJSONSchema: (input: unknown) => z.ZodTypeAny }
  return zod_any.fromJSONSchema(schema)
}

export const normalizeEventResultType = (value: EventResultTypeInput): z.ZodTypeAny | undefined => {
  if (value === undefined || value === null) {
    return undefined
  }
  if (isZodSchema(value)) {
    return value
  }
  const constructor_schema = eventResultTypeFromConstructor(value)
  if (constructor_schema) {
    return constructor_schema
  }
  return fromJsonSchema(value)
}
