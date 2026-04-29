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
import { extractZodShape, normalizeEventResultType, toJsonSchema } from './types.js'
import type { EventHandlerCallable, EventResultType } from './types.js'
import { monotonicDatetime } from './helpers.js'

const RESERVED_USER_EVENT_FIELDS = new Set(['bus', 'emit', 'first', 'toString', 'toJSON', 'fromJSON'])

function assertNoReservedUserEventFields(data: Record<string, unknown>, context: string): void {
  for (const field_name of RESERVED_USER_EVENT_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(data, field_name)) {
      throw new Error(`${context} field "${field_name}" is reserved for EventBus runtime context and cannot be set in event payload`)
    }
  }
}

function assertNoUnknownEventPrefixedFields(data: Record<string, unknown>, context: string): void {
  for (const field_name of Object.keys(data)) {
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
    event_timeout: z.number().positive().nullable(),
    event_slow_timeout: z.number().positive().nullable().optional(),
    event_handler_timeout: z.number().positive().nullable().optional(),
    event_handler_slow_timeout: z.number().positive().nullable().optional(),
    event_blocks_parent_completion: z.boolean().optional(),
    event_parent_id: z.string().uuid().nullable().optional(),
    event_path: z.array(z.string()).optional(),
    event_result_type: z.unknown().optional(),
    event_emitted_by_handler_id: z.string().uuid().nullable().optional(),
    event_pending_bus_count: z.number().nonnegative().optional(),
    event_status: z.enum(['pending', 'started', 'completed']).optional(),
    event_started_at: z.string().datetime().nullable().optional(),
    event_completed_at: z.string().datetime().nullable().optional(),
    event_results: z.array(z.unknown()).optional(),
    event_concurrency: z.enum(EVENT_CONCURRENCY_MODES).nullable().optional(),
    event_handler_concurrency: z.enum(EVENT_HANDLER_CONCURRENCY_MODES).nullable().optional(),
    event_handler_completion: z.enum(EVENT_HANDLER_COMPLETION_MODES).nullable().optional(),
  })
  .loose()

const KNOWN_BASE_EVENT_FIELDS = new Set(Object.keys(BaseEventSchema.shape))

export type BaseEventData = z.infer<typeof BaseEventSchema>
export type BaseEventJSON = BaseEventData & Record<string, unknown>
type BaseEventFields = Pick<
  BaseEventData,
  | 'event_id'
  | 'event_created_at'
  | 'event_type'
  | 'event_version'
  | 'event_timeout'
  | 'event_slow_timeout'
  | 'event_handler_timeout'
  | 'event_handler_slow_timeout'
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
>

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields & Partial<BaseEventFields>

type BaseEventSchemaShape = typeof BaseEventSchema.shape

export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<BaseEventSchemaShape & TShape>
type EventPayload<TShape extends z.ZodRawShape> = TShape extends Record<string, never> ? {} : z.infer<z.ZodObject<TShape>>

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> & Partial<BaseEventFields>

type EventWithResultSchema<TResult> = BaseEvent & { __event_result_type__?: TResult }

type ResultTypeFromEventResultTypeInput<TInput> = TInput extends z.ZodTypeAny
  ? z.infer<TInput>
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
            : unknown

type ResultSchemaFromShape<TShape> = TShape extends { event_result_type: infer S } ? ResultTypeFromEventResultTypeInput<S> : unknown
type EventResultsListInclude<TEvent extends BaseEvent> = (
  result: EventResultType<TEvent> | undefined,
  event_result: EventResult<TEvent>
) => boolean
type EventResultsListOptions<TEvent extends BaseEvent> = {
  timeout?: number | null
  include?: EventResultsListInclude<TEvent>
  raise_if_any?: boolean
  raise_if_none?: boolean
}
type EventDoneOptions = {
  raise_if_any?: boolean
}
type EventResultUpdateOptions<TEvent extends BaseEvent> = {
  eventbus?: EventBus
  status?: 'pending' | 'started' | 'completed' | 'error'
  result?: EventResultType<TEvent> | BaseEvent | undefined
  error?: unknown
}

const EVENT_CLASS_DEFAULTS = new WeakMap<Function, Record<string, unknown>>()
const ROOT_EVENTBUS_ID = '00000000-0000-0000-0000-000000000000'

export type EventFactory<TShape extends z.ZodRawShape, TResult = unknown> = {
  (data: EventInit<TShape>): EventWithResultSchema<TResult> & EventPayload<TShape>
  new (data: EventInit<TShape>): EventWithResultSchema<TResult> & EventPayload<TShape>
  schema: EventSchema<TShape>
  class?: new (data: EventInit<TShape>) => EventWithResultSchema<TResult> & EventPayload<TShape>
  event_type?: string
  event_version?: string
  event_result_type?: z.ZodTypeAny
  fromJSON?: (data: unknown) => EventWithResultSchema<TResult> & EventPayload<TShape>
}

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends 'event_result_type' ? never : TShape[K] extends z.ZodTypeAny ? K : never]: Extract<
    TShape[K],
    z.ZodTypeAny
  >
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
  event_blocks_parent_completion!: boolean // true only for children explicitly awaited via done()/eventCompleted()
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

  static event_type?: string // class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  static event_version = '0.0.1'
  static schema = BaseEventSchema // zod schema for the event data fields, used to parse and validate event data when creating a new event

  // internal runtime state
  event_bus?: EventBus // bus that dispatched this event, also used by event.emit(child)
  _event_original?: BaseEvent // underlying event object that was dispatched, if this is a bus-scoped proxy wrapping it
  _event_dispatch_context?: unknown | null // captured AsyncLocalStorage context at dispatch site, used to restore that context when running handlers

  _event_completed_signal: Deferred<this> | null
  _lock_for_event_handler: AsyncLock | null
  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    assertNoReservedUserEventFields(data as Record<string, unknown>, 'BaseEvent')
    assertNoUnknownEventPrefixedFields(data as Record<string, unknown>, 'BaseEvent')
    assertNoModelPrefixedFields(data as Record<string, unknown>, 'BaseEvent')
    const ctor = this.constructor as typeof BaseEvent & {
      event_version?: string
      event_result_type?: z.ZodTypeAny
    }
    const ctor_defaults = EVENT_CLASS_DEFAULTS.get(ctor) ?? {}
    const merged_data = {
      ...ctor_defaults,
      ...data,
    } as BaseEventInit<Record<string, unknown>>
    const event_type = merged_data.event_type ?? ctor.event_type ?? ctor.name
    const event_version = merged_data.event_version ?? ctor.event_version ?? '0.0.1'
    const raw_event_result_type = merged_data.event_result_type ?? ctor.event_result_type
    const event_result_type = normalizeEventResultType(raw_event_result_type)
    const event_id = merged_data.event_id ?? uuidv7()
    const event_created_at = monotonicDatetime(merged_data.event_created_at)
    const event_timeout = merged_data.event_timeout ?? null
    const event_blocks_parent_completion = merged_data.event_blocks_parent_completion ?? false

    const base_data = {
      ...merged_data,
      event_id,
      event_created_at,
      event_type,
      event_version,
      event_timeout,
      event_blocks_parent_completion,
      event_result_type,
    }

    const schema = ctor.schema ?? BaseEventSchema
    const parsed = schema.parse(base_data) as BaseEventData & Record<string, unknown>

    Object.assign(this, parsed)

    const parsed_path = (parsed as { event_path?: string[] }).event_path
    this.event_path = Array.isArray(parsed_path) ? [...parsed_path] : []

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

    this.event_result_type = event_result_type

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
  static extend<TShape extends z.ZodRawShape>(event_type: string, shape?: TShape): EventFactory<TShape, ResultSchemaFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape?: TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape: TShape = {} as TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>> {
    const raw_shape = shape as Record<string, unknown>
    assertNoReservedUserEventFields(raw_shape, `BaseEvent.extend(${event_type})`)
    assertNoUnknownEventPrefixedFields(raw_shape, `BaseEvent.extend(${event_type})`)
    assertNoModelPrefixedFields(raw_shape, `BaseEvent.extend(${event_type})`)
    const raw_event_result_type = raw_shape.event_result_type
    const event_result_type = normalizeEventResultType(raw_event_result_type)
    const event_version = typeof raw_shape.event_version === 'string' ? raw_shape.event_version : undefined
    const event_defaults = Object.fromEntries(
      Object.entries(raw_shape).filter(
        ([key, value]) => key !== 'event_result_type' && key !== 'event_version' && !(value instanceof z.ZodType)
      )
    )

    const zod_shape = extractZodShape(raw_shape)
    const full_schema = BaseEventSchema.extend(zod_shape)

    // create a new event class that extends BaseEvent and adds the custom fields
    class ExtendedEvent extends BaseEvent {
      static schema = full_schema as unknown as typeof BaseEvent.schema
      static event_type = event_type
      static event_version = event_version ?? BaseEvent.event_version
      static event_result_type = event_result_type

      constructor(data: EventInit<ZodShapeFrom<TShape>>) {
        super(data as BaseEventInit<Record<string, unknown>>)
      }
    }

    type FactoryResult = EventWithResultSchema<ResultSchemaFromShape<TShape>> & EventPayload<ZodShapeFrom<TShape>>

    function EventFactory(data: EventInit<ZodShapeFrom<TShape>>): FactoryResult {
      return new ExtendedEvent(data) as FactoryResult
    }

    EventFactory.schema = full_schema as EventSchema<ZodShapeFrom<TShape>>
    EventFactory.event_type = event_type
    EventFactory.event_version = event_version ?? BaseEvent.event_version
    EventFactory.event_result_type = event_result_type
    EventFactory.class = ExtendedEvent as unknown as new (
      data: EventInit<ZodShapeFrom<TShape>>
    ) => EventWithResultSchema<ResultSchemaFromShape<TShape>> & EventPayload<ZodShapeFrom<TShape>>
    EventFactory.fromJSON = (data: unknown) => (ExtendedEvent.fromJSON as (data: unknown) => FactoryResult)(data)
    EventFactory.prototype = ExtendedEvent.prototype
    EVENT_CLASS_DEFAULTS.set(ExtendedEvent, event_defaults)

    return EventFactory as unknown as EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>>
  }

  static fromJSON<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    if (!data || typeof data !== 'object') {
      const schema = this.schema ?? BaseEventSchema
      const parsed = schema.parse(data)
      return new this(parsed) as InstanceType<T>
    }
    const record = { ...(data as Record<string, unknown>) }
    if (record.event_result_type !== undefined && record.event_result_type !== null) {
      record.event_result_type = normalizeEventResultType(record.event_result_type)
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
      if (key.startsWith('_') || key === 'bus' || key === 'event_bus' || key === 'event_results') continue
      if (value === undefined || typeof value === 'function') continue
      record[key] = value
    }
    const event_results = Array.from(this.event_results.values()).map((result) => result.toJSON())

    return {
      ...record,
      event_id: this.event_id,
      event_type: this.event_type,
      event_version: this.event_version,
      event_result_type: this.event_result_type ? toJsonSchema(this.event_result_type) : this.event_result_type,

      // static configuration options
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_timeout: this.event_handler_timeout,
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
      ...(event_results.length > 0 ? { event_results } : {}),
    }
  }

  _createSlowEventWarningTimer(): ReturnType<typeof setTimeout> | null {
    const event_slow_timeout = this.event_slow_timeout ?? this.event_bus?.event_slow_timeout ?? null
    const event_warn_ms = event_slow_timeout === null ? null : event_slow_timeout * 1000
    if (event_warn_ms === null) {
      return null
    }
    const name = this.event_bus?.name ?? 'EventBus'
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
    return entry.status === 'completed' && entry.result !== undefined && entry.result !== null && !(entry.result instanceof BaseEvent)
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
    await this.event_bus.locks._runWithHandlerLock(original, this.event_bus.event_handler_concurrency, async (handler_lock) => {
      await entry.runHandler(handler_lock)
    })
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
      if (original._getHandlerLock(this.event_bus?.event_handler_concurrency) !== null) {
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
    const resolved = original.event_handler_concurrency ?? default_concurrency ?? original.event_bus?.event_handler_concurrency ?? 'serial'
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
      // handlers, but _markCompleted() resolves the done() promise so callers
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

  // Cancel all handler results for an event except the winner, used by first() mode.
  // Cancels pending handlers immediately, aborts started handlers via _signalAbort(),
  // and cancels any child events emitted by the losing handlers.
  _markRemainingFirstModeResultCancelled(winner: EventResult): void {
    const cause = new Error('first() resolved: another handler returned a result first')
    const bus_id = winner.eventbus_id

    for (const result of this.event_results.values()) {
      if (result === winner) continue
      if (result.eventbus_id !== bus_id) continue

      if (result.status === 'pending') {
        result._markError(
          new EventHandlerCancelledError(`Cancelled: first() resolved`, {
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
        const aborted_error = new EventHandlerAbortedError(`Aborted: first() resolved`, {
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

  // awaitable that triggers immediate (queue-jump) processing of the event on all buses where it is queued
  // use eventCompleted() to wait for normal queue-order completion without queue-jumping.
  done(options: EventDoneOptions = {}): Promise<this> {
    if (!this.event_bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    const original = this._event_original ?? this
    original._markBlocksParentCompletionIfAwaitedFromEmittingHandler()
    const raise_if_any = options.raise_if_any ?? true
    const completion_promise =
      this.event_status === 'completed' ? Promise.resolve(original as this) : this.event_bus._processEventImmediately(this)

    if (!raise_if_any) {
      return completion_promise
    }

    // Always delegate to _processEventImmediately — it walks up the parent event tree
    // to determine whether we're inside a handler (works cross-bus). If no
    // ancestor handler is in-flight, it falls back to eventCompleted().
    return completion_promise.then((completed_event) => {
      const first_error = completed_event._firstProcessingError()
      if (first_error !== undefined) {
        if (first_error instanceof Error) {
          throw first_error
        }
        throw new Error(String(first_error))
      }
      return completed_event
    })
  }

  // returns the first non-undefined handler result value, cancelling remaining handlers
  // when any handler completes. Works with all event_handler_concurrency modes:
  //   parallel: races all handlers, returns first non-undefined, aborts the rest
  //   serial: runs handlers sequentially, returns first non-undefined, skips remaining
  first(): Promise<EventResultType<this> | undefined> {
    if (!this.event_bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    const original = this._event_original ?? this
    original.event_handler_completion = 'first'
    return this.done({ raise_if_any: false }).then((completed_event) => {
      const first_error = completed_event._firstProcessingError({ ignore_first_mode_control_errors: true })
      if (first_error !== undefined) {
        if (first_error instanceof Error) {
          throw first_error
        }
        throw new Error(String(first_error))
      }
      const orig = completed_event._event_original ?? completed_event
      return Array.from(orig.event_results.values())
        .filter(
          (result) =>
            result.status === 'completed' && result.result !== undefined && result.result !== null && !(result.result instanceof BaseEvent)
        )
        .sort((a, b) => compareIsoDatetime(a.completed_at, b.completed_at))
        .map((result) => result.result as EventResultType<this>)
        .at(0)
    })
  }

  // returns handler result values in event_results insertion order.
  // equivalent to await event.done(); Array.from(event.event_results.values()).map((entry) => entry.result)
  eventResultsList(
    include: EventResultsListInclude<this>,
    options?: EventResultsListOptions<this>
  ): Promise<Array<EventResultType<this> | undefined>>
  eventResultsList(options?: EventResultsListOptions<this>): Promise<Array<EventResultType<this> | undefined>>
  async eventResultsList(
    include_or_options?: EventResultsListInclude<this> | EventResultsListOptions<this>,
    maybe_options?: EventResultsListOptions<this>
  ): Promise<Array<EventResultType<this> | undefined>> {
    const default_include: EventResultsListInclude<this> = (_result, event_result) =>
      event_result.status === 'completed' &&
      event_result.result !== undefined &&
      event_result.result !== null &&
      !(event_result.result instanceof Error) &&
      !(event_result.result instanceof BaseEvent) &&
      event_result.error === undefined

    let options: EventResultsListOptions<this>
    let include: EventResultsListInclude<this>
    if (typeof include_or_options === 'function') {
      options = maybe_options ?? {}
      include = include_or_options
    } else {
      options = include_or_options ?? {}
      include = options.include ?? default_include
    }
    const raise_if_any = options.raise_if_any ?? true
    const raise_if_none = options.raise_if_none ?? true

    const original = this._event_original ?? this
    const resolved_timeout_seconds = options.timeout ?? original.event_timeout ?? this.event_bus?.event_timeout ?? null
    let completed_event: this

    if (resolved_timeout_seconds === null) {
      completed_event = await this.done({ raise_if_any: false })
    } else {
      completed_event = await _runWithTimeout(
        resolved_timeout_seconds,
        () => new Error(`Timed out waiting for ${original.event_type} results after ${resolved_timeout_seconds}s`),
        () => this.done({ raise_if_any: false })
      )
    }

    const all_results: EventResult<this>[] = Array.from(completed_event.event_results.values())
    const error_results = all_results.filter((event_result) => event_result.error !== undefined || event_result.result instanceof Error)

    if (raise_if_any && error_results.length > 0) {
      if (error_results.length === 1) {
        const first_error = error_results[0]
        if (first_error.error instanceof Error) {
          throw first_error.error
        }
        if (first_error.result instanceof Error) {
          throw first_error.result
        }
        throw new Error(String(first_error.error ?? first_error.result))
      }

      const errors = error_results.map((event_result) => {
        if (event_result.error instanceof Error) {
          return event_result.error
        }
        if (event_result.result instanceof Error) {
          return event_result.result
        }
        return new Error(String(event_result.error ?? event_result.result))
      })
      throw new AggregateError(
        errors,
        `Event ${completed_event.event_type}#${completed_event.event_id.slice(-4)} had ${errors.length} handler error(s)`
      )
    }

    const included_results = all_results.filter((event_result) => include(event_result.result, event_result))
    if (raise_if_none && included_results.length === 0) {
      throw new Error(
        `Expected at least one handler to return a non-null result, but none did: ${completed_event.event_type}#${completed_event.event_id.slice(-4)}`
      )
    }

    return included_results.map((event_result) => event_result.result)
  }

  // awaitable that waits for the event to be processed in normal queue order by the _runloop
  eventCompleted(): Promise<this> {
    const original = this._event_original ?? this
    original._markBlocksParentCompletionIfAwaitedFromEmittingHandler()
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    this._notifyDoneListeners()
    return this._event_completed_signal!.promise
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

  _markPending(): this {
    const original = this._event_original ?? this
    original.event_status = 'pending'
    original.event_started_at = null
    original.event_completed_at = null
    original.event_results.clear()
    original.event_pending_bus_count = 0
    original._setDispatchContext(undefined)
    original._event_completed_signal = null
    original._lock_for_event_handler = null
    original.event_bus = undefined
    return this
  }

  eventReset(): this {
    const original = this._event_original ?? this
    const ctor = original.constructor as typeof BaseEvent
    const fresh_event = ctor.fromJSON(original.toJSON()) as this
    fresh_event.event_id = uuidv7()
    return fresh_event._markPending()
  }

  _markStarted(started_at: string | null = null, notify_hook: boolean = true): void {
    const original = this._event_original ?? this
    if (original.event_status !== 'pending') {
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
    if (original.event_status === 'completed') {
      return
    }
    if (!force) {
      if (original.event_pending_bus_count > 0) {
        return
      }
      if (!original._areAllChildrenComplete()) {
        return
      }
    }
    original.event_status = 'completed'
    original.event_completed_at = monotonicDatetime()
    if (original.event_bus) {
      const bus_for_hook = original.event_bus
      const event_for_bus = bus_for_hook._getEventProxyScopedToThisBus(original)
      void bus_for_hook.onEventChange(event_for_bus, 'completed')
    }
    original._setDispatchContext(null)
    original._notifyDoneListeners()
    original._event_completed_signal!.resolve(original)
    original._event_completed_signal = null
    original.dropFromZeroHistoryBuses()
    if (notify_parents && original.event_bus) {
      original._notifyEventParentsOfCompletion()
    }
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

  private _isFirstModeControlError(error: unknown): boolean {
    if (!(error instanceof EventHandlerCancelledError || error instanceof EventHandlerAbortedError)) {
      return false
    }
    if (error.message.includes('first() resolved')) {
      return true
    }
    return error.cause instanceof Error && error.cause.message.includes('first() resolved')
  }

  _firstProcessingError(options: { ignore_first_mode_control_errors?: boolean } = {}): unknown | undefined {
    const ignore_first_mode_control_errors = options.ignore_first_mode_control_errors ?? false
    return Array.from(this.event_results.values())
      .filter((event_result) => event_result.error !== undefined && event_result.completed_at !== null)
      .filter((event_result) => (ignore_first_mode_control_errors ? !this._isFirstModeControlError(event_result.error) : true))
      .sort((event_result_a, event_result_b) => compareIsoDatetime(event_result_a.completed_at, event_result_b.completed_at))
      .map((event_result) => event_result.error)
      .at(0)
  }

  // Returns the first non-undefined completed handler result, sorted by completion time.
  // Useful after first() or done() to get the winning result value.
  get event_result(): EventResultType<this> | undefined {
    return Array.from(this.event_results.values())
      .filter((event_result) => event_result.completed_at !== null && event_result.result !== undefined)
      .sort((event_result_a, event_result_b) => compareIsoDatetime(event_result_a.completed_at, event_result_b.completed_at))
      .map((event_result) => event_result.result as EventResultType<this>)
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
  if (!Array.isArray(raw_event_results)) {
    return event_results
  }
  for (const item of raw_event_results) {
    const result = EventResult.fromJSON(event, item)
    const map_key = typeof result.handler_id === 'string' && result.handler_id.length > 0 ? result.handler_id : result.id
    event_results.set(map_key, result)
  }
  return event_results
}
